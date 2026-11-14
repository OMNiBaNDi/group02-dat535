"""Save and reload the project's Gold tables.

The catalogue and monthly coverage are saved together as one release. Its
manifest lists the files, checksums, Gold rule and hourly Silver input (parent).
The manifest is published last, so readers do not see a half-written release."""

import json
from pathlib import Path
import fcntl
import sys
from tempfile import TemporaryDirectory
from gold import load_gold_rules
from silver_files import file_sha256, silver_child_path, silver_json_sha256, verify_silver_file_inventory


def stage_gold_table(table, bundle: Path, name: str) -> dict:
    """Write one table, check its reloaded schema and return row counts and file checksums."""
    # Write to a new staging directory without replacing existing output.
    directory = bundle / name
    table.write.mode("errorifexists").parquet(str(directory))

    files = sorted(directory.rglob("*.parquet"))
    if not files:
        raise ValueError(f"{name}: Spark wrote no Parquet files.")

    # Check that saving preserved column names, order and types. The fixture tests
    # compare values too; this production step records file hashes and row counts.
    saved = table.sparkSession.read.parquet(*(str(path) for path in files))
    if saved.dtypes != table.dtypes:
        raise ValueError(f"{name}: column names, order or types changed after saving.")

    # Record actual file paths, sizes and hashes for the publication loader.
    inventory = [
        {
            "path": path.relative_to(bundle).as_posix(),
            "bytes": path.stat().st_size,
            "sha256": file_sha256(path),
        }
        for path in files
    ]
    return {
        "path": name,
        "records": saved.count(),
        "schema": saved.schema.jsonValue(),
        "files": inventory,
        "bytes": sum(item["bytes"] for item in inventory),
    }

def gold_release_inputs(context, parent, parent_path, parent_sha256, rules, rule_path):
    """Verify the selected hourly Silver manifest and saved Gold rule, then return their identities."""
    root = Path(context.environment_root).resolve()
    path = silver_child_path(root, parent_path, must_exist=True)

    if not path.is_relative_to(root / "published" / "silver" / "hourly"):
        raise ValueError("Gold requires a published hourly Silver manifest.")
    if file_sha256(path) != parent_sha256:
        raise ValueError("The hourly Silver manifest checksum has changed.")

    # Require the saved Silver parent to equal the verified one used for the calculation.
    saved_parent = json.loads(path.read_text(encoding="utf-8"))
    if (
        saved_parent != parent
        or saved_parent["kind"] != "hourly_silver"
        or saved_parent["status"] != "published"
        or saved_parent["environment"] != context.environment
    ):
        raise ValueError("The saved Silver manifest differs from the verified Gold input.")

    # Match the in-memory rule to its selected config, including an alternative Analyse rule.
    saved_rules = load_gold_rules(rule_path)
    if rules != saved_rules:
        raise ValueError("Gold publication requires the rules in the selected config.")

    return {
        "parent_hourly": {
            "publication_id": saved_parent["publication_id"],
            "manifest_path": path.relative_to(root).as_posix(),
            "manifest_sha256": parent_sha256,
        },
        "rules": saved_rules,
        "rule_config_sha256": file_sha256(rule_path),
    }

def check_gold_coverage(monthly, catalogue):
    """Check that the release tables account for the same hours and periods."""
    from pyspark.sql import functions as F

    # Sum monthly counts independently of the catalogue durations and period count.
    totals = monthly.agg(
        F.count("*").alias("months"),
        F.coalesce(F.sum("elevated_hours"), F.lit(0)).alias("elevated_hours"),
        F.coalesce(F.sum("qualifying_period_hours"), F.lit(0)).alias("member_hours"),
        F.coalesce(F.sum("periods_started"), F.lit(0)).alias("periods"),
    ).first()

    periods = catalogue.agg(
        F.count("*").alias("periods"),
        F.coalesce(F.sum("duration_hours"), F.lit(0)).alias("member_hours"),
    ).first()

    # Both outputs must account for the same number of qualifying hours and periods.
    if (
        totals.member_hours != periods.member_hours
        or totals.periods != periods.periods
    ):
        raise ValueError("Gold monthly totals do not reconcile with the hours and catalogue.")

    return totals.asDict()

def publish_gold(
    *, catalogue, monthly,
    rules, parent, parent_path, parent_sha256, context, rule_path=None,
):
    """Stage both Gold tables and publish their manifest after validation."""
    # Default to the primary rule. A changed alternative rule needs its own version label.
    project = Path(__file__).resolve().parents[1]
    primary_path = project / "config/gold_rules.json"
    if rule_path is None:
        rule_path = primary_path
    elif rule_path != primary_path:
        primary = load_gold_rules(primary_path)
        if rules != primary and rules["rule_version"] == primary["rule_version"]:
            raise ValueError("Give the changed Gold rule its own rule_version.")
    # Check the saved parent and rule, then reconcile the two prepared outputs.
    inputs = gold_release_inputs(
        context, parent, parent_path, parent_sha256, rules,
        rule_path,
    )
    coverage = check_gold_coverage(monthly, catalogue)

    profile = parent["evidence"]["profile"]
    region = profile["region"].strip().lower().replace(" ", "_")
    year = profile["year"]
    root = context.environment_root
    release_dir = silver_child_path(root, f"published/gold/{region}/{year}")
    release_dir.mkdir(parents=True, exist_ok=True)
    context.staging_root.mkdir(parents=True, exist_ok=True)

    # Stage both tables before publishing. A write failure here leaves no new release manifest.
    with TemporaryDirectory(prefix="gold-", dir=context.staging_root) as scratch:
        scratch = Path(scratch)
        bundle = scratch / "tables"
        datasets = {
            "period_catalogue": stage_gold_table(catalogue, bundle, "period_catalogue"),
            "monthly_coverage": stage_gold_table(monthly, bundle, "monthly_coverage"),
        }
        if (
            datasets["period_catalogue"]["records"] != coverage["periods"]
            or datasets["monthly_coverage"]["records"] != coverage["months"]
        ):
            raise ValueError("Staged Gold row counts differ from the prepared outputs.")
        verify_silver_file_inventory(bundle, datasets)

        # Record the selected rule, exact Silver parent, file inventory and code used for this run.
        metadata = {
            "manifest_version": 1,
            "kind": "gold",
            "environment": context.environment,
            "run_id": context.run_id,
            "region": region,
            "year": year,
            **inputs,
            "datasets": datasets,
            "keys": {
                "period_catalogue": ["region", "year", "monitor_id", "period_start_utc"],
                "monthly_coverage": [
                    "region", "year", "monitor_id", "station_id", "month_start_utc"
                ],
            },
            "coverage_check": coverage,
            "scope_note": (
                "Results describe eligible ozone hours for the selected monitor. "
                "Missing hours can interrupt an observed period."
            ),
            "spark_version": catalogue.sparkSession.version,
            "python_version": sys.version.split()[0],
            "code_sha256": {
                name: file_sha256(project / name)
                for name in ("notebooks/pipeline.ipynb", "src/gold.py", "src/gold_io.py")
            },
        }

        # Run and file metadata contribute to the ID; equal analytical results can have different IDs.
        release_id = silver_json_sha256(metadata)
        bundle_path = f"data/gold/{region}/{year}/{release_id}"
        destination = silver_child_path(root, bundle_path)
        manifest_path = release_dir / release_id / "manifest.json"
        manifest = {**metadata, "publication_id": release_id, "bundle_path": bundle_path}

        # Prepare the manifest beside the staged tables before moving them to their final paths.
        pending = scratch / "manifest.json"
        pending.write_text(json.dumps(manifest, indent=2, allow_nan=False) + "\n", encoding="utf-8")

        # Only one publisher can enter this block at a time. This prevents two runs
        # from changing the release paths together; closing the file releases the lock.
        with (release_dir / ".publish.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            destination.parent.mkdir(parents=True, exist_ok=True)
            manifest_path.parent.mkdir(parents=True, exist_ok=True)

            # Verify an existing identical release, or place the data and write its manifest last.
            if manifest_path.exists():
                if json.loads(manifest_path.read_text(encoding="utf-8")) != manifest:
                    raise ValueError("An existing Gold manifest has different contents.")
                verify_silver_file_inventory(destination, datasets)
            else:
                if destination.exists():
                    # A previous attempt may have stopped between the two renames.
                    verify_silver_file_inventory(destination, datasets)
                else:
                    bundle.rename(destination)
                pending.rename(manifest_path)

    return {"manifest_path": str(manifest_path), "manifest_sha256": file_sha256(manifest_path)}


def load_gold(manifest_path, *, expected_sha256, expected_parent_sha256, context, spark):
    """Verify a selected Gold release and read its listed Parquet files."""
    # Load the explicitly selected release, not whichever directory is newest.
    root = context.environment_root
    path = Path(manifest_path).resolve(strict=True)
    if not path.is_relative_to(root / "published/gold") or file_sha256(path) != expected_sha256:
        raise ValueError("Gold manifest path or checksum differs from the expected release.")

    manifest = json.loads(path.read_text(encoding="utf-8"))
    release_id = manifest["publication_id"]
    # Recalculate the release ID from its recorded metadata and check the directory layout.
    metadata = {k: v for k, v in manifest.items() if k not in ("publication_id", "bundle_path")}
    suffix = f"gold/{manifest['region']}/{manifest['year']}/{release_id}"
    if (
        manifest["manifest_version"] != 1
        or manifest["kind"] != "gold"
        or manifest["environment"] != context.environment
        or silver_json_sha256(metadata) != release_id
        or path != silver_child_path(root, f"published/{suffix}/manifest.json")
        or manifest["bundle_path"] != f"data/{suffix}"
    ):
        raise ValueError("Gold manifest identity is inconsistent.")

    # Confirm this Gold release names the expected hourly Silver input.
    # This checks its manifest; checking the Silver tables is the hourly loader's job.
    parent = manifest["parent_hourly"]
    parent_path = silver_child_path(root, parent["manifest_path"], must_exist=True)
    if (
        parent["manifest_sha256"] != expected_parent_sha256
        or file_sha256(parent_path) != expected_parent_sha256
    ):
        raise ValueError("Gold release points to a different hourly Silver parent.")

    datasets = manifest["datasets"]
    if set(datasets) != {"period_catalogue", "monthly_coverage"}:
        raise ValueError("Gold release must contain the catalogue and monthly coverage.")

    bundle = silver_child_path(root, manifest["bundle_path"], must_exist=True)
    # Verify file hashes, then compare reloaded schemas and counts with the manifest.
    files = verify_silver_file_inventory(bundle, datasets)
    tables = {}
    for name, paths in files.items():
        table = spark.read.parquet(*paths)
        if table.schema.jsonValue() != datasets[name]["schema"] or table.count() != datasets[name]["records"]:
            raise ValueError(f"{name}: reloaded schema or row count differs from the manifest.")
        tables[name] = table

    return manifest, tables


def write_gold_completion(context, manifest, release, write_json):
    """Write the receipt after load_gold has verified both tables."""
    # Save the release location and checksum so the runner and later runs can find it.
    path = context.reports_dir / "gold_completion.json"
    write_json(path, {
        "status": "success",
        "environment": context.environment,
        "run_id": context.run_id,
        "publication_id": manifest["publication_id"],
        "manifest_path": Path(release["manifest_path"]).relative_to(context.environment_root).as_posix(),
        "manifest_sha256": release["manifest_sha256"],
        "parent_hourly": manifest["parent_hourly"],
        "datasets": {name: info["records"] for name, info in manifest["datasets"].items()},
    })
    return path
