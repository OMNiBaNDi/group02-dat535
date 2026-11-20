"""Save and reload the project's Gold tables.

The catalogue and monthly coverage are saved together as one release. Its
manifest lists the files, checksums, Gold rule and hourly Silver input (parent).
The manifest is published last, so readers do not see a half-written release."""

import json
from pathlib import Path
import fcntl
import sys
from tempfile import TemporaryDirectory
from pipeline_files import file_sha256, child_path, json_sha256, verify_file_inventory, write_json


def stage_gold_table(table, bundle: Path, name: str):
    """Write a table in staging and return its file details and saved DataFrame."""
    directory = bundle / name
    # Write typed Parquet files in a new directory before checking the saved table.
    table.write.mode("errorifexists").parquet(str(directory))

    # Zero rows are allowed. An empty table can still have a Parquet file.
    files = sorted(directory.rglob("*.parquet"))
    if not files:
        raise ValueError(f"{name}: Spark wrote no Parquet files.")

    # Reopen the files and check the column names, order and types.
    # This does not compare row values or whether columns allow nulls.
    saved = table.sparkSession.read.parquet(*(str(path) for path in files))
    if saved.dtypes != table.dtypes:
        raise ValueError(f"{name}: column names, order or types changed after saving.")

    # Save relative paths so they still work after the folder is moved.
    # File sizes and checksums let us check the saved files later.
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
        "schema": saved.schema.jsonValue(),
        "files": inventory,
        "bytes": sum(item["bytes"] for item in inventory),
    }, saved

def check_gold_coverage(monthly, catalogue):
    """Compare period counts and their total hours, then return the release counts."""
    from pyspark.sql import functions as F

    # Compare monthly totals with catalogue durations and period counts.
    # Empty tables contribute zero to the totals.
    totals = monthly.agg(
        F.count("*").alias("months"),
        F.coalesce(F.sum("elevated_hours"), F.lit(0)).alias("elevated_hours"),
        F.coalesce(F.sum("qualifying_period_hours"), F.lit(0)).alias("member_hours"),
        F.coalesce(F.sum("periods_started"), F.lit(0)).alias("periods"),
    ).first()

    # Each catalogue row is one period. Adding the durations gives the member hours.
    # The monthly table already stores counts, so add up periods_started there.
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

    # Period boundaries are checked in build_period_catalogue().
    return totals.asDict()

def publish_gold(
    *, catalogue, monthly,
    rules, parent, parent_path, parent_sha256, context, rule_config_sha256,
):
    """Save both Gold tables as one release and return the manifest path and checksum."""
    # Record the Silver release and ozone rule used to produce these results.
    inputs = {
        "parent_hourly": {
            "publication_id": parent["publication_id"],
            "manifest_path": parent_path,
            "manifest_sha256": parent_sha256,
        },
        "rules": rules,
        "rule_config_sha256": rule_config_sha256,
    }

    # Use the region and year from the selected Silver release.
    profile = parent["evidence"]["profile"]
    region = profile["region"].strip().lower().replace(" ", "_")
    year = profile["year"]
    root = context.environment_root
    release_dir = child_path(root, f"published/gold/{region}/{year}")
    release_dir.mkdir(parents=True, exist_ok=True)
    context.staging_root.mkdir(parents=True, exist_ok=True)

    # Write both tables in staging before publishing them.
    # Files left in this temporary folder are removed when the block ends.
    # Published tables are moved out first, so they are kept.
    with TemporaryDirectory(prefix="gold-", dir=context.staging_root) as scratch:
        scratch = Path(scratch)
        bundle = scratch / "tables"
        catalogue_info, saved_catalogue = stage_gold_table(catalogue, bundle, "period_catalogue")
        monthly_info, saved_monthly = stage_gold_table(monthly, bundle, "monthly_coverage")
        # Compare the totals from the saved tables before publishing.
        coverage = check_gold_coverage(saved_monthly, saved_catalogue)
        datasets = {
            "period_catalogue": {**catalogue_info, "records": coverage["periods"]},
            "monthly_coverage": {**monthly_info, "records": coverage["months"]},
        }

        metadata = {
            "manifest_version": 1,
            "kind": "gold",
            "environment": context.environment,
            "run_id": context.run_id,
            "region": region,
            "year": year,
            **inputs,
            "datasets": datasets,
            # These columns describe what identifies one row. They do not check for duplicates.
            "keys": {
                "period_catalogue": ["region", "year", "monitor_id", "period_start_utc"],
                "monthly_coverage": [
                    "region", "year", "monitor_id", "station_id", "month_start_utc"
                ],
            },
            "coverage_check": coverage,
            "spark_version": catalogue.sparkSession.version,
            "python_version": sys.version.split()[0],
        }

        # The run ID and file details are included in this hash.
        # Another run can get a different release ID even when its results are the same.
        release_id = json_sha256(metadata)
        bundle_path = f"data/gold/{region}/{year}/{release_id}"
        destination = child_path(root, bundle_path)
        manifest_path = release_dir / release_id / "manifest.json"
        manifest = {**metadata, "publication_id": release_id, "bundle_path": bundle_path}

        # Prepare the manifest in staging. Publish it only after both tables have moved.
        pending = scratch / "manifest.json"
        pending.write_text(json.dumps(manifest, indent=2, allow_nan=False) + "\n", encoding="utf-8")

        with (release_dir / ".publish.lock").open("a") as lock:
            # Wait if another run is publishing Gold for this region and year.
            # Closing the file releases the lock.
            fcntl.flock(lock, fcntl.LOCK_EX)
            destination.parent.mkdir(parents=True, exist_ok=True)
            manifest_path.parent.mkdir(parents=True, exist_ok=True)

            if destination.exists() or manifest_path.exists():
                raise FileExistsError("Gold destination or manifest already exists; preserve it and use a fresh run.")
            bundle.rename(destination)
            pending.rename(manifest_path)  # The manifest marks both tables as ready to load.

    # This checksum covers the final manifest file, including its publication ID.
    return {"manifest_path": str(manifest_path), "manifest_sha256": file_sha256(manifest_path)}


def load_gold(manifest_path, *, expected_sha256, expected_parent_sha256, context, spark):
    """Check the selected release and return its manifest and Spark tables."""
    # Load the explicitly selected release, not whichever directory is newest.
    root = context.environment_root
    path = Path(manifest_path).resolve(strict=True)
    if not path.is_relative_to(root / "published/gold") or file_sha256(path) != expected_sha256:
        raise ValueError("Gold manifest path or checksum differs from the expected release.")

    manifest = json.loads(path.read_text(encoding="utf-8"))
    if (
        manifest["manifest_version"] != 1
        or manifest["kind"] != "gold"
        or manifest["environment"] != context.environment
    ):
        raise ValueError("Gold manifest identity is inconsistent.")

    # Check that Gold points to the expected hourly Silver manifest.
    # The Silver loader checks the parent tables.
    parent = manifest["parent_hourly"]
    parent_path = child_path(root, parent["manifest_path"], must_exist=True)
    if (
        parent["manifest_sha256"] != expected_parent_sha256
        or file_sha256(parent_path) != expected_parent_sha256
    ):
        raise ValueError("Gold release points to a different hourly Silver parent.")

    datasets = manifest["datasets"]
    # Require exactly the two published tables.
    if set(datasets) != {"period_catalogue", "monthly_coverage"}:
        raise ValueError("Gold release must contain the catalogue and monthly coverage.")

    bundle = child_path(root, manifest["bundle_path"], must_exist=True)
    # Check the listed Parquet files against their saved sizes and checksums.
    # Row counts and coverage were checked before publication.
    files = verify_file_inventory(bundle, datasets)
    tables = {}
    for name, paths in files.items():
        # Reopen each table and check its schema without counting its rows again.
        table = spark.read.parquet(*paths)
        if table.schema.jsonValue() != datasets[name]["schema"]:
            raise ValueError(f"{name}: reloaded schema differs from the manifest.")
        tables[name] = table

    return manifest, tables


def write_gold_completion(context, manifest, release):
    """Write the run's completion report after Gold has been reloaded."""
    # Call this only after load_gold returns successfully.
    # The runner uses this report to check the finished run and its Silver input.
    path = context.reports_dir / "gold_completion.json"
    write_json(path, {
        "status": "success",
        "environment": context.environment,
        "run_id": context.run_id,
        "publication_id": manifest["publication_id"],
        "manifest_path": Path(release["manifest_path"]).relative_to(context.environment_root).as_posix(),
        "manifest_sha256": release["manifest_sha256"],
        "parent_hourly": manifest["parent_hourly"],
        # Store row counts here. File details stay in the manifest.
        # The measurements stay in Parquet.
        "datasets": {name: info["records"] for name, info in manifest["datasets"].items()},
    })
    return path
