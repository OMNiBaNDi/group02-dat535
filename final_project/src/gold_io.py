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
    # Save one DataFrame in a new table directory under the temporary bundle.
    # Return a dictionary describing its saved files, not the measurement rows.
    directory = bundle / name
    # Writing Parquet is an action: Spark computes the table and writes one or more
    # part files in this directory. errorifexists avoids overwriting another output.
    # Parquet keeps typed columns; this is a directory, not a single named CSV.
    table.write.mode("errorifexists").parquet(str(directory))

    # rglob finds part files recursively. sorted gives a stable inventory order.
    # An empty table can still have a typed Parquet file; zero rows is not an error.
    files = sorted(directory.rglob("*.parquet"))
    if not files:
        raise ValueError(f"{name}: Spark wrote no Parquet files.")

    # Reopen the staged files. * supplies each file path as a separate argument.
    # dtypes checks column names, order and types. It does not compare row values
    # or whether columns allow nulls; value comparisons belong to the fixture tests.
    saved = table.sparkSession.read.parquet(*(str(path) for path in files))
    if saved.dtypes != table.dtypes:
        raise ValueError(f"{name}: column names, order or types changed after saving.")

    # Describe every Parquet file with its path, size and checksum. Relative paths
    # keep working when the bundle moves. Hashes detect changed bytes, not bad readings.
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
        # count is an action reading the saved table; schema.jsonValue is a JSON-ready
        # description of its structure. Summing file bytes below gives total file size.
        "records": saved.count(),
        "schema": saved.schema.jsonValue(),
        "files": inventory,
        "bytes": sum(item["bytes"] for item in inventory),
    }

def gold_release_inputs(context, parent, parent_path, parent_sha256, rules, rule_path):
    """Verify the selected hourly Silver manifest and saved Gold rule, then return their identities."""
    # The parent is the verified hourly Silver manifest used to calculate Gold.
    # Resolve its relative path inside the environment and require it to exist.
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

    # Require the supplied rule to match the selected JSON file, including an Analyse
    # alternative. Otherwise the published description could disagree with the calculation.
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
    # Without groupBy, agg produces one summary row for the whole table.
    # coalesce(sum(...), lit(0)) uses zero when a sum is null (for example empty input).
    # first is an action returning that small Row to Python, not the whole dataset.
    totals = monthly.agg(
        F.count("*").alias("months"),
        F.coalesce(F.sum("elevated_hours"), F.lit(0)).alias("elevated_hours"),
        F.coalesce(F.sum("qualifying_period_hours"), F.lit(0)).alias("member_hours"),
        F.coalesce(F.sum("periods_started"), F.lit(0)).alias("periods"),
    ).first()

    # One catalogue row is one period; summing durations gives member hours.
    # Monthly rows instead contain counts, so we sum periods_started there.
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

    # Return the small summary Row as a Python dictionary for the manifest.
    # Agreement of totals does not prove that every period boundary is correct.
    return totals.asDict()

def publish_gold(
    *, catalogue, monthly,
    rules, parent, parent_path, parent_sha256, context, rule_path=None,
):
    """Stage both Gold tables and publish their manifest after validation."""
    # The * in the function definition requires named arguments. Find final_project
    # relative to this Python file. Use the primary config unless another is supplied;
    # a changed alternative rule must have its own version label.
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
    # Compare period and member-hour totals between the two prepared tables.
    coverage = check_gold_coverage(monthly, catalogue)

    # Take the region and year from the selected Silver release. Remove surrounding
    # spaces, use lowercase and replace spaces with underscores for the folder name.
    profile = parent["evidence"]["profile"]
    region = profile["region"].strip().lower().replace(" ", "_")
    year = profile["year"]
    root = context.environment_root
    release_dir = silver_child_path(root, f"published/gold/{region}/{year}")
    release_dir.mkdir(parents=True, exist_ok=True)
    context.staging_root.mkdir(parents=True, exist_ok=True)

    # Write both tables in a temporary working folder before publishing anything.
    # Leaving this with block cleans up what remains in that folder. Successful
    # publication moves the tables outside it first, so the saved results remain.
    with TemporaryDirectory(prefix="gold-", dir=context.staging_root) as scratch:
        scratch = Path(scratch)
        bundle = scratch / "tables"
        # Python evaluates these two calls in order: catalogue first, monthly second.
        # Each value is a table inventory returned by stage_gold_table.
        datasets = {
            "period_catalogue": stage_gold_table(catalogue, bundle, "period_catalogue"),
            "monthly_coverage": stage_gold_table(monthly, bundle, "monthly_coverage"),
        }
        if (
            datasets["period_catalogue"]["records"] != coverage["periods"]
            or datasets["monthly_coverage"]["records"] != coverage["months"]
        ):
            raise ValueError("Staged Gold row counts differ from the prepared outputs.")
        # Despite its Silver name, this shared helper also verifies Gold file paths,
        # complete inventory, byte sizes and hashes. It does not recalculate Gold rules.
        verify_silver_file_inventory(bundle, datasets)

        # Describe this release: its inputs, saved files, totals and software versions.
        # **inputs copies the parent and rule entries into this dictionary.
        # keys lists the intended row identifiers; it does not run a uniqueness check.
        # code_sha256 records checksums for the three listed code files.
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

        # Hash the metadata, including the run ID and file details, to identify this
        # release. Another run may have identical results but a different release ID.
        # Add the ID itself afterwards so it is not part of its own calculation.
        release_id = silver_json_sha256(metadata)
        bundle_path = f"data/gold/{region}/{year}/{release_id}"
        destination = silver_child_path(root, bundle_path)
        manifest_path = release_dir / release_id / "manifest.json"
        manifest = {**metadata, "publication_id": release_id, "bundle_path": bundle_path}

        # Prepare the manifest beside the staged tables before moving them to their final paths.
        pending = scratch / "manifest.json"
        # Write a readable temporary manifest. allow_nan=False rejects nonstandard JSON
        # numbers such as NaN/infinity. Nothing is published at the final path yet.
        pending.write_text(json.dumps(manifest, indent=2, allow_nan=False) + "\n", encoding="utf-8")

        # Open the file used to coordinate publication.
        with (release_dir / ".publish.lock").open("a") as lock:
            # Wait for exclusive access: only one cooperating publisher can change these
            # release paths at a time. Closing the file releases the lock. The lock file
            # can remain on disk after it is no longer held.
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
                    # Move both staged table directories to permanent data/gold storage. They are
                    # the same files we verified, not another calculation. A failure before the next
                    # rename can leave data without a manifest; readers must not treat that as ready.
                    bundle.rename(destination)
                # Publish the manifest last. The existing-destination branch above can verify
                # files left by an interrupted attempt at this identical release before finishing.
                pending.rename(manifest_path)

    # Return the final manifest location and its file-byte checksum. This checksum
    # is distinct from the publication ID, which hashes metadata before adding the ID.
    return {"manifest_path": str(manifest_path), "manifest_sha256": file_sha256(manifest_path)}


def load_gold(manifest_path, *, expected_sha256, expected_parent_sha256, context, spark):
    """Verify a selected Gold release and read its listed Parquet files."""
    # Load the explicitly selected release, not whichever directory is newest.
    root = context.environment_root
    # Inputs identify an explicit Gold release. Output is (manifest dictionary,
    # dictionary of loaded DataFrames). No Gold detection is repeated by this loader.
    path = Path(manifest_path).resolve(strict=True)
    if not path.is_relative_to(root / "published/gold") or file_sha256(path) != expected_sha256:
        raise ValueError("Gold manifest path or checksum differs from the expected release.")

    manifest = json.loads(path.read_text(encoding="utf-8"))
    release_id = manifest["publication_id"]
    # Remove the two fields added after ID calculation, then recalculate the ID.
    # Also check that the manifest describes the right environment and file locations.
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
    # set(dictionary) returns its keys. Require exactly the two published tables.
    if set(datasets) != {"period_catalogue", "monthly_coverage"}:
        raise ValueError("Gold release must contain the catalogue and monthly coverage.")

    bundle = silver_child_path(root, manifest["bundle_path"], must_exist=True)
    # Verify file hashes, then compare reloaded schemas and counts with the manifest.
    files = verify_silver_file_inventory(bundle, datasets)
    tables = {}
    for name, paths in files.items():
        # Read each verified part file into one DataFrame. * expands the list of paths.
        # Check its schema and count against the manifest; count triggers computation.
        # These are the saved files, not the original in-memory catalogue or monthly table.
        table = spark.read.parquet(*paths)
        if table.schema.jsonValue() != datasets[name]["schema"] or table.count() != datasets[name]["records"]:
            raise ValueError(f"{name}: reloaded schema or row count differs from the manifest.")
        tables[name] = table

    # Return two objects: the metadata dictionary and the dictionary of DataFrames.
    # This checks the saved release; it does not rerun the ozone analysis.
    return manifest, tables


def write_gold_completion(context, manifest, release, write_json):
    """Write the receipt after load_gold has verified both tables."""
    # The caller must successfully reload Gold before calling this function.
    # write_json is a function supplied by the notebook; we call it to save the receipt.
    # The receipt points to the release and lets later runs find and verify it.
    path = context.reports_dir / "gold_completion.json"
    write_json(path, {
        "status": "success",
        "environment": context.environment,
        "run_id": context.run_id,
        "publication_id": manifest["publication_id"],
        "manifest_path": Path(release["manifest_path"]).relative_to(context.environment_root).as_posix(),
        "manifest_sha256": release["manifest_sha256"],
        "parent_hourly": manifest["parent_hourly"],
        # The receipt stores only row counts here; detailed schemas and inventories
        # remain in the manifest. The actual measurements remain in Parquet.
        "datasets": {name: info["records"] for name, info in manifest["datasets"].items()},
    })
    return path
