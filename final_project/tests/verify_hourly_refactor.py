"""VM equivalence check: open a pinned dev release, then prepare from its exact parent."""

import argparse
import json
from pathlib import Path
import sys

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT / "src"))

from run_context import create_run_context
from spark_runtime import configure_vm_python, start_silver_session


def check_metric_cases(spark):
    from silver_io import verify_hourly_records

    schema = "station string, hour int, value double"
    keys = ["station", "hour"]
    cases = [
        ("valid", [("A", 1, None), ("A", 2, 0.0)], 2, None),
        ("empty", [], 0, None),
        ("duplicate", [("A", 1, 1.0), ("A", 1, 2.0)], 2, "keys"),
        ("blank", [(" ", 1, 1.0)], 1, "keys"),
        ("null", [("A", None, 1.0)], 1, "keys"),
        ("wrong count", [("A", 1, 1.0)], 2, "counts"),
    ]
    for name, rows, expected, error in cases:
        table = spark.createDataFrame(rows, schema)
        try:
            verify_hourly_records(table, name, keys, len(rows), expected)
        except ValueError as exc:
            if error is None or error not in str(exc):
                raise
        else:
            if error is not None:
                raise AssertionError(f"{name}: invalid data was accepted")
    print("PASS persisted-table count and key cases")


def check_published_references(spark, context, profile, old_manifest, old_tables, reports):
    """Inspect only the new source release named by this full run's completion reports."""
    from silver_files import silver_child_path, silver_json_sha256
    from silver_io import load_source_silver

    reports = reports.resolve(strict=True)
    source = json.loads((reports / "source_silver_completion.json").read_text())
    hourly = json.loads((reports / "hourly_silver_completion.json").read_text())
    if (source["status"], hourly["status"], source["environment"], hourly["environment"]) != (
        "success", "success", "dev", "dev"
    ) or reports != context.environment_root / "runs" / source["run_id"] / "reports":
        raise ValueError("Expected successful development completion reports under this run's path.")

    manifest, tables, counts = load_source_silver(
        silver_child_path(context.environment_root, source["manifest_path"], must_exist=True),
        context, spark=spark, expected_manifest_sha256=source["manifest_sha256"],
        profile=profile, rule_versions=old_manifest["rule_versions"],
    )
    if (source["run_id"] != hourly["run_id"] or manifest["run_id"] != source["run_id"]
            or hourly["source_silver_publication_id"] != manifest["publication_id"]
            or source["run_id"] == old_manifest["run_id"]):
        raise ValueError("Source and hourly must belong to the same new run.")
    provenance = manifest["provenance"]
    if provenance["bronze_manifests_sha256"] != old_manifest["provenance"]["bronze_manifests_sha256"]:
        raise ValueError("The selected Bronze manifests differ from the baseline.")
    notebook = json.loads((PROJECT / "notebooks/pipeline.ipynb").read_text())
    saved_hash = silver_json_sha256(["".join(c["source"]) for c in notebook["cells"] if c["cell_type"] == "code"])
    if provenance["saved_notebook_code_sha256"]["pipeline.ipynb"] != saved_hash:
        raise ValueError("Selected report was produced from different notebook code.")

    for name in ("epa_sites", "epa_monitors", "noaa_stations", "reference_audit"):
        old, new = old_tables[name], tables[name]
        expected = old_manifest["datasets"][name]["row_count"]
        if expected != (3 if name == "reference_audit" else 1):
            raise ValueError(f"{name}: expected the pinned Alabama 2025 baseline.")
        if new.dtypes != old.dtypes or counts[name] != expected:
            raise ValueError(f"{name}: published schema or count differs.")
        # These selected reference tables have 1-3 rows. Sort map keys before
        # comparison so raw-field/flag maps, nulls and duplicate multiplicity survive.
        def rows(frame):
            return sorted(json.dumps(row.asDict(recursive=True), sort_keys=True, default=str)
                          for row in frame.collect())
        if rows(old) != rows(new):
            raise ValueError(f"{name}: published values, flags or lineage differ.")
        if name == "reference_audit":
            new.select("reference", "entity_id", "disposition", "record_count").orderBy("reference").show(truncate=False)
        else:
            new.select({"epa_sites": "site_id", "epa_monitors": "monitor_id", "noaa_stations": "station_id"}[name],
                       "latitude_deg", "longitude_deg", "quality_flags").show(truncate=False)
        print(f"PASS {name}: {expected} published rows match in every column")
    print("Verified new source publication:", manifest["publication_id"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--sha256", required=True)
    parser.add_argument("--new-reports", type=Path,
                        help="After the full notebook, inspect its source references against the pinned release.")
    args = parser.parse_args()

    configure_vm_python()
    from silver_files import silver_child_path
    from silver_hourly import prepare_hourly_silver
    from silver_io import load_hourly_silver, load_source_silver

    context = create_run_context(PROJECT / "config/environments.json", "dev")
    profile = json.loads((PROJECT / "config/alabama_profile.json").read_text())
    spark = start_silver_session(PROJECT, context)
    try:
        baseline, expected, verified = load_hourly_silver(
            args.manifest, context, spark=spark,
            expected_manifest_sha256=args.sha256, profile=profile,
        )
        print("PASS verified baseline and parent:", baseline["publication_id"])
        parent = baseline["evidence"]["source"]
        manifest, tables, _ = load_source_silver(
            silver_child_path(context.environment_root, parent["manifest_path"], must_exist=True),
            context, spark=spark, expected_manifest_sha256=parent["manifest_sha256"],
            profile=profile, rule_versions=parent["rule_versions"],
        )
        if args.new_reports:
            check_published_references(spark, context, profile, manifest, tables, args.new_reports)
            return
        check_metric_cases(spark)
        result = prepare_hourly_silver(tables, manifest, environment="dev")
        if result["checks"] != baseline["evidence"]["checks"] or result["rules"] != baseline["evidence"]["rules"]:
            raise ValueError("Preparation metrics or rules differ from the baseline.")
        if set(expected) != set(result["tables"]):
            raise ValueError("Preparation returned different tables.")
        for name, saved in expected.items():
            actual = result["tables"][name]
            # Parquet readers can widen nullability. Compare column names, order, types
            # and every value (including nulls and repeated rows) on the same parent.
            if actual.dtypes != saved.dtypes:
                raise ValueError(f"{name}: columns or types differ.")
            if actual.exceptAll(saved).limit(1).count() or saved.exceptAll(actual).limit(1).count():
                raise ValueError(f"{name}: values or multiplicities differ.")
            print(f"PASS {name}: {verified[name]['records']} rows")
        print("All six hourly tables match", baseline["publication_id"])
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
