"""Read versioned Silver bundles and verify their identity, evidence and files."""

import json
from pathlib import Path

from pyspark.sql import functions as F

from silver_files import file_sha256, silver_child_path, silver_json_sha256, verify_silver_file_inventory
from silver_hourly import usable_hourly_key


hourly_output_keys = {
    "yearly_mapping": ["monitor_id", "valid_from_utc"],
    "temperature_hourly": ["station_id", "hour_start_utc"],
    "ozone_enriched": ["monitor_id", "hour_start_utc"],
    "hourly_coverage": ["monitor_id", "hour_start_utc"],
    "monthly_coverage": ["monitor_id", "month_start_utc"],
    "hourly_gaps": ["monitor_id", "gap_kind", "gap_start_utc"],
}


def load_source_silver(
    manifest_path, context, *,
    spark, expected_manifest_sha256, profile, rule_versions,
):
    """Load only the files listed in a validated source Silver manifest."""
    root = Path(context.environment_root).resolve()
    manifest_path = Path(manifest_path).resolve(strict=True)
    if (
        manifest_path.name != "manifest.json"
        or not manifest_path.is_relative_to(
            root / "published" / "silver" / "source"
        )
        or file_sha256(manifest_path) != expected_manifest_sha256
    ):
        raise ValueError(
            "The selected publication manifest path or checksum differs."
        )

    manifest = json.loads(
        manifest_path.read_text(encoding="utf-8")
    )
    validation = manifest["validation"]
    datasets = manifest["datasets"]
    contracts = manifest["contracts"]
    names = {
        "ozone_observations", "temperature_reports",
        "epa_sites", "epa_monitors", "noaa_stations",
        "ozone_audit", "temperature_audit", "reference_audit",
    }
    clean_names = names - {
        "ozone_audit", "temperature_audit", "reference_audit"
    }
    contract_hash = silver_json_sha256(contracts)
    if (
        manifest["manifest_version"] != 1
        or manifest["kind"] != "source_silver"
        or manifest["status"] != "published"
        or manifest["environment"] != context.environment
        or manifest["profile"] != profile
        or manifest["rule_versions"] != rule_versions
        or manifest["storage"]["format"] != "parquet"
        or manifest["publication_id"] != manifest["stage_sha256"]
        or validation["status"] != "validated"
        or validation["run_id"] != manifest["run_id"]
        or validation["stage_sha256"] != manifest["stage_sha256"]
        or manifest["contract_sha256"] != contract_hash
        or validation["contract_sha256"] != contract_hash
        or set(datasets) != names
        or set(contracts) != names
        or set(validation["datasets"]) != names
        or set(validation["audits"]) != clean_names
    ):
        raise ValueError(
            "The manifest does not match the requested source Silver bundle."
        )

    for name, check in validation["audits"].items():
        if (
            check["status"] != "passed"
            or sum(check["dispositions"].values())
            != check["input_records"]
            or check["dispositions"].get("eligible", 0)
            != datasets[name]["row_count"]
        ):
            raise ValueError(
                f"{name}: manifest audit counts do not reconcile."
            )

    region = profile["region"].strip().lower().replace(" ", "_")
    suffix = (
        f"silver/source/{region}/{profile['year']}/"
        f"{manifest['publication_id']}"
    )
    if (
        manifest["bundle_path"] != f"data/{suffix}"
        or manifest_path != silver_child_path(
            root, f"published/{suffix}/manifest.json"
        )
    ):
        raise ValueError(
            "The manifest and data locations do not match their publication."
        )

    bundle = silver_child_path(
        root, manifest["bundle_path"], must_exist=True
    )
    files = verify_silver_file_inventory(bundle, datasets)
    tables, counts = {}, {}

    for name in sorted(names):
        info, contract = datasets[name], contracts[name]
        check = validation["datasets"][name]
        if (
            contract["schema_version"] != f"{name}-v1"
            or check["status"] != "passed"
            or check["records"] != info["row_count"]
            or check["parquet_files"] != len(files[name])
        ):
            raise ValueError(
                f"{name}: unsupported schema or inconsistent validation."
            )

        table = (
            spark.read
            .option("mergeSchema", "true")
            .option("ignoreCorruptFiles", "false")
            .option("ignoreMissingFiles", "false")
            .parquet(*files[name])
        )
        if (
            table.columns != [
                field["name"] for field in info["schema"]["fields"]
            ]
            or dict(table.dtypes) != contract["columns"]
        ):
            raise ValueError(
                f"{name}: published schema differs from its contract."
            )

        count = table.count()
        if (
            count != info["row_count"]
            or (
                contract["expected_rows"] is not None
                and count != contract["expected_rows"]
            )
        ):
            raise ValueError(
                f"{name}: published row count differs."
            )
        tables[name], counts[name] = table, count

    return manifest, tables, counts


def check_hourly_evidence(evidence):
    """Require acceptance bound to the prepared profile, rules and source."""
    acceptance = evidence["acceptance"]
    mapping = evidence["mapping"]
    profile = evidence["profile"]
    coverage = evidence["checks"]["coverage"]
    if (
        not isinstance(acceptance, dict)
        or acceptance.get("status") != "accepted_with_limitations"
        or evidence["checks"]["status"] != "passed"
        or evidence["fixtures"]["status"] != "passed"
        or evidence["rules"]["version"] != "hourly-integration-v1"
        or evidence["rules"]["timezone"] != "UTC"
        or mapping["structural_status"] != "passed"
        or mapping["approval_status"] != "accepted_with_limitations"
        or mapping["review_flags"] != []
        or acceptance["mapping_review_flags"] != []
        or acceptance["region"] != profile["region"]
        or acceptance["year"] != profile["year"]
        or acceptance["profile_sha256"] != silver_json_sha256(profile)
        or mapping["profile_sha256"] != silver_json_sha256(profile)
        or acceptance["rules_sha256"] != silver_json_sha256(evidence["rules"])
        or acceptance["mapping_id"] != mapping["mapping_id"]
        or mapping["approval_decision_sha256"] != silver_json_sha256(acceptance)
        or acceptance["source_silver_publication_id"]
        != evidence["source"]["publication_id"]
        or mapping["source_silver_publication_id"]
        != evidence["source"]["publication_id"]
        or acceptance["coverage"] != {
            name: coverage[name]
            for name in (
                "calendar_hours", "eligible_ozone_hours",
                "eligible_temperature_hours", "shared_hours",
            )
        }
    ):
        raise ValueError("Hourly publication evidence is incomplete or inconsistent.")


def hourly_expected_rows(evidence):
    checks = evidence["checks"]
    return {
        "yearly_mapping": 1,
        "temperature_hourly": checks["temperature"]["hourly_records"],
        "ozone_enriched": checks["enrichment"]["enriched_records"],
        "hourly_coverage": checks["coverage"]["calendar_hours"],
        "monthly_coverage": checks["monthly"]["months"],
        "hourly_gaps": sum(
            item["gap_count"] for item in checks["gaps"].values()
        ),
    }


def verify_hourly_records(table, name, keys, stored_count, expected_count):
    """Decode all fields and verify cardinality and composite keys in one aggregation."""
    # Decode all columns; a count alone may read only Parquet metadata.
    encoded = F.to_json(
        F.struct(*[F.col(column) for column in table.columns]),
        options={"ignoreNullFields": "false"},
    )
    metrics = table.agg(
        F.count("*").alias("records"),
        F.countDistinct(*keys).alias("distinct_keys"),
        F.count(F.when(~usable_hourly_key(keys), 1)).alias("unusable_keys"),
        F.sum(F.length(encoded)).alias("decoded_characters"),
    ).first()
    count = metrics["records"]
    if metrics["unusable_keys"] or count != metrics["distinct_keys"]:
        raise ValueError(f"{name}: missing or repeated hourly keys.")
    if count != stored_count or count != expected_count:
        raise ValueError(f"{name}: record counts differ.")
    return count


def read_hourly_files(bundle, datasets, contracts, *, spark, expected=None):
    """Verify every stored field, row count and key before returning tables."""
    if set(datasets) != set(contracts) or set(contracts) != set(hourly_output_keys):
        raise ValueError("The hourly bundle must contain all six datasets.")
    if spark.conf.get("spark.sql.session.timeZone") != "UTC":
        raise ValueError("Hourly Silver requires UTC.")

    paths = verify_silver_file_inventory(bundle, datasets)
    tables, checks = {}, {}

    for name in sorted(contracts):
        contract, info = contracts[name], datasets[name]
        table = (
            spark.read.option("mergeSchema", "true")
            .option("ignoreCorruptFiles", "false")
            .option("ignoreMissingFiles", "false")
            .parquet(*paths[name])
        )
        if (
            contract["schema_version"] != f"{name}-v1"
            or contract["key"] != hourly_output_keys[name]
            or table.columns != contract["column_order"]
            or table.columns != [field["name"] for field in info["schema"]["fields"]]
            or dict(table.dtypes) != contract["columns"]
        ):
            raise ValueError(f"{name}: schema or key contract differs.")

        table.cache()
        try:
            count = verify_hourly_records(
                table, name, contract["key"], info["row_count"], contract["expected_rows"]
            )
            if expected is not None:
                reference = expected[name].select(*table.columns)
                if (
                    dict(reference.dtypes) != dict(table.dtypes)
                    or reference.exceptAll(table).limit(1).count()
                    or table.exceptAll(reference).limit(1).count()
                ):
                    raise ValueError(
                        f"{name}: persisted values differ from preparation."
                    )

            checks[name] = {
                "status": "passed",
                "records": count,
                "parquet_files": len(paths[name]),
                "complete_values_match": expected is not None,
            }
            tables[name] = table
            print(f"Verified {name}: {count} rows.")
        finally:
            table.unpersist(blocking=True)

    return tables, checks


def load_hourly_silver(
    manifest_path, context, *, spark, expected_manifest_sha256, profile,
):
    """Load an explicitly selected hourly publication for downstream use."""
    root = Path(context.environment_root).resolve()
    path = Path(manifest_path).resolve(strict=True)
    if (
        path.name != "manifest.json"
        or not path.is_relative_to(root / "published/silver/hourly")
        or file_sha256(path) != expected_manifest_sha256
    ):
        raise ValueError("The hourly manifest path or checksum differs.")

    manifest = json.loads(path.read_text(encoding="utf-8"))
    evidence = manifest["evidence"]
    validation = manifest["validation"]
    contracts = manifest["contracts"]
    check_hourly_evidence(evidence)

    expected_id = silver_json_sha256({
        "stage_sha256": manifest["stage_sha256"],
        "evidence_sha256": silver_json_sha256(evidence),
    })
    if (
        manifest["manifest_version"] != 1
        or manifest["kind"] != "hourly_silver"
        or manifest["status"] != "published"
        or manifest["environment"] != context.environment
        or evidence["profile"] != profile
        or manifest["publication_id"] != expected_id
        or manifest["evidence_sha256"] != silver_json_sha256(evidence)
        or manifest["contract_sha256"] != silver_json_sha256(contracts)
        or manifest["storage"]["format"] != "parquet"
        or validation["status"] != "validated"
        or validation["run_id"] != manifest["run_id"]
        or validation["stage_sha256"] != manifest["stage_sha256"]
        or validation["evidence_sha256"] != manifest["evidence_sha256"]
        or validation["contract_sha256"] != manifest["contract_sha256"]
        or set(validation["datasets"]) != set(hourly_output_keys)
        or {name: item["expected_rows"] for name, item in contracts.items()}
        != hourly_expected_rows(evidence)
    ):
        raise ValueError(
            "The hourly publication metadata is inconsistent."
        )

    region = profile["region"].strip().lower().replace(" ", "_")
    suffix = f"silver/hourly/{region}/{profile['year']}/{expected_id}"
    if (
        manifest["bundle_path"] != f"data/{suffix}"
        or path != silver_child_path(
            root, f"published/{suffix}/manifest.json"
        )
    ):
        raise ValueError(
            "The publication is in an unexpected location."
        )

    source = evidence["source"]
    parent, _, _ = load_source_silver(
        silver_child_path(
            root, source["manifest_path"], must_exist=True
        ),
        context,
        spark=spark,
        expected_manifest_sha256=source["manifest_sha256"],
        profile=profile,
        rule_versions=source["rule_versions"],
    )
    if parent["publication_id"] != source["publication_id"]:
        raise ValueError(
            "The source Silver publication identity differs."
        )

    tables, checks = read_hourly_files(
        silver_child_path(
            root, manifest["bundle_path"], must_exist=True
        ),
        manifest["datasets"],
        contracts,
        spark=spark,
    )
    for name, actual in checks.items():
        recorded = validation["datasets"][name]
        if (
            recorded["status"] != "passed"
            or recorded["complete_values_match"] is not True
            or recorded["records"] != actual["records"]
            or recorded["parquet_files"] != actual["parquet_files"]
        ):
            raise ValueError(
                f"{name}: published validation evidence differs."
            )

    return manifest, tables, checks
