"""Synthetic Silver checks called by the notebook.

Each fixture starts with valid input, changes selected fields, and compares the
result with explicit expectations. The large case lists are test data, not extra
pipeline steps. Publication checks use temporary copies of the staged files."""

import hashlib
import json
import time
from collections import Counter
from pathlib import Path

from pyspark.sql import functions as F, types as T


def verify_delimited_readers(*, staging_root, read_validated_ozone_csv, read_validated_delimited_file):
    'Verify CSV and PSV readers using small files in the supplied staging root.'
    staging_root = Path(staging_root)

    reader_fixture_dir = (
        staging_root / f"ozone-reader-{time.time_ns()}"
    )

    reader_fixture_dir.mkdir(parents=True, exist_ok=False)

    reader_fixture_header = "state_code,ozone_raw,note\n"

    # Small files exercise quoting and malformed rows before any Silver field rules run.
    reader_fixture_cases = {
        "valid": "01,0.040,plain\n",
        "empty_final_value": "01,0.040,\n",
        "quoted_comma": '01,0.040,"contains, comma"\n',
        "escaped_quotes": '01,0.040,"uses ""quoted"" text"\n',
        "too_few_fields": "01,0.040\n",
        "too_many_fields": "01,0.040,plain,EXTRA\n",
        "unclosed_quote": '01,0.040,"unfinished\n',
    }

    for case_name, content in reader_fixture_cases.items():
        fixture_path = reader_fixture_dir / f"{case_name}.csv"
        fixture_path.write_text(
            reader_fixture_header + content,
            encoding="utf-8",
        )

    validated_reader_notes = {
        "valid": "plain",
        "empty_final_value": None,
        "quoted_comma": "contains, comma",
        "escaped_quotes": 'uses "quoted" text',
    }

    for case_name, expected_note in validated_reader_notes.items():
        records, structure = read_validated_ozone_csv(
            reader_fixture_dir / f"{case_name}.csv"
        )

        assert structure == {
            "columns": ["state_code", "ozone_raw", "note"],
            "records": 1,
        }, f"{case_name}: unexpected structure summary"

        assert all(
            field.dataType.simpleString() == "string"
            for field in records.schema.fields
        ), f"{case_name}: source fields must remain strings"

        assert [row.asDict() for row in records.collect()] == [{
            "state_code": "01",
            "ozone_raw": "0.040",
            "note": expected_note,
        }], f"{case_name}: incorrect decoded values"

        assert records.count() == structure["records"], (
            f"{case_name}: Spark and structural counts differ"
        )

        print(f"{case_name}: accepted; decoded values verified")


    # These files must be rejected, rather than silently dropping or shifting columns.
    validated_reader_failures = {
        "too_few_fields": "expected 3 fields, found 2",
        "too_many_fields": "expected 3 fields, found 4",
        "unclosed_quote": "CSV parsing failed",
    }

    for case_name, expected_message in validated_reader_failures.items():
        try:
            read_validated_ozone_csv(
                reader_fixture_dir / f"{case_name}.csv"
            )
        except ValueError as error:
            assert expected_message in str(error), (
                f"{case_name}: unexpected rejection: {error}"
            )
            print(f"{case_name}: rejected; reason verified")
        else:
            raise AssertionError(
                f"{case_name}: malformed input was accepted"
            )

    print(
        "Ozone CSV reader fixtures passed: "
        f"{len(validated_reader_notes) + len(validated_reader_failures)} cases."
    )

    # Use a pipe-delimited temperature fixture to test a different separator,
    # including a quoted value that contains a pipe.
    temperature_reader_fixture_dir = (
        staging_root / f"temperature-reader-{time.time_ns()}"
    )
    temperature_reader_fixture_dir.mkdir(parents=True, exist_ok=False)

    temperature_reader_header = "station_id|temperature_raw|note\n"

    temperature_reader_valid_cases = {
        "valid": (
            "USW00053864|11.7|plain\n",
            "plain",
        ),
        "empty_final_value": (
            "USW00053864|11.7|\n",
            None,
        ),
        "quoted_pipe": (
            'USW00053864|11.7|"contains|pipe"\n',
            "contains|pipe",
        ),
        "escaped_quotes": (
            'USW00053864|11.7|"uses ""quoted"" text"\n',
            'uses "quoted" text',
        ),
    }

    for case_name, (content, expected_note) in (
        temperature_reader_valid_cases.items()
    ):
        fixture_path = temperature_reader_fixture_dir / f"{case_name}.psv"
        fixture_path.write_text(
            temperature_reader_header + content,
            encoding="utf-8",
        )

        fixture_records, fixture_structure = read_validated_delimited_file(
            fixture_path,
            delimiter="|",
        )

        assert fixture_structure == {
            "columns": ["station_id", "temperature_raw", "note"],
            "records": 1,
        }, f"{case_name}: unexpected structure summary"

        assert [row.asDict() for row in fixture_records.collect()] == [{
            "station_id": "USW00053864",
            "temperature_raw": "11.7",
            "note": expected_note,
        }], f"{case_name}: incorrect decoded values"

        print(f"{case_name}: accepted; decoded values verified")


    temperature_reader_invalid_cases = {
        "too_few_fields": (
            "USW00053864|11.7\n",
            "expected 3 fields, found 2",
        ),
        "too_many_fields": (
            "USW00053864|11.7|plain|EXTRA\n",
            "expected 3 fields, found 4",
        ),
        "unclosed_quote": (
            'USW00053864|11.7|"unfinished\n',
            "CSV parsing failed",
        ),
        "multiline_record": (
            'USW00053864|11.7|"two\nlines"\n',
            "multiline record",
        ),
    }

    for case_name, (content, expected_message) in (
        temperature_reader_invalid_cases.items()
    ):
        fixture_path = temperature_reader_fixture_dir / f"{case_name}.psv"
        fixture_path.write_text(
            temperature_reader_header + content,
            encoding="utf-8",
        )

        try:
            read_validated_delimited_file(fixture_path, delimiter="|")
        except ValueError as error:
            assert expected_message in str(error), (
                f"{case_name}: unexpected rejection: {error}"
            )
            print(f"{case_name}: rejected; reason verified")
        else:
            raise AssertionError(
                f"{case_name}: unsupported input was accepted"
            )

    print(
        "Temperature PSV reader fixtures passed: "
        f"{len(temperature_reader_valid_cases) + len(temperature_reader_invalid_cases)} "
        "cases."
    )
    return {"ozone_csv_cases": len(validated_reader_notes) + len(validated_reader_failures), "temperature_psv_cases": len(temperature_reader_valid_cases) + len(temperature_reader_invalid_cases)}


def verify_ozone_rules(*, spark, type_ozone_fields, ozone_field_checks, classify_ozone_fields, resolve_source_keys, ozone_key_columns, build_ozone_observations, build_ozone_audit):
    'Verify ozone field rules, key resolution, observation outputs and audit accounting.'
    # Start from a valid ozone report; each case below changes only the fields it needs.
    ozone_fixture_base = {
        "State Code": "01",
        "County Code": "073",
        "Site Num": "1005",
        "Parameter Code": "44201",
        "POC": "1",
        "Date GMT": "2025-06-01",
        "Time GMT": "12:00",
        "Sample Measurement": "0.040",
        "Units of Measure": "Parts per million",
        "MDL": "0.005",
        "Qualifier": None,
    }

    # Each case specifies changed fields, primary disposition and all reasons.
    ozone_fixture_cases = [
        ("valid", {}, "eligible", []),
        ("lower_boundary", {"Sample Measurement": "-0.004"}, "eligible", []),
        ("below_boundary", {"Sample Measurement": "-0.005"},
         "below_minimum", ["below_minimum"]),
        ("qualifier_6", {"Qualifier": "6"}, "qualifier_6", ["qualifier_6"]),
        ("unknown_qualifier", {"Qualifier": "ZZ"},
         "unsupported_qualifier", ["unsupported_qualifier"]),
        ("invalid_value", {"Sample Measurement": "bad"},
         "unusable_ozone", ["unusable_ozone"]),
        ("missing_value", {"Sample Measurement": None},
         "unusable_ozone", ["unusable_ozone"]),
        ("nan_value", {"Sample Measurement": "NaN"},
         "unusable_ozone", ["unusable_ozone"]),
        ("infinite_value", {"Sample Measurement": "Infinity"},
         "unusable_ozone", ["unusable_ozone"]),
        ("invalid_date", {"Date GMT": "2025-02-30"},
         "unusable_timestamp", ["unusable_timestamp"]),
        ("missing_date", {"Date GMT": None},
         "unusable_timestamp", ["unusable_timestamp"]),
        ("wrong_year", {"Date GMT": "2024-06-01"},
         "outside_year", ["outside_year"]),
        ("off_hour", {"Time GMT": "12:30"}, "off_hour", ["off_hour"]),
        ("missing_identifier", {"Site Num": None},
         "missing_identifier", ["missing_identifier"]),
        ("wrong_units", {"Units of Measure": "Parts per billion"},
         "unusable_units", ["unusable_units"]),
        ("missing_mdl", {"MDL": None}, "eligible", []),
        ("overlapping_reasons", {"Sample Measurement": "bad", "Qualifier": "6"},
         "unusable_ozone", ["unusable_ozone", "qualifier_6"]),
    ]

    ozone_fixture_rows = [
        {**ozone_fixture_base, **changes, "case_id": case_id}
        for case_id, changes, _, _ in ozone_fixture_cases
    ]

    ozone_fixture_schema = ", ".join(
        f"`{name}` STRING"
        for name in ["case_id", *ozone_fixture_base]
    )

    ozone_fixture_raw = spark.createDataFrame(
        ozone_fixture_rows,
        schema=ozone_fixture_schema,
    )

    ozone_fixture_typed = type_ozone_fields(ozone_fixture_raw)

    ozone_fixture_checked = ozone_fixture_typed.select(
        "*",
        *[
            condition.alias(name)
            for name, condition in ozone_field_checks.items()
        ],
    )

    ozone_fixture_results = (
        classify_ozone_fields(ozone_fixture_checked, year=2025)
        .select("case_id", "field_disposition", "field_reasons", "field_eligible")
        .collect()
    )

    expected = {
        case_id: (disposition, reasons, disposition == "eligible")
        for case_id, _, disposition, reasons in ozone_fixture_cases
    }

    actual = {
        row["case_id"]: (
            row["field_disposition"],
            row["field_reasons"],
            row["field_eligible"],
        )
        for row in ozone_fixture_results
    }

    mismatches = {
        case_id: {
            "expected": expected.get(case_id),
            "actual": actual.get(case_id),
        }
        for case_id in expected.keys() | actual.keys()
        if expected.get(case_id) != actual.get(case_id)
    }

    assert len(ozone_fixture_results) == len(expected), "Fixture row count changed."
    assert not mismatches, f"Ozone field fixture failures: {mismatches}"

    print(f"Ozone field fixtures passed: {len(expected)}")

    # Now test duplicate and conflicting keys separately from individual field validity.
    key_fixture_base = {
        **ozone_fixture_base,
        "County Name": "Jefferson",
    }

    key_fixture_source = {
        "source_provider": "fixture",
        "source_dataset": "ozone_keys",
        "source_version_sha256": "a" * 64,
        "source_file_path": "fixture/ozone.csv",
        "source_file_sha256": "c" * 64,
        "source_manifest_path": "fixture/manifest.json",
        "source_record_hash_scheme": "sha256-json-raw-fields-v1",
    }

    # Expected counts: input, duplicate extras, eligible, quarantined, field-excluded.
    key_fixture_cases = [
        ("single", 0, [{}], (1, 0, 1, 0, 0)),
        ("exact_copies", 1, [{}, {}, {}], (3, 2, 1, 0, 0)),
        ("value_conflict", 2,
         [{}, {"Sample Measurement": "0.041"}], (2, 0, 0, 2, 0)),
        ("mixed_quality", 3,
         [{}, {"Qualifier": "6"}], (2, 0, 0, 2, 0)),
        ("copies_and_conflict", 4,
         [{}, {}, {"Sample Measurement": "0.041"}], (3, 1, 0, 2, 0)),
        ("missing_key", 5,
         [{"Site Num": None}, {"Site Num": None, "Sample Measurement": "0.041"}],
         (2, 0, 0, 0, 2)),
        ("invalid_time", 6,
         [{"Date GMT": "invalid"},
          {"Date GMT": "invalid", "Sample Measurement": "0.041"}],
         (2, 0, 0, 0, 2)),
        ("separate_versions", 7,
         [{}, {"source_version_sha256": "b" * 64}], (2, 0, 2, 0, 0)),
        ("excluded_copies", 8,
         [{"Qualifier": "6"}, {"Qualifier": "6"}], (2, 1, 0, 0, 1)),
        ("separate_files", 9,
         [{}, {"source_file_path": "fixture/other_ozone.csv"}],
         (2, 0, 2, 0, 0)),
        ("metadata_conflict", 10,
         [{}, {"County Name": "Jefferson County"}], (2, 0, 0, 2, 0)),
        ("different_monitors", 11,
         [{}, {"POC": "2"}], (2, 0, 2, 0, 0)),
    ]

    key_fixture_rows = []
    for case_name, hour, variants, _ in key_fixture_cases:
        for changes in variants:
            record = {
                **key_fixture_base,
                "Time GMT": f"{hour:02d}:00",
                **key_fixture_source,
                **changes,
                "fixture_case": case_name,
            }
            source_content = {
                name: record[name] for name in key_fixture_base
            }
            source_json = json.dumps(
                source_content,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            )
            record["source_record_sha256"] = hashlib.sha256(
                source_json.encode("utf-8")
            ).hexdigest()
            key_fixture_rows.append(record)

    key_fixture_schema = ", ".join(
        f"`{name}` STRING" for name in key_fixture_rows[0]
    )
    key_fixture_raw = spark.createDataFrame(
        key_fixture_rows,
        schema=key_fixture_schema,
    )

    key_fixture_checked = type_ozone_fields(key_fixture_raw).select(
        "*",
        *[
            condition.alias(name)
            for name, condition in ozone_field_checks.items()
        ],
    )
    key_fixture_classified = classify_ozone_fields(
        key_fixture_checked,
        year=2025,
    )

    key_fixture_results = resolve_source_keys(
        key_fixture_classified,
        key_columns=ozone_key_columns,
        key_is_usable=(
            ~F.col("missing_identifier")
            & F.col("timestamp_utc").isNotNull()
        ),
    ).collect()

    key_expected = {
        name: expected for name, _, _, expected in key_fixture_cases
    }
    key_actual = {name: [0, 0, 0, 0, 0] for name in key_expected}

    for row in key_fixture_results:
        counts = key_actual[row["fixture_case"]]
        counts[0] += row["source_occurrences"]
        counts[1] += row["duplicate_extra_records"]

        if row["record_eligible"]:
            counts[2] += 1
        elif row["key_conflict"]:
            counts[3] += 1
        else:
            counts[4] += 1

        if row["key_conflict"]:
            assert row["record_disposition"] == "conflicting_key"

    key_actual = {
        name: tuple(counts) for name, counts in key_actual.items()
    }
    assert key_actual == key_expected, (
        f"Unexpected source-key fixture counts: {key_actual}"
    )

    lineage_fields = [*key_fixture_source, "source_record_sha256"]
    expected_lineage = {
        tuple(row[name] for name in lineage_fields)
        for row in key_fixture_rows
    }
    actual_lineage = {
        tuple(row[name] for name in lineage_fields)
        for row in key_fixture_results
    }
    assert actual_lineage == expected_lineage, "Fixture source lineage changed."

    mixed_quality_reasons = {
        tuple(row["record_reasons"])
        for row in key_fixture_results
        if row["fixture_case"] == "mixed_quality"
    }
    assert mixed_quality_reasons == {
        ("conflicting_key",),
        ("conflicting_key", "qualifier_6"),
    }, "Conflict and field reasons were not preserved."

    print(f"Ozone key fixtures passed: {len(key_fixture_cases)}")

    # The audit must account for every source occurrence, including copies and rejected rows.
    audit_fixture_resolved = resolve_source_keys(
        key_fixture_classified,
        key_columns=ozone_key_columns,
        key_is_usable=(
            ~F.col("missing_identifier")
            & F.col("timestamp_utc").isNotNull()
        ),
    )

    audit_fixture = build_ozone_audit(
        audit_fixture_resolved,
        source_columns=list(key_fixture_base),
        diagnostic_columns=list(ozone_field_checks),
    )

    expected_audit_types = {
        "record_count": "bigint",
        "raw_fields": "map<string,string>",
        "field_flags": "map<string,boolean>",
    }
    for name, expected_type in expected_audit_types.items():
        assert audit_fixture.schema[name].dataType.simpleString() == expected_type, (
            f"Unexpected audit type for {name}."
        )

    audit_fixture_rows = audit_fixture.collect()
    audit_lineage_columns = [*key_fixture_source, "source_record_sha256"]


    def audit_fixture_key(record):
        """Identify fixture source content within its complete lineage."""
        return tuple(record[name] for name in audit_lineage_columns)


    expected_occurrences = Counter(
        audit_fixture_key(row) for row in key_fixture_rows
    )
    expected_raw_fields = {
        audit_fixture_key(row): {
            name: row[name] for name in key_fixture_base
        }
        for row in key_fixture_rows
    }
    resolved_reference = {
        audit_fixture_key(row): row
        for row in key_fixture_results
    }

    actual_occurrences = Counter()
    actual_dispositions = Counter()
    seen_entries = set()

    monitor_columns = [
        "State Code", "County Code", "Site Num", "Parameter Code", "POC"
    ]

    for row in audit_fixture_rows:
        source_key = audit_fixture_key(row)
        assert source_key in expected_raw_fields, "Unexpected audit lineage."

        entry_key = (source_key, row["disposition"])
        assert entry_key not in seen_entries, "Repeated audit entry."
        seen_entries.add(entry_key)

        assert row["record_count"] > 0, "Audit counts must be positive."
        actual_occurrences[source_key] += row["record_count"]
        actual_dispositions[row["disposition"]] += row["record_count"]

        reference = resolved_reference[source_key]
        assert row["raw_fields"] == expected_raw_fields[source_key], (
            "Original source fields changed."
        )
        assert row["field_flags"] == {
            name: reference[name] for name in ozone_field_checks
        }, "Diagnostic flags changed."

        for name in (
            "timestamp_utc",
            "field_disposition",
            "field_reasons",
            "key_variant_count",
            "key_conflict",
        ):
            assert row[name] == reference[name], f"Audit changed {name}."

        expected_monitor_id = (
            None
            if reference["missing_identifier"]
            else "-".join(
                expected_raw_fields[source_key][name]
                for name in monitor_columns
            )
        )
        assert row["monitor_id"] == expected_monitor_id, "Incorrect audit monitor ID."

        if row["disposition"] == "exact_duplicate":
            assert row["record_count"] == reference["duplicate_extra_records"]
            assert row["reasons"] == [
                "exact_duplicate", *reference["record_reasons"]
            ]
        else:
            assert row["record_count"] == 1
            assert row["disposition"] == reference["record_disposition"]
            assert row["reasons"] == reference["record_reasons"]

    expected_dispositions = {
        "eligible": 8,
        "exact_duplicate": 4,
        "conflicting_key": 8,
        "missing_identifier": 2,
        "unusable_timestamp": 2,
        "qualifier_6": 1,
    }

    assert actual_dispositions == expected_dispositions, (
        f"Unexpected audit dispositions: {actual_dispositions}"
    )
    assert actual_occurrences == expected_occurrences, (
        "Audit lost or double-counted source records."
    )
    assert len(audit_fixture_rows) == 24, "Unexpected number of audit entries."

    print(
        f"Ozone audit fixtures passed: {len(audit_fixture_rows)} audit entries "
        f"account for {sum(actual_occurrences.values())} source records."
    )

    # Check the final observation values and source details, not just whether a row passed.
    observation_fixture_base = {
        **key_fixture_base,
        "Date GMT": "2025-06-01",
        "Method Type": "FEM",
        "Method Code": "087",
        "Method Name": "Fixture ozone method",
    }

    # Expected result: ozone_ppm, mdl_ppm, quality_flags, source_occurrences.
    # None means that the case must produce no observation.
    observation_fixture_cases = [
        ("valid", [{}], (0.040, 0.005, [], 1)),
        (
            "negative",
            [{"Sample Measurement": "-0.001"}],
            (-0.001, 0.005, ["negative_ozone", "below_reported_mdl"], 1),
        ),
        (
            "lower_boundary",
            [{"Sample Measurement": "-0.004"}],
            (-0.004, 0.005, ["negative_ozone", "below_reported_mdl"], 1),
        ),
        (
            "below_mdl",
            [{"Sample Measurement": "0.004"}],
            (0.004, 0.005, ["below_reported_mdl"], 1),
        ),
        (
            "equal_to_mdl",
            [{"Sample Measurement": "0.005"}],
            (0.005, 0.005, [], 1),
        ),
        (
            "zero_mdl",
            [{"Sample Measurement": "0.000", "MDL": "0.000"}],
            (0.0, 0.0, [], 1),
        ),
        (
            "qualifier_xs",
            [{"Qualifier": " XS "}],
            (0.040, 0.005, ["source_qualifier_XS"], 1),
        ),
        (
            "qualifier_md",
            [{"Qualifier": "MD"}],
            (0.040, 0.005, ["source_qualifier_MD"], 1),
        ),
        (
            "qualifier_1v",
            [{"Qualifier": "1V"}],
            (0.040, 0.005, ["source_validation_qualifier"], 1),
        ),
        (
            "qualifier_v",
            [{"Qualifier": "V"}],
            (0.040, 0.005, ["source_validation_qualifier"], 1),
        ),
        ("missing_mdl", [{"MDL": None}], (0.040, None, ["missing_mdl"], 1)),
        ("invalid_mdl", [{"MDL": "bad"}], (0.040, None, ["invalid_mdl"], 1)),
        ("nan_mdl", [{"MDL": "NaN"}], (0.040, None, ["nonfinite_mdl"], 1)),
        (
            "infinite_mdl",
            [{"MDL": "Infinity"}],
            (0.040, None, ["nonfinite_mdl"], 1),
        ),
        (
            "negative_mdl",
            [{"Sample Measurement": "-0.001", "MDL": "-0.0005"}],
            (-0.001, None, ["negative_ozone", "negative_mdl"], 1),
        ),
        ("exact_copies", [{}, {}, {}], (0.040, 0.005, [], 3)),
        ("excluded_qualifier", [{"Qualifier": "6"}], None),
        ("below_minimum", [{"Sample Measurement": "-0.005"}], None),
        ("conflicting_key", [{}, {"Sample Measurement": "0.041"}], None),
    ]

    observation_fixture_rows = []
    observation_fixture_expected = {}

    # Each case has a distinct hour; variants within a case share the same key.
    for hour, (case_name, variants, expected) in enumerate(
        observation_fixture_cases
    ):
        case_records = []

        for changes in variants:
            raw_values = {
                **observation_fixture_base,
                "Time GMT": f"{hour:02d}:00",
                **changes,
            }
            raw_json = json.dumps(
                raw_values,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            )
            record = {
                **raw_values,
                **key_fixture_source,
                "source_record_sha256": hashlib.sha256(
                    raw_json.encode("utf-8")
                ).hexdigest(),
            }
            observation_fixture_rows.append(record)
            case_records.append(record)

        if expected is not None:
            timestamp_text = f"2025-06-01 {hour:02d}:00:00"
            observation_fixture_expected[timestamp_text] = (
                case_name,
                case_records[0],
                expected,
            )

    observation_fixture_schema = ", ".join(
        f"`{name}` STRING" for name in observation_fixture_rows[0]
    )
    observation_fixture_raw = spark.createDataFrame(
        observation_fixture_rows,
        schema=observation_fixture_schema,
    )

    observation_fixture_checked = type_ozone_fields(
        observation_fixture_raw
    ).select(
        "*",
        *[
            condition.alias(name)
            for name, condition in ozone_field_checks.items()
        ],
    )
    observation_fixture_classified = classify_ozone_fields(
        observation_fixture_checked,
        year=2025,
    )
    observation_fixture_resolved = resolve_source_keys(
        observation_fixture_classified,
        key_columns=ozone_key_columns,
        key_is_usable=(
            ~F.col("missing_identifier")
            & F.col("timestamp_utc").isNotNull()
        ),
    )
    observation_fixture_output = build_ozone_observations(
        observation_fixture_resolved
    )

    observation_raw_columns = {
        "state_code": "State Code",
        "county_code": "County Code",
        "site_number": "Site Num",
        "parameter_code": "Parameter Code",
        "poc": "POC",
        "ozone_raw": "Sample Measurement",
        "source_unit": "Units of Measure",
        "mdl_raw": "MDL",
        "qualifier_raw": "Qualifier",
        "method_type": "Method Type",
        "method_code": "Method Code",
        "method_name": "Method Name",
    }
    observation_lineage_columns = [
        *key_fixture_source,
        "source_record_sha256",
    ]

    expected_observation_types = {
        name: "string"
        for name in [
            *observation_raw_columns,
            "monitor_id",
            "qualifier_code",
            *observation_lineage_columns,
        ]
    }
    expected_observation_types.update({
        "timestamp_utc": "timestamp",
        "ozone_ppm": "double",
        "mdl_ppm": "double",
        "quality_flags": "array<string>",
        "source_occurrences": "bigint",
    })

    actual_observation_types = {
        field.name: field.dataType.simpleString()
        for field in observation_fixture_output.schema.fields
    }
    assert actual_observation_types == expected_observation_types, (
        f"Unexpected observation schema: {actual_observation_types}"
    )

    # Format timestamps in Spark's UTC session for fixture lookup.
    observation_fixture_results = (
        observation_fixture_output
        .withColumn(
            "_fixture_time_utc",
            F.date_format("timestamp_utc", "yyyy-MM-dd HH:mm:ss"),
        )
        .collect()
    )

    actual_by_time = {}
    for row in observation_fixture_results:
        timestamp_text = row["_fixture_time_utc"]
        assert timestamp_text not in actual_by_time, (
            f"Repeated output key: {timestamp_text}"
        )
        actual_by_time[timestamp_text] = row

    assert set(actual_by_time) == set(observation_fixture_expected), (
        "Missing eligible observations or unexpected excluded observations: "
        f"missing={set(observation_fixture_expected) - set(actual_by_time)}, "
        f"unexpected={set(actual_by_time) - set(observation_fixture_expected)}"
    )

    expected_qualifier_codes = {
        "qualifier_xs": "XS",
        "qualifier_md": "MD",
        "qualifier_1v": "1V",
        "qualifier_v": "V",
    }

    for timestamp_text, (
        case_name,
        source_record,
        expected,
    ) in observation_fixture_expected.items():
        row = actual_by_time[timestamp_text]
        expected_ozone, expected_mdl, expected_flags, expected_occurrences = (
            expected
        )

        expected_values = {
            "monitor_id": "01-073-1005-44201-1",
            "ozone_ppm": expected_ozone,
            "mdl_ppm": expected_mdl,
            "quality_flags": expected_flags,
            "source_occurrences": expected_occurrences,
            "qualifier_code": expected_qualifier_codes.get(case_name, ""),
        }

        for name, expected_value in expected_values.items():
            assert row[name] == expected_value, (
                f"{case_name}: {name}={row[name]!r}; "
                f"expected {expected_value!r}"
            )

        for output_name, source_name in observation_raw_columns.items():
            assert row[output_name] == source_record[source_name], (
                f"{case_name}: changed source field {source_name}"
            )

        for name in observation_lineage_columns:
            assert row[name] == source_record[name], (
                f"{case_name}: changed lineage field {name}"
            )

    print(
        "Ozone observation fixtures passed: "
        f"{len(observation_fixture_cases)} cases, "
        f"{len(observation_fixture_results)} output records."
    )
    return {"field_cases": len(ozone_fixture_cases), "key_cases": len(key_fixture_cases), "observation_cases": len(observation_fixture_cases), "observation_records": len(observation_fixture_results), "audit_entries": len(audit_fixture_rows), "audited_source_records": sum(actual_occurrences.values())}


def verify_temperature_rules(*, spark, profile, temperature_columns, type_temperature_fields, temperature_field_checks, classify_temperature_fields, resolve_temperature_keys, build_temperature_observations, build_temperature_audit):
    'Verify temperature field rules, key resolution, observation outputs and audit accounting.'
    alabama_profile = profile
    temperature_station_id = profile["weather_mapping"]["noaa_station_id"]
    temperature_inspection_columns = temperature_columns

    temperature_fixture_year = alabama_profile["year"]

    temperature_fixture_defaults = {
        "STATION": temperature_station_id,
        "DATE": f"{temperature_fixture_year}-06-01T12:53:00",
        "temperature": "11.7",
        "temperature_Measurement_Code": None,
        "temperature_Quality_Code": "5",
        "temperature_Report_Type": "FM15",
        "temperature_Source_Code": "343",
        "temperature_Source_Station_ID": "fixture-source",
        "wind_speed": "3.0",
    }

    # Each case supplies source-field changes and independently expected reasons.
    temperature_field_cases = [
        ("valid_343", {}, []),
        (
            "valid_223",
            {"temperature_Source_Code": "223", "temperature_Quality_Code": "1"},
            [],
        ),
        ("negative", {"temperature": "-10.0"}, []),
        ("zero", {"temperature": "0.0"}, []),
        ("lower_boundary", {"temperature": "-100.0"}, []),
        ("upper_boundary", {"temperature": "60.0"}, []),
        (
            "below_range",
            {"temperature": "-100.1"},
            ["outside_temperature_range"],
        ),
        (
            "above_range",
            {"temperature": "60.1"},
            ["outside_temperature_range"],
        ),
        ("missing_value", {"temperature": None}, ["missing_temperature"]),
        ("blank_value", {"temperature": "   "}, ["missing_temperature"]),
        ("invalid_value", {"temperature": "warm"}, ["invalid_temperature"]),
        ("nan", {"temperature": "NaN"}, ["nonfinite_temperature"]),
        ("positive_infinity", {"temperature": "Infinity"}, ["nonfinite_temperature"]),
        ("negative_infinity", {"temperature": "-Infinity"}, ["nonfinite_temperature"]),
        ("missing_station", {"STATION": None}, ["missing_station"]),
        ("wrong_station", {"STATION": "OTHER_STATION"}, ["unexpected_station"]),
        ("missing_time", {"DATE": None}, ["missing_timestamp"]),
        ("invalid_time", {"DATE": "not-a-date"}, ["invalid_timestamp"]),
        (
            "outside_year",
            {"DATE": f"{temperature_fixture_year + 1}-01-01T00:00:00"},
            ["outside_year"],
        ),
        (
            "year_start",
            {"DATE": f"{temperature_fixture_year}-01-01T00:00:00"},
            [],
        ),
        (
            "year_end",
            {"DATE": f"{temperature_fixture_year}-12-31T23:59:59"},
            [],
        ),
        ("synoptic_report", {"temperature_Report_Type": "FM12"}, []),
        ("special_report", {"temperature_Report_Type": "FM16"}, []),
        (
            "unknown_report",
            {"temperature_Report_Type": "UNKNOWN"},
            ["unsupported_report_type"],
        ),
        (
            "missing_report",
            {"temperature_Report_Type": None},
            ["unsupported_report_type"],
        ),
        (
            "unknown_measurement",
            {"temperature_Measurement_Code": "X"},
            ["unsupported_measurement_code"],
        ),
        (
            "metadata_whitespace",
            {
                "temperature_Source_Code": " 343 ",
                "temperature_Quality_Code": " 5 ",
                "temperature_Report_Type": " FM15 ",
                "temperature_Measurement_Code": "   ",
            },
            [],
        ),
        (
            "unknown_source",
            {"temperature_Source_Code": "999", "temperature_Quality_Code": None},
            ["unsupported_source_quality"],
        ),
        (
            "missing_source",
            {"temperature_Source_Code": None},
            ["unsupported_source_quality"],
        ),
        (
            "missing_quality",
            {"temperature_Quality_Code": None},
            ["unsupported_source_quality"],
        ),
        (
            "unselected_quality_pair",
            {"temperature_Quality_Code": "1"},
            ["unsupported_source_quality"],
        ),
        (
            "quality_code_wrong_source",
            {"temperature_Source_Code": "223", "temperature_Quality_Code": "6"},
            ["unsupported_source_quality"],
        ),
        (
            "missing_original_station",
            {"temperature_Source_Station_ID": None},
            [],
        ),
        ("unused_wind_invalid", {"wind_speed": "not-a-number"}, []),
        (
            "multiple_reasons",
            {
                "temperature": None,
                "temperature_Source_Code": "999",
                "temperature_Quality_Code": None,
                "temperature_Report_Type": "UNKNOWN",
                "temperature_Measurement_Code": "X",
            },
            [
                "missing_temperature",
                "unsupported_source_quality",
                "unsupported_report_type",
                "unsupported_measurement_code",
            ],
        ),
    ]

    # Cover the rejected legacy codes for each supported source family.
    for fixture_source, fixture_quality in [
        ("343", "2"),
        ("343", "3"),
        ("343", "6"),
        ("343", "7"),
        ("223", "2"),
        ("223", "3"),
        ("223", "5"),
    ]:
        temperature_field_cases.append((
            f"rejected_{fixture_source}_{fixture_quality}",
            {
                "temperature_Source_Code": fixture_source,
                "temperature_Quality_Code": fixture_quality,
            },
            ["rejected_source_quality"],
        ))

    temperature_fixture_inputs = {}
    temperature_fixture_expected = {}

    for case_id, changes, expected_reasons in temperature_field_cases:
        assert case_id not in temperature_fixture_inputs, case_id
        assert not (set(changes) - set(temperature_fixture_defaults)), case_id

        temperature_fixture_inputs[case_id] = {
            **temperature_fixture_defaults,
            **changes,
        }
        temperature_fixture_expected[case_id] = expected_reasons

    # Explicit string types allow fixtures containing null source values.
    temperature_fixture_schema = T.StructType()
    temperature_fixture_schema.add("fixture_id", "string", nullable=False)

    for field_name in temperature_fixture_defaults:
        temperature_fixture_schema.add(field_name, "string", nullable=True)

    temperature_fixture_raw = spark.createDataFrame(
        [
            {"fixture_id": case_id, **source_values}
            for case_id, source_values in temperature_fixture_inputs.items()
        ],
        schema=temperature_fixture_schema,
    )

    temperature_fixture_typed = type_temperature_fields(
        temperature_fixture_raw
    )

    temperature_fixture_checked = temperature_fixture_typed.select(
        "*",
        *[
            condition.alias(name)
            for name, condition in temperature_field_checks.items()
        ],
    )

    temperature_fixture_classified = classify_temperature_fields(
        temperature_fixture_checked,
        year=temperature_fixture_year,
        station_id=temperature_station_id,
        source_quality_pairs=[("223", "1"), ("343", "5")],
    )

    temperature_fixture_results = (
        temperature_fixture_classified
        .withColumn(
            "fixture_timestamp_text",
            F.date_format("timestamp_utc", "yyyy-MM-dd'T'HH:mm:ss"),
        )
        .collect()
    )

    # Verify one output per input, including excluded records.
    assert len(temperature_fixture_results) == len(temperature_field_cases)

    temperature_fixture_actual = {
        row["fixture_id"]: row
        for row in temperature_fixture_results
    }

    assert (
        set(temperature_fixture_actual)
        == set(temperature_fixture_expected)
    )

    for case_id, expected_reasons in temperature_fixture_expected.items():
        actual = temperature_fixture_actual[case_id]
        expected_disposition = (
            expected_reasons[0] if expected_reasons else "eligible"
        )

        assert actual["field_reasons"] == expected_reasons, (
            case_id,
            actual["field_reasons"],
            expected_reasons,
        )
        assert actual["field_eligible"] == (not expected_reasons), case_id
        assert actual["field_disposition"] == expected_disposition, case_id

        for field_name, original in temperature_fixture_inputs[case_id].items():
            assert actual[field_name] == original, (case_id, field_name)

    # Accepted measurements retain their values, including negative boundaries.
    for case_id, expected_value in {
        "negative": -10.0,
        "zero": 0.0,
        "lower_boundary": -100.0,
        "upper_boundary": 60.0,
    }.items():
        assert (
            temperature_fixture_actual[case_id]["temperature_value"]
            == expected_value
        ), case_id

    # Preserve actual report times rather than rounding to clock hours.
    for case_id in ("valid_343", "year_start", "year_end"):
        assert (
            temperature_fixture_actual[case_id]["fixture_timestamp_text"]
            == temperature_fixture_inputs[case_id]["DATE"]
        ), case_id

    normalized = temperature_fixture_actual["metadata_whitespace"]
    assert [
        normalized[name]
        for name in ("source_code", "quality_code", "report_type", "measurement_code")
    ] == ["343", "5", "FM15", ""]

    print(
        f"Temperature field fixtures passed: "
        f"{len(temperature_field_cases)} cases."
    )

    # Test reports sharing a station and timestamp: exact copies, conflicting
    # readings, and matching temperature evidence from different report types.
    temperature_key_base = dict(temperature_fixture_defaults)

    temperature_key_source = {
        "source_provider": "fixture",
        "source_dataset": "temperature_keys",
        "source_version_sha256": "a" * 64,
        "source_file_path": "fixture/weather.psv",
        "source_file_sha256": "c" * 64,
        "source_manifest_path": "fixture/manifest.json",
        "source_record_hash_scheme": "sha256-json-raw-fields-v1",
    }

    # Expected counts:
    # input, duplicate extras, eligible, quarantined, redundant, field-excluded.
    temperature_key_cases = [
        ("single", 0, [{}], (1, 0, 1, 0, 0, 0)),
        ("exact_copies", 1, [{}, {}, {}], (3, 2, 1, 0, 0, 0)),
        (
            "value_conflict", 2,
            [{}, {"temperature": "12.0"}],
            (2, 0, 0, 2, 0, 0),
        ),
        (
            "mixed_quality", 3,
            [{}, {"temperature_Quality_Code": "6"}],
            (2, 0, 0, 2, 0, 0),
        ),
        (
            "copies_and_conflict", 4,
            [{}, {}, {"temperature": "12.0"}],
            (3, 1, 0, 2, 0, 0),
        ),
        (
            "missing_station", 5,
            [{"STATION": None}, {"STATION": None, "temperature": "12.0"}],
            (2, 0, 0, 0, 0, 2),
        ),
        (
            "invalid_time", 6,
            [{"DATE": "invalid"}, {"DATE": "invalid", "temperature": "12.0"}],
            (2, 0, 0, 0, 0, 2),
        ),
        (
            "separate_versions", 7,
            [{}, {"source_version_sha256": "b" * 64, "temperature": "12.0"}],
            (2, 0, 2, 0, 0, 0),
        ),
        (
            "excluded_copies", 8,
            [{"temperature": None}, {"temperature": None}],
            (2, 1, 0, 0, 0, 1),
        ),
        (
            "separate_files", 9,
            [{}, {
                "source_file_path": "fixture/other_weather.psv",
                "temperature": "12.0",
            }],
            (2, 0, 2, 0, 0, 0),
        ),
        (
            "source_station_conflict", 10,
            [{}, {"temperature_Source_Station_ID": "another-source-station"}],
            (2, 0, 0, 2, 0, 0),
        ),
        (
            "different_stations", 11,
            [{}, {"STATION": "OTHER_STATION"}],
            (2, 0, 1, 0, 0, 1),
        ),
        (
            "other_weather_only", 12,
            [{}, {}, {"wind_speed": "9.0"}],
            (3, 1, 1, 0, 1, 0),
        ),
        (
            "excluded_other_weather", 13,
            [{"temperature": None}, {"temperature": None, "wind_speed": "9.0"}],
            (2, 0, 0, 0, 0, 2),
        ),
        (
            "report_conflict", 14,
            [{}, {"temperature_Report_Type": "FM16"}],
            (2, 0, 0, 2, 0, 0),
        ),
        (
            "raw_value_format", 15,
            [{}, {"temperature": "11.70"}],
            (2, 0, 0, 2, 0, 0),
        ),
        (
            "missing_time", 16,
            [{"DATE": None}, {"DATE": None, "temperature": "12.0"}],
            (2, 0, 0, 0, 0, 2),
        ),
        (
            "raw_quality_format", 17,
            [{}, {"temperature_Quality_Code": " 5 "}],
            (2, 0, 0, 2, 0, 0),
        ),
    ]

    temperature_key_rows = []

    for case_name, hour, variants, _ in temperature_key_cases:
        for changes in variants:
            record = {
                **temperature_key_base,
                "DATE": (
                    f"{temperature_fixture_year}-06-01T{hour:02d}:53:00"
                ),
                **temperature_key_source,
                **changes,
                "fixture_case": case_name,
            }

            source_content = {
                name: record[name]
                for name in temperature_key_base
            }
            source_json = json.dumps(
                source_content,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            )
            record["source_record_sha256"] = hashlib.sha256(
                source_json.encode("utf-8")
            ).hexdigest()

            temperature_key_rows.append(record)

    temperature_key_schema = ", ".join(
        f"`{name}` STRING"
        for name in temperature_key_rows[0]
    )

    temperature_key_raw = spark.createDataFrame(
        temperature_key_rows,
        schema=temperature_key_schema,
    )

    temperature_key_checked = type_temperature_fields(
        temperature_key_raw
    ).select(
        "*",
        *[
            condition.alias(name)
            for name, condition in temperature_field_checks.items()
        ],
    )

    temperature_key_classified = classify_temperature_fields(
        temperature_key_checked,
        year=temperature_fixture_year,
        station_id=temperature_station_id,
        source_quality_pairs=[("223", "1"), ("343", "5")],
    )

    temperature_key_resolved = resolve_temperature_keys(
        temperature_key_classified.repartition(2),
        source_columns=list(temperature_key_base),
        temperature_columns=temperature_inspection_columns,
    )
    temperature_key_results = temperature_key_resolved.collect()

    temperature_key_expected = {
        name: expected
        for name, _, _, expected in temperature_key_cases
    }
    temperature_key_actual = {
        name: [0, 0, 0, 0, 0, 0]
        for name in temperature_key_expected
    }

    # Match complete input content and lineage, preserving multiplicity.
    temperature_key_identity_columns = list(temperature_key_rows[0])

    temperature_expected_occurrences = Counter(
        tuple(record[name] for name in temperature_key_identity_columns)
        for record in temperature_key_rows
    )
    temperature_actual_occurrences = Counter()
    temperature_seen_records = set()

    temperature_unusable_key_cases = {
        "missing_station", "invalid_time", "missing_time",
    }
    temperature_separate_key_cases = {
        "separate_versions", "separate_files", "different_stations",
    }

    for row in temperature_key_results:
        case_name = row["fixture_case"]
        expected = temperature_key_expected[case_name]
        counts = temperature_key_actual[case_name]

        identity = tuple(
            row[name] for name in temperature_key_identity_columns
        )
        assert identity in temperature_expected_occurrences, case_name
        assert identity not in temperature_seen_records, case_name
        temperature_seen_records.add(identity)
        temperature_actual_occurrences[identity] += row["source_occurrences"]

        assert row["duplicate_extra_records"] == row["source_occurrences"] - 1
        assert row["record_eligible"] == (
            row["record_disposition"] == "eligible"
        )
        assert not (
            row["key_conflict"] and row["redundant_temperature"]
        )

        counts[0] += row["source_occurrences"]
        counts[1] += row["duplicate_extra_records"]

        if row["record_eligible"]:
            counts[2] += 1
        elif row["key_conflict"]:
            counts[3] += 1
        elif row["redundant_temperature"]:
            counts[4] += 1
        else:
            counts[5] += 1

        if row["key_conflict"]:
            assert row["record_disposition"] == "conflicting_key"
            expected_prefix = ["conflicting_key"]
        elif row["redundant_temperature"]:
            assert row["record_disposition"] == "redundant_temperature"
            expected_prefix = ["redundant_temperature"]
        else:
            assert row["record_disposition"] == row["field_disposition"]
            expected_prefix = []

        assert row["record_reasons"] == (
            expected_prefix + row["field_reasons"]
        ), case_name

        if case_name in temperature_unusable_key_cases:
            assert row["key_variant_count"] is None, case_name
            assert row["key_source_records"] is None, case_name
        else:
            expected_variants = 2 if expected[3] > 0 else 1
            expected_key_records = (
                1 if case_name in temperature_separate_key_cases
                else expected[0]
            )
            assert row["key_variant_count"] == expected_variants, case_name
            assert row["key_source_records"] == expected_key_records, case_name

    temperature_key_actual = {
        name: tuple(counts)
        for name, counts in temperature_key_actual.items()
    }

    assert temperature_key_actual == temperature_key_expected, (
        f"Unexpected temperature key counts: {temperature_key_actual}"
    )
    assert temperature_actual_occurrences == temperature_expected_occurrences, (
        "Source content, lineage or occurrence counts changed."
    )

    # A conflict retains the rejected report's field-level explanation.
    temperature_mixed_reasons = {
        tuple(row["record_reasons"])
        for row in temperature_key_results
        if row["fixture_case"] == "mixed_quality"
    }
    assert temperature_mixed_reasons == {
        ("conflicting_key",),
        ("conflicting_key", "rejected_source_quality"),
    }

    # Equivalent eligible reports use the lowest fingerprint.
    temperature_expected_winner = min(
        record["source_record_sha256"]
        for record in temperature_key_rows
        if record["fixture_case"] == "other_weather_only"
    )
    temperature_actual_winners = [
        row["source_record_sha256"]
        for row in temperature_key_results
        if row["fixture_case"] == "other_weather_only"
        and row["record_eligible"]
    ]
    assert temperature_actual_winners == [temperature_expected_winner]

    # Changing input partitions must preserve the complete resolved results.
    temperature_key_replayed = resolve_temperature_keys(
        temperature_key_classified.repartition(3),
        source_columns=list(temperature_key_base),
        temperature_columns=temperature_inspection_columns,
    ).collect()


    def temperature_key_result_signature(row):
        """Serialize fixture results for order-independent comparison."""
        return json.dumps(
            row.asDict(recursive=True),
            sort_keys=True,
            default=str,
        )


    assert Counter(
        temperature_key_result_signature(row)
        for row in temperature_key_results
    ) == Counter(
        temperature_key_result_signature(row)
        for row in temperature_key_replayed
    ), "Temperature key resolution changed after repartitioning."

    print(
        f"Temperature key fixtures passed: {len(temperature_key_cases)} cases, "
        f"{len(temperature_key_rows)} source records."
    )

    # Add output-specific cases to check cleaned values and the source information kept with them.
    temperature_output_extra_cases = [
        ("output_negative", 0, {"temperature": "-10.0"}),
        ("output_lower_boundary", 1, {"temperature": "-100.0"}),
        ("output_upper_boundary", 2, {"temperature": "60.0"}),
        ("output_zero", 3, {"temperature": "0.0"}),
        (
            "output_missing_source_station", 4,
            {"temperature_Source_Station_ID": None},
        ),
        (
            "output_blank_source_station", 5,
            {"temperature_Source_Station_ID": "   "},
        ),
        (
            "output_empty_source_station", 6,
            {"temperature_Source_Station_ID": ""},
        ),
        (
            "output_normalized_metadata", 7,
            {
                "temperature_Source_Code": " 223 ",
                "temperature_Quality_Code": " 1 ",
                "temperature_Report_Type": " FM16 ",
                "temperature_Measurement_Code": "   ",
                "temperature_Source_Station_ID": " ICAO-TEST ",
            },
        ),
    ]

    temperature_output_rows = [
        dict(record) for record in temperature_key_rows
    ]

    for case_name, hour, changes in temperature_output_extra_cases:
        record = {
            **temperature_key_base,
            "DATE": f"{temperature_fixture_year}-06-02T{hour:02d}:53:00",
            **temperature_key_source,
            **changes,
            "fixture_case": case_name,
        }

        source_json = json.dumps(
            {name: record[name] for name in temperature_key_base},
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        record["source_record_sha256"] = hashlib.sha256(
            source_json.encode("utf-8")
        ).hexdigest()

        temperature_output_rows.append(record)

    temperature_output_raw = spark.createDataFrame(
        temperature_output_rows,
        schema=temperature_key_schema,
    )

    temperature_output_checked = type_temperature_fields(
        temperature_output_raw
    ).select(
        "*",
        *[
            condition.alias(name)
            for name, condition in temperature_field_checks.items()
        ],
    )

    temperature_output_classified = classify_temperature_fields(
        temperature_output_checked,
        year=temperature_fixture_year,
        station_id=temperature_station_id,
        source_quality_pairs=[("223", "1"), ("343", "5")],
    )

    temperature_output_resolved = resolve_temperature_keys(
        temperature_output_classified,
        source_columns=list(temperature_key_base),
        temperature_columns=temperature_inspection_columns,
    )

    temperature_output_fixture = build_temperature_observations(
        temperature_output_resolved
    )

    temperature_output_lineage_columns = [
        "source_provider",
        "source_dataset",
        "source_version_sha256",
        "source_file_path",
        "source_file_sha256",
        "source_manifest_path",
        "source_record_hash_scheme",
        "source_record_sha256",
    ]

    temperature_output_expected_types = {
        "station_id": "string",
        "timestamp_utc": "timestamp",
        "temperature_c": "double",
        "temperature_raw": "string",
        "timestamp_raw": "string",
        "source_code": "string",
        "quality_code": "string",
        "report_type": "string",
        "measurement_code": "string",
        "source_station_id": "string",
        "source_code_raw": "string",
        "quality_code_raw": "string",
        "report_type_raw": "string",
        "measurement_code_raw": "string",
        "source_station_id_raw": "string",
        "quality_flags": "array<string>",
        "source_occurrences": "bigint",
        "supporting_source_records": "bigint",
        **{
            name: "string"
            for name in temperature_output_lineage_columns
        },
    }

    assert temperature_output_fixture.columns == list(
        temperature_output_expected_types
    ), "Unexpected temperature observation columns or order."

    assert {
        field.name: field.dataType.simpleString()
        for field in temperature_output_fixture.schema.fields
    } == temperature_output_expected_types, (
        "Unexpected temperature observation types."
    )


    def temperature_output_lineage_key(record):
        """Identify a fixture source row within its complete lineage."""
        return tuple(
            record[name] for name in temperature_output_lineage_columns
        )


    # Select expected representatives independently of the output DataFrame.
    temperature_expected_output_cases = {
        "single",
        "exact_copies",
        "separate_versions",
        "separate_files",
        "different_stations",
        "other_weather_only",
    } | {
        name for name, _, _ in temperature_output_extra_cases
    }

    temperature_expected_output = {}

    for record in temperature_output_rows:
        case_name = record["fixture_case"]

        if case_name not in temperature_expected_output_cases:
            continue

        if (
            case_name == "different_stations"
            and record["STATION"] != temperature_station_id
        ):
            continue

        if (
            case_name == "other_weather_only"
            and record["source_record_sha256"] != temperature_expected_winner
        ):
            continue

        temperature_expected_output[
            temperature_output_lineage_key(record)
        ] = record

    assert len(temperature_expected_output) == 16

    temperature_expected_copy_counts = Counter(
        temperature_output_lineage_key(record)
        for record in temperature_output_rows
    )

    temperature_output_scope_columns = [
        "source_provider",
        "source_dataset",
        "source_version_sha256",
        "source_file_path",
        "source_file_sha256",
        "STATION",
        "DATE",
    ]

    temperature_expected_support_counts = Counter(
        tuple(record[name] for name in temperature_output_scope_columns)
        for record in temperature_output_rows
    )

    # Compare UTC timestamp text without depending on the driver's local timezone.
    temperature_output_results = (
        temperature_output_fixture
        .withColumn(
            "timestamp_utc",
            F.date_format("timestamp_utc", "yyyy-MM-dd'T'HH:mm:ss"),
        )
        .collect()
    )

    assert len(temperature_output_results) == 16

    temperature_output_raw_mapping = {
        "station_id": "STATION",
        "temperature_raw": "temperature",
        "timestamp_raw": "DATE",
        "source_code_raw": "temperature_Source_Code",
        "quality_code_raw": "temperature_Quality_Code",
        "report_type_raw": "temperature_Report_Type",
        "measurement_code_raw": "temperature_Measurement_Code",
        "source_station_id_raw": "temperature_Source_Station_ID",
    }

    temperature_missing_source_station_cases = {
        "output_missing_source_station",
        "output_blank_source_station",
        "output_empty_source_station",
    }

    temperature_seen_output = set()

    for row in temperature_output_results:
        lineage_key = temperature_output_lineage_key(row)

        assert lineage_key in temperature_expected_output, (
            "Unexpected observation or changed lineage."
        )
        assert lineage_key not in temperature_seen_output, (
            "Repeated observation representative."
        )
        temperature_seen_output.add(lineage_key)

        original = temperature_expected_output[lineage_key]
        case_name = original["fixture_case"]

        for output_name, source_name in temperature_output_raw_mapping.items():
            assert row[output_name] == original[source_name], (
                case_name, output_name
            )

        assert row["timestamp_utc"] == original["DATE"], case_name
        assert row["temperature_c"] == float(original["temperature"]), case_name

        expected_metadata = {
            "source_code": "343",
            "quality_code": "5",
            "report_type": "FM15",
            "measurement_code": "",
            "source_station_id": "fixture-source",
        }
        expected_flags = []

        if case_name in temperature_missing_source_station_cases:
            expected_metadata["source_station_id"] = None
            expected_flags = ["missing_source_station_id"]

        if case_name == "output_normalized_metadata":
            expected_metadata.update({
                "source_code": "223",
                "quality_code": "1",
                "report_type": "FM16",
                "source_station_id": "ICAO-TEST",
            })

        for name, expected_value in expected_metadata.items():
            assert row[name] == expected_value, (case_name, name)

        assert row["quality_flags"] == expected_flags, case_name

        assert row["source_occurrences"] == (
            temperature_expected_copy_counts[lineage_key]
        ), case_name

        source_key = tuple(
            original[name] for name in temperature_output_scope_columns
        )
        assert row["supporting_source_records"] == (
            temperature_expected_support_counts[source_key]
        ), case_name

    assert temperature_seen_output == set(temperature_expected_output), (
        "Expected temperature observations are missing."
    )

    print(
        "Temperature observation fixtures passed: "
        f"{len(temperature_key_cases) + len(temperature_output_extra_cases)} cases, "
        f"{len(temperature_output_results)} output records."
    )

    # Check that the temperature audit accounts for all input records and preserves rejection reasons.
    temperature_audit_fixture = build_temperature_audit(
        temperature_output_resolved,
        source_columns=list(temperature_key_base),
        diagnostic_columns=list(temperature_field_checks),
    )

    temperature_audit_expected_types = {
        "record_count": "bigint",
        "raw_fields": "map<string,string>",
        "field_flags": "map<string,boolean>",
        "field_reasons": "array<string>",
        "reasons": "array<string>",
    }

    for name, expected_type in temperature_audit_expected_types.items():
        actual_type = (
            temperature_audit_fixture.schema[name].dataType.simpleString()
        )
        assert actual_type == expected_type, (
            f"Unexpected audit type for {name}: {actual_type}"
        )

    temperature_audit_fixture_rows = temperature_audit_fixture.collect()

    temperature_audit_reference = {
        temperature_output_lineage_key(row): row
        for row in temperature_output_resolved.collect()
    }

    temperature_audit_expected_occurrences = Counter(
        temperature_output_lineage_key(record)
        for record in temperature_output_rows
    )

    temperature_audit_expected_raw = {
        temperature_output_lineage_key(record): {
            name: record[name]
            for name in temperature_key_base
        }
        for record in temperature_output_rows
    }

    assert set(temperature_audit_reference) == set(
        temperature_audit_expected_occurrences
    ), "Resolved fixture source identities differ from the input."

    temperature_audit_actual_occurrences = Counter()
    temperature_audit_actual_dispositions = Counter()
    temperature_audit_seen_entries = set()
    temperature_audit_eligible_keys = set()

    for row in temperature_audit_fixture_rows:
        source_key = temperature_output_lineage_key(row)

        assert source_key in temperature_audit_expected_raw, (
            "Unexpected audit lineage."
        )

        entry_key = (source_key, row["disposition"])
        assert entry_key not in temperature_audit_seen_entries, (
            "Repeated audit entry."
        )
        temperature_audit_seen_entries.add(entry_key)

        assert row["record_count"] > 0, "Audit counts must be positive."

        temperature_audit_actual_occurrences[source_key] += row["record_count"]
        temperature_audit_actual_dispositions[
            row["disposition"]
        ] += row["record_count"]

        reference = temperature_audit_reference[source_key]
        original = temperature_audit_expected_raw[source_key]

        assert row["raw_fields"] == original, (
            "Original source fields changed."
        )
        assert row["field_flags"] == {
            name: reference[name]
            for name in temperature_field_checks
        }, "Field diagnostic flags changed."

        for name in (
            "timestamp_utc",
            "field_disposition",
            "field_reasons",
            "key_variant_count",
            "key_source_records",
            "key_conflict",
            "redundant_temperature",
        ):
            assert row[name] == reference[name], (
                f"Audit changed {name}."
            )

        expected_station_id = (
            None
            if reference["missing_station"]
            else original["STATION"]
        )
        assert row["station_id"] == expected_station_id, (
            "Incorrect audit station identifier."
        )

        if row["disposition"] == "exact_duplicate":
            assert row["record_count"] == reference["duplicate_extra_records"]
            assert row["reasons"] == [
                "exact_duplicate",
                *reference["record_reasons"],
            ]
        else:
            assert row["record_count"] == 1
            assert row["disposition"] == reference["record_disposition"]
            assert row["reasons"] == reference["record_reasons"]

        if row["disposition"] == "eligible":
            temperature_audit_eligible_keys.add(source_key)

    temperature_audit_expected_dispositions = {
        "eligible": 16,
        "exact_duplicate": 5,
        "conflicting_key": 14,
        "redundant_temperature": 1,
        "missing_station": 2,
        "invalid_timestamp": 2,
        "missing_temperature": 3,
        "unexpected_station": 1,
        "missing_timestamp": 2,
    }

    assert (
        temperature_audit_actual_dispositions
        == temperature_audit_expected_dispositions
    ), (
        "Unexpected audit dispositions: "
        f"{temperature_audit_actual_dispositions}"
    )

    assert (
        temperature_audit_actual_occurrences
        == temperature_audit_expected_occurrences
    ), "Audit lost or double-counted source records."

    temperature_observation_fixture_keys = {
        temperature_output_lineage_key(row)
        for row in temperature_output_results
    }

    assert (
        temperature_audit_eligible_keys
        == temperature_observation_fixture_keys
    ), "Eligible audit entries and observation identities differ."

    assert len(temperature_audit_fixture_rows) == 45, (
        "Unexpected number of audit entries."
    )

    print(
        "Temperature audit fixtures passed: "
        f"{len(temperature_audit_fixture_rows)} audit entries account for "
        f"{sum(temperature_audit_actual_occurrences.values())} source records."
    )
    return {"field_cases": len(temperature_field_cases), "key_cases": len(temperature_key_cases), "observation_cases": len(temperature_key_cases) + len(temperature_output_extra_cases), "observation_records": len(temperature_output_results), "audit_entries": len(temperature_audit_fixture_rows), "audited_source_records": sum(temperature_audit_actual_occurrences.values())}


def _reference_fixture_inputs():
    """Return valid raw reference records to use as the starting point for each test case."""
    epa = {
        "State Code": "01", "County Code": "073", "Site Number": "1005",
        "Latitude": "30.0", "Longitude": "-90.0", "Datum": "WGS84",
        "Extraction Date": "2026-06-26", "fixture_note": "original",
    }
    return {
        "epa_sites": {
            **epa, "Elevation": "100.0",
            "Site Established Date": "1987-06-17", "Site Closed Date": None,
            "Local Site Name": " Fixture site ",
        },
        "epa_monitors": {
            **epa, "Parameter Code": "44201", "POC": "1",
            "First Year of Data": "1987", "Last Sample Date": "2026-03-31",
            "Parameter Name": " Ozone ",
        },
        "noaa_stations": {
            "GHCN_ID": "USW00053864", "LATITUDE": "30.0", "LONGITUDE": "-90.0",
            "ELEVATION": "100.0", "NAME": " Fixture station ",
            "STATE": "AL", "ICAO": "KEET", "ISO_CODE": "US",
            "fixture_note": "original",
        },
    }


def _reference_fixture_frame(spark, rows):
    """Build a string-valued Spark table, like the raw reference input before conversion."""
    columns = sorted(rows[0])
    schema = T.StructType([
        T.StructField(column, T.StringType(), True) for column in columns
    ])
    return spark.createDataFrame(rows, schema)


def verify_reference_fields(
    *, spark, type_reference_fields, classify_reference_fields,
):
    """Check reference field rules against explicit expected reasons and flags."""
    total_cases = 0
    for name, base in _reference_fixture_inputs().items():
        is_epa = name.startswith("epa_")
        key = "State Code" if is_epa else "GHCN_ID"
        key_reason = (
            "missing_identifier_state_code" if is_epa
            else "missing_identifier_ghcn_id"
        )
        lat = "Latitude" if is_epa else "LATITUDE"
        lon = "Longitude" if is_epa else "LONGITUDE"

        # Each case supplies changes, exclusion reasons and optional flags.
        cases = [
            ("valid", {}, (), ()),
            ("missing_key", {key: None}, (key_reason,), ()),
            ("blank_key", {key: " "}, (key_reason,), ()),
            ("missing_lat", {lat: None}, ("missing_latitude_deg",), ()),
            ("invalid_lat", {lat: "bad"}, ("invalid_latitude_deg",), ()),
            ("nonfinite_lat", {lat: "NaN"}, ("nonfinite_latitude_deg",), ()),
            ("high_lat", {lat: "90.1"}, ("latitude_out_of_range",), ()),
            ("low_lat", {lat: "-90.1"}, ("latitude_out_of_range",), ()),
            ("missing_lon", {lon: ""}, ("missing_longitude_deg",), ()),
            ("invalid_lon", {lon: "bad"}, ("invalid_longitude_deg",), ()),
            ("nonfinite_lon", {lon: "Infinity"}, ("nonfinite_longitude_deg",), ()),
            ("high_lon", {lon: "180.1"}, ("longitude_out_of_range",), ()),
            ("low_lon", {lon: "-180.1"}, ("longitude_out_of_range",), ()),
            ("upper_lower_bounds", {lat: "90", lon: "-180"}, (), ()),
            ("lower_upper_bounds", {lat: "-90", lon: "180"}, (), ()),
            ("two_failures", {key: None, lat: "bad"},
             (key_reason, "invalid_latitude_deg"), ()),
        ]
        if is_epa:
            cases += [
                ("missing_datum", {"Datum": " "}, (), ("missing_datum",)),
                ("other_datum", {"Datum": "NAD83"}, (), ("non_wgs84_datum",)),
            ]
        if name != "epa_monitors":
            elevation = "Elevation" if is_epa else "ELEVATION"
            cases += [
                ("missing_elevation", {elevation: None}, (), ("missing_elevation_m",)),
                ("invalid_elevation", {elevation: "bad"}, (), ("invalid_elevation_m",)),
                ("nonfinite_elevation", {elevation: "NaN"}, (), ("nonfinite_elevation_m",)),
                ("negative_elevation", {elevation: "-5"}, (), ()),
            ]
        if name == "epa_sites":
            cases += [
                ("invalid_closure", {"Site Closed Date": "bad"},
                 (), ("invalid_site_closed_date",)),
                ("early_closure", {"Site Closed Date": "1980-01-01"},
                 (), ("closure_before_establishment",)),
                ("late_closure", {"Site Closed Date": "2027-01-01"},
                 (), ("closure_after_extraction",)),
                ("late_establishment", {"Site Established Date": "2027-01-01"},
                 (), ("establishment_after_extraction",)),
                ("missing_establishment", {"Site Established Date": None},
                 (), ("missing_site_established_date",)),
            ]
        elif name == "epa_monitors":
            cases += [
                ("invalid_year", {"First Year of Data": "bad"},
                 (), ("invalid_first_year_of_data",)),
                ("zero_year", {"First Year of Data": "0"},
                 (), ("first_year_out_of_range",)),
                ("high_year", {"First Year of Data": "10000"}, (),
                 ("first_year_out_of_range", "first_year_after_last_sample",
                  "first_year_after_extraction")),
                ("future_year", {"First Year of Data": "2027"}, (),
                 ("first_year_after_last_sample", "first_year_after_extraction")),
                ("early_sample", {"Last Sample Date": "1980-01-01"},
                 (), ("first_year_after_last_sample",)),
                ("late_sample", {"Last Sample Date": "2027-01-01"},
                 (), ("last_sample_after_extraction",)),
                ("missing_sample", {"Last Sample Date": None},
                 (), ("missing_last_sample_date",)),
                ("invalid_extraction", {"Extraction Date": "bad"},
                 (), ("invalid_extraction_date",)),
            ]
        else:
            cases.append((
                "missing_elevation_code", {"ELEVATION": "-999.9"},
                (), ("missing_elevation_m",),
            ))

        raw = _reference_fixture_frame(spark, [
            {**base, **changes, "_case": label}
            for label, changes, _, _ in cases
        ])
        typed = type_reference_fields(raw, name)
        types = dict(typed.dtypes)
        assert types["latitude_deg"] == types["longitude_deg"] == "double"
        if name == "epa_monitors":
            assert types["first_year_of_data"] == "int"
            assert types["last_sample_date"] == "date"
        elif name == "epa_sites":
            assert types["site_established_date"] == "date"
            assert types["site_closed_date"] == "date"

        actual = {
            row["_case"]: row
            for row in classify_reference_fields(typed, name).collect()
        }
        assert len(actual) == len(cases), f"{name}: missing fixture results"
        for label, _, reasons, flags in cases:
            row = actual[label]
            expected_disposition = reasons[0] if reasons else "eligible"
            assert row["field_disposition"] == expected_disposition, (name, label, row)
            assert row["field_reasons"] == list(reasons), (name, label, row)
            assert row["field_eligible"] == (not reasons), (name, label, row)
            assert sorted(row["reference_quality_flags"]) == sorted(flags), (name, label, row)

        total_cases += len(cases)
        print(f"{name}: field fixtures passed: {len(cases)}")

    return {"field_cases": total_cases}


def verify_reference_outputs(
    *, spark, type_reference_fields, classify_reference_fields,
    resolve_reference_keys, build_reference_outputs,
):
    """Check reference keys, cleaned values, lineage and counted audits."""
    # Each reference has a different identifier, but follows the same duplicate and audit rules.
    key_specs = {
        "epa_sites": (
            "site_id", "01-073-1005",
            {"State Code": "state_code", "County Code": "county_code",
             "Site Number": "site_number"},
        ),
        "epa_monitors": (
            "monitor_id", "01-073-1005-44201-1",
            {"State Code": "state_code", "County Code": "county_code",
             "Site Number": "site_number", "Parameter Code": "parameter_code",
             "POC": "poc"},
        ),
        "noaa_stations": (
            "station_id", "USW00053864", {"GHCN_ID": "station_id"},
        ),
    }
    totals = Counter()
    for name, base in _reference_fixture_inputs().items():
        id_column, expected_id, keys = key_specs[name]
        first_key = next(iter(keys))
        entity_change, alternate_id = {
            "epa_sites": ({"Site Number": "1006"}, "01-073-1006"),
            "epa_monitors": ({"POC": "2"}, "01-073-1005-44201-2"),
            "noaa_stations": ({"GHCN_ID": "USW00013874"}, "USW00013874"),
        }[name]
        key_reason = (
            "missing_identifier_state_code" if name.startswith("epa_")
            else "missing_identifier_ghcn_id"
        )
        lat = "LATITUDE" if name == "noaa_stations" else "Latitude"
        optional_raw, optional_typed, normal_optional = {
            "epa_sites": ("Elevation", "elevation_m", 100.0),
            "epa_monitors": ("First Year of Data", "first_year_of_data", 1987),
            "noaa_stations": ("ELEVATION", "elevation_m", 100.0),
        }[name]
        unusable_value, unusable_flag = (
            ("0", "first_year_out_of_range") if name == "epa_monitors"
            else ("NaN", "nonfinite_elevation_m")
        )
        warning_change, warning_flag, warning_column, warning_value = {
            "epa_sites": (
                {"Site Closed Date": "1980-01-01"},
                "closure_before_establishment", "site_closed_date", "1980-01-01",
            ),
            "epa_monitors": (
                {"Last Sample Date": "2027-01-01"},
                "last_sample_after_extraction", "last_sample_date", "2027-01-01",
            ),
            "noaa_stations": (
                {"ELEVATION": "-999.9"},
                "missing_elevation_m", "elevation_m", None,
            ),
        }[name]

        # Rows contain raw-field changes and a synthetic source-version label.
        a = ({}, "a")
        changed = ({"fixture_note": "changed"}, "a")
        invalid = ({lat: None}, "a")
        cases = [
            ("valid", [a], {"eligible": 1}, 1),
            ("exact_copies", [a, a, a], {"eligible": 1, "exact_duplicate": 2}, 1),
            ("conflict", [a, changed], {"conflicting_key": 2}, 0),
            ("conflict_with_invalid", [a, invalid], {"conflicting_key": 2}, 0),
            ("missing_key", [({first_key: None}, "a")], {key_reason: 1}, 0),
            ("missing_coordinate", [invalid], {"missing_latitude_deg": 1}, 0),
            ("invalid_optional", [({optional_raw: "bad"}, "a")], {"eligible": 1}, 1),
            ("unusable_optional", [({optional_raw: unusable_value}, "a")],
             {"eligible": 1}, 1),
            ("optional_warning", [(warning_change, "a")], {"eligible": 1}, 1),
            ("separate_versions", [a, ({lat: "31.0"}, "b")], {"eligible": 2}, 2),
            ("separate_entities", [a, (entity_change, "a")], {"eligible": 2}, 2),
            ("invalid_copies", [invalid, invalid],
             {"missing_latitude_deg": 1, "exact_duplicate": 1}, 0),
            ("conflicting_copies", [a, a, changed],
             {"conflicting_key": 2, "exact_duplicate": 1}, 0),
        ]
        rows, originals, expected_occurrences = [], {}, Counter()
        expected_dispositions, expected_outputs, expected_flags = {}, {}, {}
        labels = {}
        for label, variants, dispositions, output_count in cases:
            path = f"fixtures/{name}/{label}.csv"
            labels[path] = label
            expected_dispositions[path] = Counter(dispositions)
            expected_outputs[path] = output_count
            expected_flags[path] = {
                "invalid_optional": [f"invalid_{optional_typed}"],
                "unusable_optional": [unusable_flag],
                "optional_warning": [warning_flag],
            }.get(label, [])
            for index, (changes, version) in enumerate(variants):
                raw = {**base, **changes}
                fingerprint = hashlib.sha256(
                    json.dumps(raw, sort_keys=True).encode("utf-8")
                ).hexdigest()
                provider, dataset = name.split("_", 1)
                lineage = {
                    "source_provider": provider, "source_dataset": dataset,
                    "source_version_sha256": version * 64,
                    "source_file_path": path, "source_file_sha256": "c" * 64,
                    "source_manifest_path": f"fixtures/{name}/{label}/{version}.json",
                    "source_record_hash_scheme": "fixture-sha256-json-v1",
                    "source_record_sha256": fingerprint,
                }
                identity = (path, version * 64, fingerprint)
                originals[identity] = (raw, lineage)
                expected_occurrences[identity] += 1
                rows.append({**raw, **lineage, "_reverse_flags": str(index % 2)})

        # Run the fixture through the same conversion and classification functions as the notebook.
        raw_frame = _reference_fixture_frame(spark, rows)
        classified = classify_reference_fields(
            type_reference_fields(raw_frame, name), name,
        )
        classified = classified.withColumn(
            "reference_field_flags",
            F.when(
                F.col("_reverse_flags") == "1",
                F.map_from_entries(F.reverse(F.map_entries("reference_field_flags"))),
            ).otherwise(F.col("reference_field_flags")),
        ).drop("_reverse_flags")

        resolved = resolve_reference_keys(classified, key_columns=list(keys))
        result = build_reference_outputs(resolved, name, list(base))
        clean_rows = result["reference"].collect()
        audit_rows = result["audit"].collect()

        # First check the audit: each input occurrence must be counted under the right reason.
        actual_dispositions = {path: Counter() for path in labels}
        audited_occurrences, output_counts = Counter(), Counter()
        for row in audit_rows:
            path = row["source_file_path"]
            identity = (path, row["source_version_sha256"], row["source_record_sha256"])
            raw, lineage = originals[identity]
            assert row["raw_fields"] == raw, (name, labels[path], "raw fields")
            assert all(row[column] == value for column, value in lineage.items())
            assert row["reference"] == name
            assert row["record_count"] > 0
            actual_dispositions[path][row["disposition"]] += row["record_count"]
            audited_occurrences[identity] += row["record_count"]

            reasons = (
                [key_reason] if raw[first_key] is None
                else ["missing_latitude_deg"] if raw[lat] is None else []
            )
            conflict = labels[path] in {
                "conflict", "conflict_with_invalid", "conflicting_copies",
            }
            row_id = (
                alternate_id if all(raw[k] == v for k, v in entity_change.items())
                else expected_id
            )
            assert row["entity_id"] == (None if raw[first_key] is None else row_id)
            assert row["field_reasons"] == reasons
            assert row["field_flags"][key_reason] == (raw[first_key] is None)
            assert row["field_flags"]["missing_latitude_deg"] == (raw[lat] is None)
            assert all(row["field_flags"][flag] for flag in expected_flags[path])
            assert row["field_disposition"] == (reasons[0] if reasons else "eligible")
            assert row["key_conflict"] == conflict
            assert row["key_variant_count"] == (
                None if raw[first_key] is None else 2 if conflict else 1
            )
            assert sorted(row["reference_quality_flags"]) == sorted(expected_flags[path])
            expected_reasons = (["conflicting_key"] if conflict else []) + reasons
            if row["disposition"] == "exact_duplicate":
                expected_reasons = ["exact_duplicate"] + expected_reasons
            assert row["reasons"] == expected_reasons

        assert actual_dispositions == expected_dispositions, (name, actual_dispositions)
        assert audited_occurrences == expected_occurrences, (name, "source accounting")

        # Then check the usable reference rows, including values, warnings and original source details.
        for row in clean_rows:
            path = row["source_file_path"]
            label = labels[path]
            identity = (path, row["source_version_sha256"], row["source_record_sha256"])
            raw, lineage = originals[identity]
            output_counts[path] += 1
            assert all(row[column] == value for column, value in lineage.items())
            row_id = (
                alternate_id if all(raw[k] == v for k, v in entity_change.items())
                else expected_id
            )
            assert row[id_column] == row_id
            assert all(row[output] == raw[original] for original, output in keys.items())
            assert row["source_occurrences"] == expected_occurrences[identity]
            assert row["latitude_deg"] == (
                31.0 if row["source_version_sha256"] == "b" * 64 else 30.0
            )
            assert row["longitude_deg"] == -90.0
            assert sorted(row["quality_flags"]) == sorted(expected_flags[path])
            if label in {"invalid_optional", "unusable_optional"}:
                assert row[optional_typed] is None
            elif label == "optional_warning" and name == "noaa_stations":
                assert row[optional_typed] is None
            else:
                assert row[optional_typed] == normal_optional
            if label == "optional_warning":
                value = row[warning_column]
                assert (str(value) if value is not None else None) == warning_value
            text_column, text_value = {
                "epa_sites": ("site_name", "Fixture site"),
                "epa_monitors": ("parameter_name", "Ozone"),
                "noaa_stations": ("station_name", "Fixture station"),
            }[name]
            assert row[text_column] == text_value

        assert all(output_counts[path] == count for path, count in expected_outputs.items())
        output_types = dict(result["reference"].dtypes)
        assert output_types[id_column] == "string"
        assert output_types["quality_flags"] == "array<string>"
        assert dict(result["audit"].dtypes)["raw_fields"] == "map<string,string>"
        assert dict(result["audit"].dtypes)["field_flags"] == "map<string,boolean>"
        totals.update(
            key_output_audit_cases=len(cases), source_records=len(rows),
            output_records=len(clean_rows), audit_entries=len(audit_rows),
        )
        print(f"{name}: key, output and audit fixtures passed: {len(cases)}")

    return dict(totals)


def verify_silver_publication(
    *, stage, validation, contracts, context, profile, rule_versions,
    publish_source_silver, verify_silver_file_inventory, silver_json_sha256,
):
    """Exercise publication using isolated copies of a validated bundle."""
    from copy import deepcopy
    from tempfile import TemporaryDirectory
    from types import SimpleNamespace
    import shutil

    source_bundle = Path(context.environment_root) / stage["bundle_path"]
    verify_silver_file_inventory(source_bundle, stage["datasets"])
    results = []

    def passed(name):
        """Record and print one successfully checked publication case."""
        results.append(name)
        print(f"{name}: passed")

    def rejected(name, exception_type, message, action):
        """Require the expected error type and message, so an unrelated failure cannot pass."""
        try:
            action()
        except exception_type as error:
            assert message in str(error), (name, str(error))
        else:
            raise AssertionError(
                f"{name}: expected {exception_type.__name__}"
            )
        passed(name)

    with TemporaryDirectory(
        prefix="silver-publication-fixtures-",
        dir=context.staging_root,
    ) as temporary:
        root = Path(temporary).resolve()
        fixture_context = SimpleNamespace(
            environment="fixture",
            environment_root=root,
            run_id="publication-fixtures",
            staging_root=root / "staging" / "publication-fixtures",
        )

        def new_case(name):
            """Copy the validated bundle into a separate temporary folder for this test case."""
            candidate = deepcopy(stage)
            candidate.update({
                "environment": fixture_context.environment,
                "environment_root": str(root),
                "run_id": fixture_context.run_id,
                "bundle_path": f"staging/{fixture_context.run_id}/{name}",
            })
            destination = root / candidate["bundle_path"]
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copytree(source_bundle, destination)

            report = deepcopy(validation)
            report.update({
                "run_id": fixture_context.run_id,
                "bundle_path": candidate["bundle_path"],
                "stage_sha256": silver_json_sha256(candidate),
            })
            return candidate, report

        def publish(candidate, report):
            """Call the real source publisher with the isolated fixture context."""
            return publish_source_silver(
                candidate, report, contracts, fixture_context,
                profile=profile,
                rule_versions=rule_versions,
                provenance={"code_source": "publication_fixture"},
            )

        def locations(candidate):
            """Find the data and manifest paths that this fixture would publish."""
            region = profile["region"].strip().lower().replace(" ", "_")
            suffix = (
                f"silver/source/{region}/{profile['year']}/"
                f"{silver_json_sha256(candidate)}"
            )
            return (
                root / f"data/{suffix}",
                root / f"published/{suffix}/manifest.json",
            )

        # Establish a valid release before testing retries and deliberately broken attempts.
        first_stage, first_report = new_case("successful")
        first = publish(first_stage, first_report)
        first_manifest = Path(first["manifest_path"])
        first_manifest_bytes = first_manifest.read_bytes()
        first_bundle, _ = locations(first_stage)

        assert first["outcome"] == "published"
        assert not (root / first_stage["bundle_path"]).exists()
        assert first_manifest.is_file() and first_bundle.is_dir()
        verify_silver_file_inventory(first_bundle, first_stage["datasets"])
        passed("successful_publication")

        # Repeating the same publication should reuse the release without changing its manifest.
        repeated = publish(first_stage, first_report)
        assert repeated["outcome"] == "reused"
        assert first_manifest.read_bytes() == first_manifest_bytes
        passed("identical_attempt_reused")

        # Each case breaks one thing in its own copy, so failures cannot affect real published data.
        failure_cases = [
            ("failed_validation", ValueError, "matching stage"),
            ("changed_receipt", ValueError, "matching stage"),
            ("changed_file", ValueError, "checksum differs"),
            ("missing_file", FileNotFoundError, ""),
            ("extra_file", ValueError, "inventory differs"),
            ("existing_lock", RuntimeError, "lock already exists"),
            (
                "destination_collision",
                FileExistsError,
                "without a publication manifest",
            ),
            (
                "manifest_write_failure",
                IsADirectoryError,
                "manifest.json.tmp",
            ),
        ]
        lock = root / "published" / "silver" / "source" / ".publish.lock"

        for name, exception_type, message in failure_cases:
            candidate, report = new_case(name)
            staged = root / candidate["bundle_path"]
            info = next(iter(candidate["datasets"].values()))
            data_file = staged / info["files"][0]["path"]
            final_bundle, manifest_path = locations(candidate)

            if name == "failed_validation":
                report["status"] = "failed"
            elif name == "changed_receipt":
                candidate["elapsed_seconds"] += 1
            elif name == "changed_file":
                with data_file.open("r+b") as handle:
                    first_byte = handle.read(1)
                    handle.seek(0)
                    handle.write(bytes([first_byte[0] ^ 1]))
            elif name == "missing_file":
                data_file.unlink()
            elif name == "extra_file":
                shutil.copyfile(
                    data_file, data_file.parent / "unexpected.parquet"
                )
            elif name == "existing_lock":
                lock.mkdir()
            elif name == "destination_collision":
                final_bundle.mkdir(parents=True)
            elif name == "manifest_write_failure":
                manifest_path.parent.mkdir(parents=True)
                manifest_path.with_suffix(".json.tmp").mkdir()

            try:
                rejected(
                    name, exception_type, message,
                    lambda: publish(candidate, report),
                )
                assert not manifest_path.exists(), name
                if name == "existing_lock":
                    assert lock.is_dir(), (
                        "The publisher removed another owner's lock."
                    )
                else:
                    assert not lock.exists(), (
                        "The publisher left its lock behind."
                    )
            finally:
                if name == "existing_lock":
                    lock.rmdir()

            if name == "manifest_write_failure":
                assert final_bundle.is_dir() and not staged.exists()
                rejected(
                    "incomplete_attempt_not_overwritten",
                    FileExistsError,
                    "without a publication manifest",
                    lambda: publish(candidate, report),
                )

        assert first_manifest.read_bytes() == first_manifest_bytes
        verify_silver_file_inventory(first_bundle, first_stage["datasets"])
        passed("previous_publication_preserved")

    print(f"Silver publication fixtures passed: {len(results)} cases.")
    return {"cases": len(results), "passed": results}


def verify_hourly_integration(
    *, spark, profile, references, build_yearly_mapping,
    select_hourly_temperature, enrich_hourly_ozone, prepare_hourly_silver,
):
    """Check hourly integration with synthetic records and leap-year coverage."""
    passed = []

    def check(label, condition):
        """Stop on a failed expectation; otherwise record the completed hourly check."""
        if not condition:
            raise AssertionError(label)
        passed.append(label)

    def rejects(label, action, message):
        """Check that an invalid hourly input raises the expected validation message."""
        try:
            action()
        except ValueError as error:
            check(label, message in str(error))
        else:
            raise AssertionError(f"{label}: expected ValueError")

    # Use a leap year to check the full calendar, including February and the final hour.
    fixture_profile = json.loads(json.dumps(profile))
    fixture_profile.update(region="Fixture", year=2024)
    monitor_id = "-".join(fixture_profile["ozone_monitor"][key] for key in (
        "state_code", "county_code", "site_number", "parameter_code", "poc"
    ))
    station_id = fixture_profile["weather_mapping"]["noaa_station_id"]

    def timed(rows, schema):
        """Convert the fixture timestamp strings into Spark timestamps for hourly processing."""
        return spark.createDataFrame(rows, schema).withColumn(
            "timestamp_utc", F.to_timestamp("timestamp_utc")
        )

    ozone = timed([
        (monitor_id, "2024-01-31 23:00:00", 0.040, "o1", []),
        (monitor_id, "2024-02-01 00:00:00", 0.050, "o2", []),
        (monitor_id, "2024-02-01 01:00:00", -0.001, "o3", ["negative_ozone"]),
        (monitor_id, "2024-12-31 23:00:00", 0.020, "o4", []),
    ], "monitor_id STRING, timestamp_utc STRING, ozone_ppm DOUBLE, "
       "source_record_sha256 STRING, quality_flags ARRAY<STRING>")

    reports = timed([
        (station_id, "2024-01-31 23:05:00", 1.0, "t1", "FM15", []),
        (station_id, "2024-01-31 23:55:00", 2.0, "t2", "FM16", []),
        (station_id, "2024-02-01 01:00:00", 3.0, "t3", "FM15", []),
        (station_id, "2024-12-31 23:59:00", 4.0, "t4", "FM15", []),
    ], "station_id STRING, timestamp_utc STRING, temperature_c DOUBLE, "
       "source_record_sha256 STRING, report_type STRING, quality_flags ARRAY<STRING>")

    # The audit includes extra copies, excluded hours and an unassignable timestamp.
    # These counts must stay distinct from the number of usable hourly readings.
    ozone_audit = ozone.select("monitor_id", "timestamp_utc").withColumn(
        "record_count", F.lit(1).cast("long")
    ).unionByName(timed([
        (monitor_id, "2024-01-31 23:00:00", 2),
        (monitor_id, "2024-02-01 02:00:00", 1),
        (monitor_id, None, 1),
    ], "monitor_id STRING, timestamp_utc STRING, record_count LONG"))

    finite_flags = {
        "missing_temperature": False,
        "invalid_temperature": False,
        "nonfinite_temperature": False,
    }
    temperature_audit = timed([
        (station_id, timestamp, 1, finite_flags)
        for timestamp in (
            "2024-01-31 23:05:00", "2024-01-31 23:55:00",
            "2024-02-01 01:00:00", "2024-12-31 23:59:00",
            "2024-02-01 00:00:00", None,
        )
    ] + [
        (station_id, "2024-02-01 02:00:00", 1,
         {**finite_flags, "missing_temperature": True}),
    ], "station_id STRING, timestamp_utc STRING, record_count LONG, "
       "field_flags MAP<STRING,BOOLEAN>")

    tables = {
        **{name: references[name] for name in (
            "epa_sites", "epa_monitors", "noaa_stations"
        )},
        "ozone_observations": ozone,
        "temperature_reports": reports,
        "ozone_audit": ozone_audit,
        "temperature_audit": temperature_audit,
    }
    # Synthetic metadata models the input contract; it is not published.
    manifest = {
        "kind": "source_silver", "status": "published",
        "environment": "fixture", "publication_id": "f" * 64,
        "profile": fixture_profile,
        "validation": {
            "status": "validated",
            "audits": {
                "ozone_observations": {"input_records": 8},
                "temperature_reports": {"input_records": 7},
            },
        },
    }
    bundle = prepare_hourly_silver(tables, manifest, environment="fixture")
    outputs, checks = bundle["tables"], bundle["checks"]

    def keyed(table):
        """Index the small fixture output by hour so individual results are easy to check."""
        return {
            row["hour"]: row.asDict()
            for row in table.withColumn(
                "hour", F.date_format("hour_start_utc", "yyyy-MM-dd HH:mm")
            ).collect()
        }

    weather = keyed(outputs["temperature_hourly"].withColumn(
        "report_time", F.date_format("report_timestamp_utc", "yyyy-MM-dd HH:mm")
    ))
    enriched = keyed(outputs["ozone_enriched"])
    monthly = {
        row["month"]: row.asDict()
        for row in outputs["monthly_coverage"].withColumn(
            "month", F.date_format("month_start_utc", "yyyy-MM")
        ).collect()
    }

    check("Latest report and its lineage", (
        weather["2024-01-31 23:00"]["temperature_c"],
        weather["2024-01-31 23:00"]["source_record_sha256"],
        weather["2024-01-31 23:00"]["report_time"],
        weather["2024-01-31 23:00"]["qualifying_report_count"],
    ) == (2.0, "t2", "2024-01-31 23:55", 2))
    check("Exact-hour report stays in its hour", "2024-02-01 01:00" in weather)
    check("Final year hour is included", "2024-12-31 23:00" in weather)
    check("Ozone rows retained", len(enriched) == 4)

    missing = enriched["2024-02-01 00:00"]
    check("Missing temperature remains null and flagged",
          missing["temperature_c"] is None
          and missing["integration_flags"] == ["no_eligible_temperature"])
    check("Negative ozone and flags preserved",
          enriched["2024-02-01 01:00"]["ozone_ppm"] == -0.001
          and enriched["2024-02-01 01:00"]["quality_flags"] == ["negative_ozone"])
    check("Separate source lineage",
          enriched["2024-01-31 23:00"]["source_record_sha256"] == "o1"
          and enriched["2024-01-31 23:00"]["temperature_source_record_sha256"] == "t2")
    check("Leap-year calendar", checks["coverage"]["calendar_hours"] == 8784)
    check("All months including leap February",
          len(monthly) == 12 and monthly["2024-02"]["calendar_hours"] == 696)
    check("February match denominator",
          monthly["2024-02"]["temperature_match_pct"] == 50.0)
    check("Zero denominator has null percentage and reason",
          monthly["2024-03"]["temperature_match_pct"] is None
          and monthly["2024-03"]["temperature_match_reason"] == "no_eligible_ozone_hours")
    check("Operating history remains unknown",
          all(row["documented_ozone_operating_hours"] is None
              and row["documented_temperature_operating_hours"] is None
              for row in monthly.values()))
    check("Audit copies counted without extra hours",
          checks["coverage"]["assigned_ozone_source_records"] == 7
          and checks["coverage"]["observed_ozone_hours"] == 5)
    check("Unassignable audit records retained in accounting",
          checks["audit_assignment"]["ozone"]["unassigned_source_records"] == 1
          and checks["audit_assignment"]["temperature"]["unassigned_source_records"] == 1)
    check("Numeric and eligible temperature differ",
          checks["coverage"]["numeric_temperature_hours"] == 4
          and checks["coverage"]["eligible_temperature_hours"] == 3)

    gap_rows = outputs["hourly_gaps"].select(
        "gap_kind", "gap_hours",
        F.date_format("gap_start_utc", "yyyy-MM-dd HH:mm").alias("start"),
        F.date_format("gap_end_utc", "yyyy-MM-dd HH:mm").alias("end"),
    ).collect()
    check("Gap ends are exclusive", any(
        row["gap_kind"] == "ozone_without_temperature"
        and (row["start"], row["end"], row["gap_hours"])
        == ("2024-02-01 00:00", "2024-02-01 01:00", 1)
        for row in gap_rows
    ))
    check("Gap crosses month boundaries intact", any(
        row["gap_kind"] == "no_eligible_ozone"
        and row["start"] == "2024-02-01 02:00"
        and row["end"] == "2024-12-31 23:00"
        for row in gap_rows
    ))

    mapping = outputs["yearly_mapping"]
    selected = outputs["temperature_hourly"]
    empty_enriched, empty_checks = enrich_hourly_ozone(
        ozone, selected.limit(0), mapping
    )
    check("Empty weather retains all ozone",
          empty_checks["missing_temperature"] == 4
          and empty_enriched.filter(F.col("temperature_c").isNotNull()).count() == 0)

    def select(table):
        """Apply the real hourly temperature selection policy to a fixture table."""
        return select_hourly_temperature(
            table, policy_version="temperature-hourly-v1"
        )

    extra_station = reports.filter(F.col("source_record_sha256") == "t1").withColumn(
        "station_id", F.lit("OTHER")
    )
    check("Stations partition independently",
          select(reports.unionByName(extra_station)).count() == 4)

    # Try invalid scope, keys and timestamps to confirm the hourly preparation rejects them.
    negative_cases = [
        ("Repeated report timestamps",
         lambda: select(reports.unionByName(reports.limit(1))),
         "Repeated report timestamps"),
        ("Missing report timestamp",
         lambda: select(reports.limit(1).withColumn(
             "timestamp_utc", F.lit(None).cast("timestamp"))),
         "usable station and timestamp keys"),
        ("Repeated hourly temperature key",
         lambda: enrich_hourly_ozone(
             ozone, selected.unionByName(selected.limit(1)), mapping),
         "missing or repeated keys"),
        ("Repeated ozone key",
         lambda: enrich_hourly_ozone(
             ozone.unionByName(ozone.limit(1)), selected, mapping),
         "missing or repeated keys"),
        ("Missing mapping",
         lambda: enrich_hourly_ozone(ozone, selected, mapping.limit(0)),
         "exactly one fixed mapping"),
        ("Overlapping mappings",
         lambda: enrich_hourly_ozone(
             ozone, selected, mapping.unionByName(mapping)),
         "exactly one fixed mapping"),
        ("Exclusive mapping end",
         lambda: enrich_hourly_ozone(ozone, selected, mapping.withColumn(
             "valid_until_utc",
             F.to_timestamp(F.lit("2024-12-31 23:00:00")))),
         "no usable mapping"),
        ("Missing site reference",
         lambda: build_yearly_mapping(
             {**tables, "epa_sites": tables["epa_sites"].limit(0)},
             fixture_profile, "f" * 64),
         "expected exactly one matching reference record"),
        ("Unpublished source bundle",
         lambda: prepare_hourly_silver(
             tables, {**manifest, "status": "unpublished"},
             environment="fixture"),
         "validated source Silver publication"),
        ("Report outside profile year",
         lambda: prepare_hourly_silver(
             {**tables, "temperature_reports": reports.unionByName(
                 reports.limit(1).withColumn(
                     "timestamp_utc",
                     F.to_timestamp(F.lit("2025-01-01 00:00:00"))))},
             manifest, environment="fixture"),
         "hourly input checks failed"),
    ]
    for label, action, message in negative_cases:
        rejects(label, action, message)

    return {"checks": len(passed), "calendar_hours": 8784, "months": 12}
