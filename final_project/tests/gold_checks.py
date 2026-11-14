"""Run isolated Gold fixtures on the VM with spark-env Python."""

import json
import sys
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT / "src"))

from spark_runtime import configure_vm_python

if __name__ == "__main__":
    configure_vm_python()

from pyspark.sql import SparkSession, functions as F

from gold import build_monthly_coverage, build_period_catalogue, elevated_runs, load_gold_rules, summarise_year
from gold_io import load_gold, publish_gold, stage_gold_table
from run_context import RunContext
from silver_files import file_sha256


def check_detector(spark):
    """Check threshold equality, missing hours and separate monitors using small made-up readings."""
    schema = "monitor_id string, hour string, ozone_ppm double, temperature_c double"
    rows = [
        ("A", "2025-04-30 22:00", 0.040, 11.0),
        ("A", "2025-04-30 23:00", 0.060, 12.0),
        ("A", "2025-05-01 00:00", 0.070, None),
        ("A", "2025-05-01 01:00", 0.050, 13.0),
        ("A", "2025-05-01 03:00", 0.080, 14.0),
        ("A", "2025-05-01 04:00", 0.080, 15.0),
        ("B", "2025-05-01 02:00", 0.080, 10.0),
    ]
    ozone = (
        spark.createDataFrame(rows, schema)
        .withColumn("hour_start_utc", F.to_timestamp("hour", "yyyy-MM-dd HH:mm"))
        .withColumn("region", F.lit("fixture"))
        .withColumn("year", F.lit(2025))
    )
    # This threshold is only for the fixture; it does not change the saved Gold rule.
    rules = {"threshold_ppm": 0.050, "comparison": ">=", "minimum_hours": 3}

    elevated = elevated_runs(ozone, rules)
    members = elevated.where("run_hours >= 3")
    assert elevated.count() == 6  # Five from A, one from B.

    actual = (
        members.select(
            "monitor_id", "hour",
            F.date_format("period_start_utc", "yyyy-MM-dd HH:mm").alias("start"),
        )
        .orderBy("hour")
        .collect()
    )
    start = "2025-04-30 23:00"
    assert [tuple(row) for row in actual] == [
        ("A", "2025-04-30 23:00", start),
        ("A", "2025-05-01 00:00", start),
        ("A", "2025-05-01 01:00", start),
    ]

    # With strict comparison, the 01:00 reading at exactly 0.050 drops out.
    strict = elevated_runs(ozone, {**rules, "comparison": ">"})
    assert strict.where("run_hours >= 3").count() == 0

    # The missing 02:00 hour splits the run, even though both sides have high readings.
    extra = elevated.where("run_hours = 2").select("monitor_id", "hour").collect()
    assert {tuple(row) for row in extra} == {
        ("A", "2025-05-01 03:00"), ("A", "2025-05-01 04:00")
    }

    print("PASS: Gold detector fixture")

def check_catalogue(spark):
    """Check period summaries, missing weather and the reasons recorded at each boundary."""
    from math import isclose

    # Each row describes one calendar hour. A null start means it is
    # outside the period. Null ozone means there is no eligible reading.
    schema = (
        "monitor_id string, hour string, start string, ozone_status string, "
        "ozone_ppm double, temperature_c double"
    )
    april = "2025-04-30 23:00"
    jan = "2025-01-01 00:00"
    dec = "2025-12-31 21:00"

    rows = [
        ("A", "2025-04-30 22:00", None, "eligible", 0.040, 11.0),
        ("A", april, april, "eligible", 0.060, 12.0),
        ("A", "2025-05-01 00:00", april, "eligible", 0.070, None),
        ("A", "2025-05-01 01:00", april, "eligible", 0.050, 13.0),
        ("A", "2025-05-01 02:00", None, "not_observed", None, None),
        ("B", jan, jan, "eligible", 0.060, None),
        ("B", "2025-01-01 01:00", jan, "eligible", 0.060, None),
        ("B", "2025-01-01 02:00", jan, "eligible", 0.060, None),
        ("B", "2025-01-01 03:00", None, "excluded", None, None),
        ("C", "2025-12-31 20:00", None, "excluded", None, None),
        ("C", dec, dec, "eligible", 0.060, 10.0),
        ("C", "2025-12-31 22:00", dec, "eligible", 0.060, 10.0),
        ("C", "2025-12-31 23:00", dec, "eligible", 0.060, 10.0),
    ]
    hours = (
        spark.createDataFrame(rows, schema)
        .withColumn("hour_start_utc", F.to_timestamp("hour", "yyyy-MM-dd HH:mm"))
        .withColumn("period_start_utc", F.to_timestamp("start", "yyyy-MM-dd HH:mm"))
        .withColumn("region", F.lit("fixture"))
        .withColumn("year", F.lit(2025))
        .withColumn("weather_station_id", F.lit("station"))
        .withColumn("mapping_id", F.col("monitor_id"))
        .withColumn("mapping_version", F.lit("v1"))
    )
    calendar = hours.select(
        "region", "year", "monitor_id",
        F.col("weather_station_id").alias("station_id"),
        "hour_start_utc", "ozone_status",
    )
    ozone = hours.where("ozone_status = 'eligible'")
    members = ozone.where("period_start_utc IS NOT NULL")
    rules = {"comparison": ">=", "threshold_ppm": 0.050, "minimum_hours": 3}

    catalogue = build_period_catalogue(members, ozone, calendar, rules)
    result = catalogue.orderBy("monitor_id").collect()
    assert len(result) == 3
    a, b, c = result

    # The end is exclusive: the final member is 01:00, so the period ends at 02:00.
    assert a.duration_hours == 3
    assert a.period_end_utc - a.period_start_utc == timedelta(hours=3)
    assert isclose(a.peak_ozone_ppm, 0.070) and isclose(a.mean_ozone_ppm, 0.060)
    assert a.temperature_hours == 2 and isclose(a.temperature_coverage_pct, 200 / 3)
    assert (a.min_temperature_c, a.mean_temperature_c, a.max_temperature_c) == (12, 12.5, 13)
    assert (a.start_boundary, a.end_boundary) == ("observed_non_elevated", "not_observed")

    assert (b.start_boundary, b.end_boundary) == ("year_boundary", "excluded")
    assert b.temperature_hours == 0 and b.temperature_coverage_pct == 0
    assert (b.min_temperature_c, b.mean_temperature_c, b.max_temperature_c) == (None, None, None)
    assert b.temperature_summary_reason == "no_eligible_temperature"
    assert (c.start_boundary, c.end_boundary) == ("excluded", "year_boundary")

    # A year with no qualifying periods must still return the same table structure.
    empty = build_period_catalogue(members.limit(0), ozone, calendar, rules)
    assert empty.schema == catalogue.schema and empty.count() == 0
    print("PASS: Gold catalogue fixture")

def check_monthly_coverage(spark):
    """Check month allocation and distinguish missing data from a year with no qualifying periods."""
    from calendar import monthrange
    from datetime import date

    # The three period hours span April and May. May also has one isolated high hour.
    keys = ["region", "year", "monitor_id"]
    scope = ("fixture", 2025, "A")
    month_rows = []
    for month in range(1, 13):
        eligible = {4: 2, 5: 4}.get(month, 0)
        shared = {4: 2, 5: 3}.get(month, 0)
        month_rows.append((
            *scope, "station", date(2025, month, 1),
            24 * monthrange(2025, month)[1], eligible, shared, shared, eligible - shared,
        ))

    silver_months = spark.createDataFrame(
        month_rows,
        "region string, year int, monitor_id string, station_id string, "
        "month_start_utc date, calendar_hours int, eligible_ozone_hours int, "
        "eligible_temperature_hours int, shared_hours int, ozone_hours_without_temperature int",
    ).withColumn("month_start_utc", F.col("month_start_utc").cast("timestamp"))

    readings = [
        ("2025-04-30 23:00", True),
        ("2025-05-01 00:00", True),
        ("2025-05-01 01:00", True),
        ("2025-05-01 03:00", False),
    ]
    elevated = (
        spark.createDataFrame(readings, "hour string, member boolean")
        .withColumn("hour_start_utc", F.to_timestamp("hour", "yyyy-MM-dd HH:mm"))
        .withColumn("region", F.lit("fixture"))
        .withColumn("year", F.lit(2025))
        .withColumn("monitor_id", F.lit("A"))
        .withColumn("weather_station_id", F.lit("station"))
    )
    members = elevated.where("member")
    catalogue = members.groupBy(*keys, "weather_station_id").agg(
        F.min("hour_start_utc").alias("period_start_utc")
    )

    months = build_monthly_coverage(silver_months, elevated, members, catalogue)
    rows = months.orderBy("month_start_utc").collect()
    assert len(rows) == 12
    for month, row in enumerate(rows, start=1):
        assert row.month_start_utc.month == month
        expected = {4: (1, 1, 1), 5: (3, 2, 0)}.get(month, (0, 0, 0))
        assert (row.elevated_hours, row.qualifying_period_hours, row.periods_started) == expected

    year = summarise_year(months).first()
    assert (year.calendar_hours, year.elevated_hours, year.qualifying_period_hours) == (8760, 4, 3)
    assert year.periods_started == 1 and year.outcome == "qualifying_periods"
    assert year.temperature_match_pct == 83.33  # Five shared hours out of six eligible.

    # Small annual examples distinguish the three ways to have zero periods.
    cases = spark.createDataFrame(
        [("none", 0, 0), ("low", 4, 0), ("short", 4, 2)],
        "monitor_id string, eligible_ozone_hours int, elevated_hours int",
    )
    for name, value in {
        "region": "fixture", "year": 2025, "station_id": "station", "calendar_hours": 24,
        "eligible_temperature_hours": 0, "shared_hours": 0,
        "qualifying_period_hours": 0, "periods_started": 0,
    }.items():
        cases = cases.withColumn(name, F.lit(value))
    cases = cases.withColumn("ozone_hours_without_temperature", F.col("eligible_ozone_hours"))
    outcomes = {row.monitor_id: row for row in summarise_year(cases).collect()}
    assert outcomes["none"].outcome == "no_eligible_ozone"
    assert outcomes["none"].temperature_match_pct is None
    assert outcomes["low"].outcome == "no_elevated_hours"
    assert outcomes["short"].outcome == "all_runs_too_short"

    print("PASS: Gold monthly coverage fixture")


def check_release(spark, context):
    """Verify the real stage/load path and an interrupted second table write."""
    parent = {"kind": "hourly_silver", "status": "published", "environment": "test",
              "publication_id": "fixture", "evidence": {"profile": {"region": "Fixture", "year": 2025}}}
    relative = "published/silver/hourly/fixture/2025/fixture/manifest.json"
    path = context.environment_root / relative
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(parent), encoding="utf-8")
    digest = file_sha256(path)
    options = dict(parent=parent, parent_path=relative, parent_sha256=digest, context=context,
                   rules=load_gold_rules(PROJECT / "config/gold_rules.json"))
    catalogue = spark.createDataFrame([(3, None)], "duration_hours long, note string")
    # Publish both a populated and an empty catalogue, then compare every reloaded value.
    for count in (1, 0):
        periods = catalogue.limit(count)
        monthly = spark.createDataFrame([(3 * count, 3 * count, count)],
            "elevated_hours long, qualifying_period_hours long, periods_started long")
        release = publish_gold(catalogue=periods, monthly=monthly, **options)
        load_args = dict(expected_parent_sha256=digest, context=context, spark=spark)
        _, tables = load_gold(release["manifest_path"], expected_sha256=release["manifest_sha256"], **load_args)
        for name, expected in [("period_catalogue", periods), ("monthly_coverage", monthly)]:
            actual = tables[name]
            assert actual.dtypes == expected.dtypes
            assert not expected.exceptAll(actual).take(1) and not actual.exceptAll(expected).take(1)

    try:
        load_gold(release["manifest_path"], expected_sha256="wrong", **load_args)
    except ValueError as error:
        assert "checksum" in str(error)
    else:
        raise AssertionError("Gold accepted a wrong manifest digest.")

    def fail_second(table, bundle, name):
        """Simulate a failed monthly write after the catalogue has already been staged."""
        if name == "monthly_coverage":
            assert (bundle / "period_catalogue").is_dir()
            raise OSError("fixture: interrupted second write")
        return stage_gold_table(table, bundle, name)

    # Interrupt the second table write. There must be no new publication manifest,
    # and the last successful release must still load.
    before = set(context.published_root.rglob("manifest.json"))
    with patch("gold_io.stage_gold_table", side_effect=fail_second):
        try:
            publish_gold(catalogue=periods, monthly=monthly, **options)
        except OSError as error:
            assert str(error) == "fixture: interrupted second write"
        else:
            raise AssertionError("Expected an interrupted publication.")
    assert set(context.published_root.rglob("manifest.json")) == before
    load_gold(release["manifest_path"], expected_sha256=release["manifest_sha256"], **load_args)
    print("PASS: Gold publication and reload")


if __name__ == "__main__":
    # Keep fixture files separate from dev and prod; remove them when the test finishes.
    with TemporaryDirectory(prefix="dat535-gold-test-") as scratch:
        context = RunContext("test", Path(scratch), "gold-fixture")
        spark = (SparkSession.builder.master("local[2]").appName("DAT535 Gold checks")
                 .config("spark.sql.session.timeZone", "UTC")
                 .config("spark.sql.shuffle.partitions", "2")
                 .config("spark.sql.warehouse.dir", str(Path(scratch) / "warehouse"))
                 .getOrCreate())
        try:
            check_detector(spark)
            check_catalogue(spark)
            check_monthly_coverage(spark)
            check_release(spark, context)
        finally:
            spark.stop()
