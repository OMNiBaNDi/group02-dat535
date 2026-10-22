"""Execute the CP1 fixture and record successful verification."""

import json
from pathlib import Path
from time import perf_counter

from pyspark.sql import SparkSession

from fixture_processing import build_fixture_totals, verify_fixture_totals
from run_context import RunContext
from stages import StageResult


def run_fixture(
    spark: SparkSession,
    context: RunContext,
    fixture_dir: Path,
) -> StageResult:
    started = perf_counter()

    expected = json.loads(
        (fixture_dir / "expected.json").read_text(encoding="utf-8")
    )
    input_path = fixture_dir / "group_values.csv"

    staging_dir = context.staging_root / "fixture"
    output_path = staging_dir / "group_totals"
    report_path = context.reports_dir / "fixture.json"

    # Check nested output paths as well as the configured environment root.
    for path in (staging_dir, output_path, report_path):
        if (
            not path.is_relative_to(context.environment_root)
            or path.resolve() != path
        ):
            raise ValueError(
                f"Fixture output path escapes or redirects its location: {path}"
            )

    # Refuse to reuse this stage's working directory for another attempt.
    staging_dir.mkdir(parents=True, exist_ok=False)
    context.reports_dir.mkdir(parents=True, exist_ok=True)

    totals = build_fixture_totals(
        spark,
        input_path,
        expected["input_row_count"],
    )
    verify_fixture_totals(totals, expected["groups"])

    totals.write.mode("errorifexists").parquet(str(output_path))

    stored_totals = spark.read.parquet(str(output_path))
    verify_fixture_totals(stored_totals, expected["groups"])

    report = {
        "stage": "fixture",
        "fixture_id": expected["fixture_id"],
        "status": "success",
        "environment": context.environment,
        "environment_root": str(context.environment_root),
        "run_id": context.run_id,
        "spark_application_id": spark.sparkContext.applicationId,
        "spark_master": spark.sparkContext.master,
        "spark_version": spark.version,
        "verified_input_row_count": expected["input_row_count"],
        "verified_groups": expected["groups"],
        "parquet_round_trip_verified": True,
        "output_path": str(output_path),
        "published": False,
        "elapsed_seconds": round(perf_counter() - started, 3),
    }

    # Exclusive creation prevents replacing an existing report.
    with report_path.open("x", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2)
        handle.write("\n")

    return StageResult(
        stage="fixture",
        output_paths=(output_path,),
        report_path=report_path,
    )
