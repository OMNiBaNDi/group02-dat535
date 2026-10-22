"""Command-line entry point for the CP1 fixture on the Spark VM."""

import argparse
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import platform
import sys
from time import perf_counter

from run_context import create_run_context


def main() -> int:
    project = Path(__file__).resolve().parents[1]

    parser = argparse.ArgumentParser(
        description="Run the synthetic CP1 fixture on the Spark VM."
    )
    parser.add_argument(
        "--environment", required=True, choices=("dev", "test", "prod")
    )
    parser.add_argument(
        "--test-run-id", help="Required only for the test environment."
    )
    parser.add_argument(
        "--environments-file",
        type=Path,
        default=project / "config" / "environments.json",
        help="JSON file containing environment storage roots.",
    )
    parser.add_argument(
        "--fixture-dir",
        type=Path,
        default=project / "tests" / "fixtures" / "cp1",
        help="Directory containing group_values.csv and expected.json.",
    )
    args = parser.parse_args()

    try:
        context = create_run_context(
            args.environments_file,
            args.environment,
            test_run_id=args.test_run_id,
        )
    except (ValueError, OSError) as error:
        parser.error(str(error))

    # These directories must be new and must not redirect elsewhere.
    for path in (context.run_root, context.staging_root):
        if (
            not path.is_relative_to(context.environment_root)
            or path.resolve() != path
        ):
            raise ValueError(f"Run directory escapes or redirects: {path}")

    context.run_root.mkdir(parents=True, exist_ok=False)
    context.staging_root.mkdir(parents=True, exist_ok=False)
    context.logs_dir.mkdir()
    context.reports_dir.mkdir()

    run_report_path = context.reports_dir / "run.json"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(
                context.logs_dir / "run.log", mode="w", encoding="utf-8"
            ),
            logging.StreamHandler(),
        ],
    )
    logging.info("Run ID: %s", context.run_id)
    logging.info("Run report: %s", run_report_path)

    started = perf_counter()
    report = {
        "stage": "fixture",
        "environment": context.environment,
        "environment_root": str(context.environment_root),
        "run_id": context.run_id,
        "status": "failure",
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "python_version": platform.python_version(),
        "python_executable": sys.executable,
    }
    spark = None

    try:
        # Give later Actions steps the paths for this specific run.
        github_output = os.environ.get("GITHUB_OUTPUT")
        if github_output:
            outputs = {
                "run_id": context.run_id,
                "run_root": str(context.run_root),
            }

            # Each output must occupy exactly one name=value line.
            if any(
                "\n" in value or "\r" in value
                for value in outputs.values()
            ):
                raise ValueError("GitHub output values must be single-line.")

            with Path(github_output).open("a", encoding="utf-8") as handle:
                for name, value in outputs.items():
                    handle.write(f"{name}={value}\n")

        # Delayed imports let --help work without PySpark installed locally.
        import pyspark
        from pyspark.sql import SparkSession
        from fixture_stage import run_fixture

        spark = (
            SparkSession.builder
            .appName(f"DAT535-CP1-fixture-{context.environment}")
            .config(
                "spark.sql.warehouse.dir",
                str(context.staging_root / "spark_warehouse"),
            )
            .getOrCreate()
        )

        report.update({
            "spark_version": spark.version,
            "pyspark_version": pyspark.__version__,
            "spark_master": spark.sparkContext.master,
            "spark_application_id": spark.sparkContext.applicationId,
        })

        if not spark.sparkContext.master.startswith("spark://"):
            raise ValueError("CP1 requires the configured standalone Spark master.")
        if spark.version != pyspark.__version__:
            raise ValueError("Spark engine and imported PySpark versions differ.")

        result = run_fixture(spark, context, args.fixture_dir)

        report["fixture_report"] = str(result.report_path)
        report["output_paths"] = [str(path) for path in result.output_paths]
        report["status"] = "success"

    except Exception as error:
        report["error"] = f"{type(error).__name__}: {error}"
        logging.exception("Fixture run failed")

    finally:
        if spark is not None:
            try:
                spark.stop()
            except Exception as error:
                report["status"] = "failure"
                report["spark_stop_error"] = f"{type(error).__name__}: {error}"
                logging.exception("Spark shutdown failed")

        report["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
        report["elapsed_seconds"] = round(perf_counter() - started, 3)

        with run_report_path.open("x", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2)
            handle.write("\n")

    logging.info("Run finished with status: %s", report["status"])
    return 0 if report["status"] == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
