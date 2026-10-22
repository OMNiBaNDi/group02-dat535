"""Spark processing and result checks for the synthetic CP1 fixture."""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def build_fixture_totals(
    spark: SparkSession,
    input_path: Path,
    expected_input_count: int,
) -> DataFrame:
    records = (
        spark.read
        .schema("record_id INT, group_id STRING, value INT")
        .option("header", True)
        .option("enforceSchema", False)
        .option("mode", "FAILFAST")
        .csv(str(input_path))
    )

    missing_value = (
        F.col("record_id").isNull()
        | F.col("group_id").isNull()
        | F.col("value").isNull()
    )
    if records.filter(missing_value).limit(1).count() > 0:
        raise ValueError("CP1 fixture contains a missing required value.")

    actual_input_count = records.count()
    if actual_input_count != expected_input_count:
        raise ValueError(
            f"Fixture input count mismatch: expected {expected_input_count}, "
            f"got {actual_input_count}."
        )

    return (
        records
        .repartition(2, "group_id")
        .groupBy("group_id")
        .agg(
            F.count("*").alias("row_count"),
            F.sum("value").alias("value_sum"),
        )
    )


def verify_fixture_totals(
    totals: DataFrame,
    expected_groups: list[dict],
) -> None:
    expected = sorted(expected_groups, key=lambda group: group["group_id"])

    # One extra row is enough to detect unexpected additional groups.
    actual = [
        row.asDict()
        for row in (
            totals
            .orderBy("group_id")
            .limit(len(expected) + 1)
            .collect()
        )
    ]

    if actual != expected:
        raise ValueError(
            f"Fixture totals mismatch: expected {expected}, got {actual}."
        )
