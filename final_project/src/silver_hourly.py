"""Prepare hourly Silver from the cleaned source tables.

Pair the configured ozone monitor and weather station, select one weather report
per hour, and join it to ozone. Build a full-year calendar to show both available
and missing data. Return six tables and their checks; the notebook publishes them."""

import calendar
from datetime import datetime
import json
import math

from pyspark.sql import Window, functions as F

from silver_files import silver_json_sha256


def inspect_hourly_inputs(tables, profile):
    """Check selected monitor/station IDs, timestamps and duplicate keys before joining."""
    year = profile["year"]
    monitor = profile["ozone_monitor"]
    monitor_id = "-".join(
        monitor[key]
        for key in (
            "state_code", "county_code", "site_number",
            "parameter_code", "poc",
        )
    )
    station_id = profile["weather_mapping"]["noaa_station_id"]

    # Include January 1 and exclude January 1 of the next year.
    start = F.to_timestamp(F.lit(f"{year}-01-01 00:00:00"))
    end = F.to_timestamp(F.lit(f"{year + 1}-01-01 00:00:00"))
    inputs = {}

    for name, id_column, expected_id in (
        ("ozone_observations", "monitor_id", monitor_id),
        ("temperature_reports", "station_id", station_id),
    ):
        table = tables[name]
        timestamp = F.col("timestamp_utc")
        hour = F.date_trunc("hour", timestamp)

        invalid_scope = (
            ~F.col(id_column).eqNullSafe(F.lit(expected_id))
            | ~F.coalesce(
                (timestamp >= start) & (timestamp < end),
                F.lit(False),
            )
        )

        # Count reports separately from clock hours: several weather reports can share an hour.
        result = table.agg(
            F.count("*").alias("records"),
            F.countDistinct(
                id_column, "timestamp_utc"
            ).alias("distinct_keys"),
            F.countDistinct(
                F.col(id_column), hour
            ).alias("clock_hours"),
            F.count(
                F.when(timestamp != hour, 1)
            ).alias("off_hour_records"),
            F.count(
                F.when(invalid_scope, 1)
            ).alias("invalid_scope_records"),
            F.date_format(
                F.min(timestamp), "yyyy-MM-dd HH:mm:ss"
            ).alias("earliest_utc"),
            F.date_format(
                F.max(timestamp), "yyyy-MM-dd HH:mm:ss"
            ).alias("latest_utc"),
        ).first().asDict()

        # Reject wrong identities, out-of-year records and repeated keys.
        # Ozone must be on the hour; weather reports may be at times such as 14:53.
        if (
            result["invalid_scope_records"]
            or result["records"] != result["distinct_keys"]
            or (
                name == "ozone_observations"
                and result["off_hour_records"]
            )
        ):
            raise ValueError(
                f"{name}: hourly input checks failed: {result}"
            )

        inputs[name] = {"dataset": name, **result}

    return inputs


def build_yearly_mapping(tables, profile, source_publication_id):
    """Check the selected entities and describe the fixed yearly mapping."""
    spark = tables["epa_sites"].sparkSession
    if spark.conf.get("spark.sql.session.timeZone") != "UTC":
        raise RuntimeError("Mapping intervals require UTC.")

    year = profile["year"]
    monitor = profile["ozone_monitor"]
    site_keys = ("state_code", "county_code", "site_number")
    monitor_keys = (*site_keys, "parameter_code", "poc")

    site_id = "-".join(monitor[key] for key in site_keys)
    monitor_id = "-".join(monitor[key] for key in monitor_keys)
    station_id = profile["weather_mapping"]["noaa_station_id"]

    if profile["weather_mapping"]["epa_site_id"] != site_id:
        raise ValueError(
            "The mapping and ozone monitor identify different EPA sites."
        )

    # Read just the selected reference rows. More than one match makes the pairing ambiguous.
    references = {}
    for name, id_column, expected_id in (
        ("epa_sites", "site_id", site_id),
        ("epa_monitors", "monitor_id", monitor_id),
        ("noaa_stations", "station_id", station_id),
    ):
        rows = tables[name].limit(2).collect()
        if len(rows) != 1 or rows[0][id_column] != expected_id:
            raise ValueError(
                f"{name}: expected exactly one matching reference record."
            )
        references[name] = rows[0].asDict(recursive=True)

    site, monitor_record, station = (
        references[name]
        for name in ("epa_sites", "epa_monitors", "noaa_stations")
    )

    if (
        any(site[key] != monitor[key] for key in site_keys)
        or any(
            monitor_record[key] != monitor[key]
            for key in monitor_keys
        )
    ):
        raise ValueError(
            "Reference identifier components disagree with the profile."
        )

    # Require usable coordinates before describing the pairing.
    for name, record in references.items():
        lat, lon = record["latitude_deg"], record["longitude_deg"]
        if (
            lat is None or lon is None
            or not math.isfinite(lat) or not math.isfinite(lon)
            or not -90 <= lat <= 90
            or not -180 <= lon <= 180
        ):
            raise ValueError(f"{name}: coordinates are unusable.")

    # Preserve reference warnings for review. Finding the requested monitor and
    # station does not by itself mean their pairing has been accepted.
    review_flags = [
        f"{name}:{flag}"
        for name, record in references.items()
        for flag in record["quality_flags"]
    ]

    if (
        site["latitude_deg"],
        site["longitude_deg"],
        site["datum"],
    ) != (
        monitor_record["latitude_deg"],
        monitor_record["longitude_deg"],
        monitor_record["datum"],
    ):
        review_flags.append(
            "epa_site_monitor_location_metadata_differ"
        )

    # Approximate distance between the published coordinate pairs.
    distance_km = None
    if (site["datum"] or "").strip().upper() == "WGS84":
        lat1, lat2 = map(
            math.radians,
            (site["latitude_deg"], station["latitude_deg"]),
        )
        dlat = lat2 - lat1
        dlon = math.radians(
            station["longitude_deg"] - site["longitude_deg"]
        )
        a = math.sin(dlat / 2) ** 2 + (
            math.cos(lat1)
            * math.cos(lat2)
            * math.sin(dlon / 2) ** 2
        )
        distance_km = (
            2 * 6371.0
            * math.asin(math.sqrt(min(1.0, max(0.0, a))))
        )
    else:
        review_flags.append("distance_requires_epa_datum_review")

    # Compare reference dates with the analysis year and flag metadata needing review.
    first_day = datetime(year, 1, 1).date()
    last_day = datetime(year, 12, 31).date()

    if (
        site["site_established_date"]
        and site["site_established_date"] > first_day
    ):
        review_flags.append("site_established_after_year_start")

    if (
        site["site_closed_date"]
        and site["site_closed_date"] <= last_day
    ):
        review_flags.append("site_closure_recorded_by_year_end")

    if (
        monitor_record["first_year_of_data"] is not None
        and monitor_record["first_year_of_data"] > year
    ):
        review_flags.append("monitor_first_year_after_analysis_year")

    if (
        monitor_record["last_sample_date"]
        and monitor_record["last_sample_date"] < last_day
    ):
        review_flags.append("monitor_last_sample_before_year_end")

    for name in ("epa_sites", "epa_monitors"):
        extraction = references[name]["extraction_date"]
        if extraction and extraction < last_day:
            review_flags.append(f"{name}:snapshot_before_year_end")

    review_flags = sorted(set(review_flags))

    # Tie the mapping ID to both the profile and source publication. A new source parent
    # can change this ID even when the physical station pairing stays the same.
    identity = {
        "mapping_version": "fixed-site-station-year-v1",
        "region": profile["region"],
        "year": year,
        "monitor_id": monitor_id,
        "site_id": site_id,
        "station_id": station_id,
        "valid_from_utc": f"{year}-01-01 00:00:00",
        "valid_until_utc": f"{year + 1}-01-01 00:00:00",
        "profile_sha256": silver_json_sha256(profile),
        "source_silver_publication_id": source_publication_id,
    }
    mapping_id = silver_json_sha256(identity)

    # Create one mapping row for the configured year, with an exclusive end time.
    mapping = spark.createDataFrame(
        [(
            mapping_id,
            identity["mapping_version"],
            profile["region"],
            year,
            monitor_id,
            site_id,
            station_id,
            identity["valid_from_utc"],
            identity["valid_until_utc"],
            distance_km,
            review_flags,
            source_publication_id,
        )],
        "mapping_id STRING, mapping_version STRING, "
        "region STRING, year INT, monitor_id STRING, "
        "site_id STRING, station_id STRING, "
        "valid_from_utc STRING, valid_until_utc STRING, "
        "distance_km DOUBLE, mapping_review_flags ARRAY<STRING>, "
        "source_silver_publication_id STRING",
    ).withColumn(
        "valid_from_utc", F.to_timestamp("valid_from_utc"),
    ).withColumn(
        "valid_until_utc", F.to_timestamp("valid_until_utc"),
    )

    # Return reference details and limitations with approval still pending.
    # The notebook records the separate reviewed acceptance decision.
    evidence = {
        **identity,
        "mapping_id": mapping_id,
        "structural_status": "passed",
        "approval_status": "pending",
        "distance_km": distance_km,
        "distance_method": "haversine; spherical Earth radius 6371 km",
        "review_flags": review_flags,
        "reference_records": json.loads(
            json.dumps(references, default=str, allow_nan=False)
        ),
        "limitations": [
            "The yearly interval is the configured pairing, not an operating schedule.",
            "Reference snapshots do not establish complete station or site history.",
            "Distance describes proximity, not equivalent local weather conditions.",
            "Distance uses published coordinates without a datum transformation or terrain model.",
        ],
    }
    return mapping, evidence


def select_hourly_temperature(reports, *, policy_version):
    """Select the latest eligible report per station and UTC clock hour."""
    if reports.sparkSession.conf.get("spark.sql.session.timeZone") != "UTC":
        raise RuntimeError("Hourly temperature selection requires UTC.")

    if policy_version != "temperature-hourly-v1":
        raise ValueError("Unsupported hourly temperature policy.")

    # Reject inputs that already contain our derived hourly columns.
    reserved = {
        "hour_start_utc",
        "report_timestamp_utc",
        "qualifying_report_count",
        "temperature_selection_rule",
        "__latest_report_timestamp",
    }
    if reserved & set(reports.columns):
        raise ValueError(
            "Input columns conflict with hourly output columns."
        )

    if reports.filter(
        F.col("station_id").isNull()
        | (F.trim("station_id") == "")
        | F.col("timestamp_utc").isNull()
    ).limit(1).count():
        raise ValueError(
            "Hourly selection requires usable station and timestamp keys."
        )

    # Repeated station/report timestamps would make the latest-report choice ambiguous.
    if reports.groupBy(
        "station_id", "timestamp_utc"
    ).count().filter(
        F.col("count") > 1
    ).limit(1).count():
        raise ValueError(
            "Repeated report timestamps could create ambiguous hourly selection."
        )

    # Compare reports only within the same station and UTC hour.
    hour_window = Window.partitionBy(
        "station_id", "hour_start_utc"
    )

    # Keep original timestamps, count each hour's reports and find the latest one.
    # For example, 14:53 represents 14:00-15:00, while 15:00 belongs to the next hour.
    candidates = (
        reports
        .withColumnRenamed(
            "timestamp_utc", "report_timestamp_utc"
        )
        .withColumn(
            "hour_start_utc",
            F.date_trunc("hour", "report_timestamp_utc"),
        )
        .withColumn(
            "qualifying_report_count",
            F.count("*").over(hour_window),
        )
        .withColumn(
            "__latest_report_timestamp",
            F.max("report_timestamp_utc").over(hour_window),
        )
    )

    retained_columns = [
        name for name in reports.columns
        if name not in {
            "station_id", "timestamp_utc", "temperature_c"
        }
    ]

    # Keep the latest report, its source details and the number of candidates.
    return (
        candidates
        .filter(
            F.col("report_timestamp_utc")
            == F.col("__latest_report_timestamp")
        )
        .withColumn(
            "temperature_selection_rule",
            F.lit(policy_version),
        )
        .select(
            "station_id",
            "hour_start_utc",
            "report_timestamp_utc",
            "temperature_c",
            "qualifying_report_count",
            "temperature_selection_rule",
            *retained_columns,
        )
    )


def usable_hourly_key(keys):
    """Return a Spark condition requiring every key field to be present and nonblank."""
    usable = F.lit(True)
    for name in keys:
        usable = usable & F.col(name).isNotNull() & (
            F.trim(F.col(name).cast("string")) != ""
        )
    return usable


def check_hourly_keys(table, keys, label):
    """Require non-empty, unique keys and return the row count."""
    usable = usable_hourly_key(keys)
    # Check that all key fields are usable and their combined values identify unique rows.
    counts = table.agg(
        F.count("*").alias("records"),
        F.countDistinct(*keys).alias("distinct_keys"),
        F.count(F.when(~usable, 1)).alias("unusable_keys"),
    ).first()

    if counts["unusable_keys"] or counts["records"] != counts["distinct_keys"]:
        raise ValueError(f"{label}: missing or repeated keys: {counts.asDict()}")
    return counts["records"]


def enrich_hourly_ozone(ozone, weather, mapping):
    """Attach the mapped hourly temperature without removing ozone rows."""
    if ozone.sparkSession.conf.get("spark.sql.session.timeZone") != "UTC":
        raise ValueError("Hourly enrichment requires UTC.")

    # Check keys before joining so repeated matches cannot multiply ozone rows.
    input_records = check_hourly_keys(
        ozone, ["monitor_id", "timestamp_utc"], "Ozone"
    )
    check_hourly_keys(
        weather, ["station_id", "hour_start_utc"], "Hourly temperature"
    )
    if mapping.count() != 1:
        raise ValueError("A region-year requires exactly one fixed mapping.")

    # Send the small mapping table to each Spark worker (broadcast), then use it
    # to attach the chosen weather station to the ozone readings.
    mapped = (
        ozone.withColumnRenamed("timestamp_utc", "hour_start_utc")
        .join(
            F.broadcast(mapping.withColumnRenamed(
                "station_id", "weather_station_id"
            )),
            on="monitor_id",
            how="left",
        )
    )
    # Require every ozone hour to fall inside the mapping's start-inclusive, end-exclusive year.
    valid_mapping = (
        F.col("mapping_id").isNotNull()
        & F.col("weather_station_id").isNotNull()
        & (F.trim("weather_station_id") != "")
        & (F.col("hour_start_utc") >= F.col("valid_from_utc"))
        & (F.col("hour_start_utc") < F.col("valid_until_utc"))
        & (F.col("hour_start_utc") == F.date_trunc("hour", "hour_start_utc"))
    )
    if mapped.filter(~F.coalesce(valid_mapping, F.lit(False))).limit(1).count():
        raise ValueError("An ozone hour has no usable mapping or is off-hour.")

    # Keep weather source details under separate column names, so we can trace
    # both readings back to their original files after the join.
    weather_names = {
        name: (
            "weather_station_id" if name == "station_id"
            else name if name == "hour_start_utc" or name.startswith("temperature_")
            else f"temperature_{name}"
        )
        for name in weather.columns
    }
    weather_columns = weather.select(
        *[F.col(name).alias(target) for name, target in weather_names.items()]
    )
    # Keep every ozone hour with a left join. Missing weather becomes null and gets a flag.
    enriched = (
        mapped.join(
            weather_columns,
            on=["weather_station_id", "hour_start_utc"],
            how="left",
        )
        .withColumn(
            "temperature_available",
            F.col("temperature_report_timestamp_utc").isNotNull(),
        )
        .withColumn(
            "integration_flags",
            F.when(
                ~F.col("temperature_available"),
                F.array(F.lit("no_eligible_temperature")),
            ).otherwise(F.array().cast("array<string>")),
        )
    )

    # Require the same number of unique ozone hours before and after the joins.
    checks = enriched.agg(
        F.count("*").alias("enriched_records"),
        F.countDistinct("monitor_id", "hour_start_utc").alias("distinct_hour_keys"),
        F.count(F.when(F.col("temperature_available"), 1)).alias("matched_temperature"),
        F.count(F.when(~F.col("temperature_available"), 1)).alias("missing_temperature"),
    ).first().asDict()
    checks["input_ozone_records"] = input_records

    if (
        checks["enriched_records"] != input_records
        or checks["distinct_hour_keys"] != input_records
    ):
        raise ValueError(f"Ozone enrichment changed the row grain: {checks}")

    return enriched, checks


def build_calendar_coverage(tables, enriched, weather, profile):
    """Build one coverage row per monitor and UTC calendar hour."""
    session = enriched.sparkSession
    if session.conf.get("spark.sql.session.timeZone") != "UTC":
        raise ValueError("Calendar coverage requires UTC.")

    year = int(profile["year"])
    monitor = profile["ozone_monitor"]
    monitor_id = "-".join(monitor[key] for key in (
        "state_code", "county_code", "site_number", "parameter_code", "poc"
    ))
    station_id = profile["weather_mapping"]["noaa_station_id"]
    start = F.to_timestamp(F.lit(f"{year}-01-01 00:00:00"))
    end = F.to_timestamp(F.lit(f"{year + 1}-01-01 00:00:00"))

    # Generate every UTC hour of the year, including hours with no source readings.
    calendar_hours = session.range(1).select(
        F.explode(F.sequence(
            start, end - F.expr("INTERVAL 1 HOUR"), F.expr("INTERVAL 1 HOUR")
        )).alias("hour_start_utc")
    ).select(
        F.lit(profile["region"]).alias("region"),
        F.lit(year).alias("year"),
        F.lit(monitor_id).alias("monitor_id"),
        F.lit(station_id).alias("station_id"),
        "hour_start_utc",
    )

    def audit_by_hour(audit, id_column, expected_id, prefix):
        """Return source-record counts per assignable hour and totals that could not be assigned."""
        # Assign only records for this identifier and year. Ozone must also be exactly on the hour.
        assignable = (
            (F.col(id_column) == expected_id)
            & (F.col("timestamp_utc") >= start)
            & (F.col("timestamp_utc") < end)
        )
        if prefix == "ozone":
            assignable = assignable & (
                F.col("timestamp_utc") == F.date_trunc("hour", "timestamp_utc")
            )
        assignable = F.coalesce(assignable, F.lit(False))

        # Count all source copies, including records whose identifier or time prevents assignment.
        assignment = audit.agg(
            F.coalesce(F.sum("record_count"), F.lit(0)).alias("source_records"),
            F.coalesce(F.sum(F.when(
                ~assignable, F.col("record_count")
            ).otherwise(0)), F.lit(0)).alias("unassigned_source_records"),
        ).first().asDict()

        aggregates = [
            F.sum("record_count").alias(f"{prefix}_source_records")
        ]
        # A numeric temperature can still fail quality rules. Count it separately from eligible weather.
        if prefix == "temperature":
            numeric = F.lit(True)
            for flag in (
                "missing_temperature", "invalid_temperature",
                "nonfinite_temperature",
            ):
                numeric = numeric & ~F.col("field_flags")[flag]
            aggregates.append(
                F.sum(F.when(
                    F.coalesce(numeric, F.lit(False)), F.col("record_count")
                ).otherwise(0)).alias("numeric_temperature_source_records")
            )

        grouped = (
            audit.filter(assignable)
            .withColumn("hour_start_utc", F.date_trunc("hour", "timestamp_utc"))
            .groupBy(id_column, "hour_start_utc")
            .agg(*aggregates)
        )
        return grouped, assignment

    ozone_hours, ozone_assignment = audit_by_hour(
        tables["ozone_audit"], "monitor_id", monitor_id, "ozone"
    )
    temperature_hours, temperature_assignment = audit_by_hour(
        tables["temperature_audit"], "station_id", station_id, "temperature"
    )

    # Join source counts and eligible results onto the full calendar.
    # Zero counts mean no matching records, not a measured ozone or temperature value.
    coverage = (
        calendar_hours
        .join(ozone_hours, ["monitor_id", "hour_start_utc"], "left")
        .join(temperature_hours, ["station_id", "hour_start_utc"], "left")
        .join(
            enriched.select(
                "monitor_id", "hour_start_utc",
                F.lit(True).alias("ozone_available"),
            ),
            ["monitor_id", "hour_start_utc"], "left",
        )
        .join(
            weather.select(
                "station_id", "hour_start_utc", "qualifying_report_count"
            ),
            ["station_id", "hour_start_utc"], "left",
        )
        .fillna({
            "ozone_source_records": 0,
            "temperature_source_records": 0,
            "numeric_temperature_source_records": 0,
            "qualifying_report_count": 0,
            "ozone_available": False,
        })
        .withColumn("temperature_available", F.col("qualifying_report_count") > 0)
        .withColumn(
            "numeric_temperature_available",
            F.col("numeric_temperature_source_records") > 0,
        )
        .withColumn(
            "shared_available",
            F.col("ozone_available") & F.col("temperature_available"),
        )
        .withColumn("expected_ozone_operating", F.lit(None).cast("boolean"))
        .withColumn("expected_temperature_operating", F.lit(None).cast("boolean"))
    )
    # Distinguish usable data, source records that were excluded and no assignable source record.
    for source in ("ozone", "temperature"):
        coverage = coverage.withColumn(
            f"{source}_status",
            F.when(F.col(f"{source}_available"), "eligible")
            .when(F.col(f"{source}_source_records") > 0, "excluded")
            .otherwise("not_observed"),
        )

    return coverage, {
        "ozone": ozone_assignment,
        "temperature": temperature_assignment,
    }


def summarise_hourly_coverage(coverage):
    """Summarise calendar coverage by month and identify continuous gaps."""
    if coverage.sparkSession.conf.get("spark.sql.session.timeZone") != "UTC":
        raise ValueError("Coverage summaries require UTC.")

    keys = ["region", "year", "monitor_id", "station_id"]
    # Build monthly counts from the complete calendar, retaining months with no observations.
    conditions = {
        "observed_ozone_hours": F.col("ozone_source_records") > 0,
        "eligible_ozone_hours": F.col("ozone_available"),
        "excluded_ozone_hours": F.col("ozone_status") == "excluded",
        "observed_temperature_hours": F.col("temperature_source_records") > 0,
        "numeric_temperature_hours": F.col("numeric_temperature_available"),
        "eligible_temperature_hours": F.col("temperature_available"),
        "excluded_temperature_hours": F.col("temperature_status") == "excluded",
        "shared_hours": F.col("shared_available"),
        "ozone_hours_without_temperature": (
            F.col("ozone_available") & ~F.col("temperature_available")
        ),
    }
    # An operating-hours total is known only when every hour has a documented operating status.
    operating_counts = [
        F.when(
            F.count(f"expected_{source}_operating") == F.count("*"),
            F.sum(F.col(f"expected_{source}_operating").cast("long")),
        ).alias(f"documented_{source}_operating_hours")
        for source in ("ozone", "temperature")
    ]
    monthly = (
        coverage.withColumn("month_start_utc", F.date_trunc("month", "hour_start_utc"))
        .groupBy(*keys, "month_start_utc")
        .agg(
            F.count("*").alias("calendar_hours"),
            *[F.count(F.when(condition, 1)).alias(name)
              for name, condition in conditions.items()],
            *operating_counts,
        )
    )
    # Calendar coverage uses all hours; weather-match coverage uses eligible ozone hours.
    # Leave a percentage null when its denominator is zero.
    percentages = {
        "ozone_calendar_pct": ("eligible_ozone_hours", "calendar_hours"),
        "temperature_calendar_pct": ("eligible_temperature_hours", "calendar_hours"),
        "temperature_match_pct": ("shared_hours", "eligible_ozone_hours"),
    }
    for name, (numerator, denominator) in percentages.items():
        monthly = monthly.withColumn(
            name,
            F.when(
                F.col(denominator) > 0,
                F.round(100.0 * F.col(numerator) / F.col(denominator), 2),
            ),
        )
    monthly = (
        monthly.withColumn(
            "temperature_match_reason",
            F.when(F.col("eligible_ozone_hours") == 0, "no_eligible_ozone_hours"),
        )
        .withColumn(
            "operating_calendar_reason",
            F.when(
                F.col("documented_ozone_operating_hours").isNull()
                | F.col("documented_temperature_operating_hours").isNull(),
                "unknown_or_incomplete_operating_calendar",
            ),
        )
    )

    # An hour can belong to several gap categories; summarise each category independently.
    gap_conditions = {
        "no_eligible_ozone": ~F.col("ozone_available"),
        "no_eligible_temperature": ~F.col("temperature_available"),
        "ozone_without_temperature": (
            F.col("ozone_available") & ~F.col("temperature_available")
        ),
    }
    # Create one row per applicable gap category and hour.
    missing_hours = coverage.select(
        *keys, "hour_start_utc",
        F.explode(F.filter(
            F.array(*[
                F.when(condition, F.lit(name))
                for name, condition in gap_conditions.items()
            ]),
            lambda name: name.isNotNull(),
        )).alias("gap_kind"),
    )
    gap_order = Window.partitionBy(*keys, "gap_kind").orderBy("hour_start_utc")

    # Subtract row position * 3600 from each timestamp in seconds. Consecutive
    # missing hours get the same value, so they form one gap. Its end is one
    # hour after the final missing hour; that end timestamp is not part of the gap.
    gaps = (
        missing_hours.withColumn(
            "__gap_group",
            F.unix_timestamp("hour_start_utc")
            - F.row_number().over(gap_order).cast("long") * 3600,
        )
        .groupBy(*keys, "gap_kind", "__gap_group")
        .agg(
            F.min("hour_start_utc").alias("gap_start_utc"),
            (F.max("hour_start_utc") + F.expr("INTERVAL 1 HOUR")).alias("gap_end_utc"),
            F.count("*").alias("gap_hours"),
        )
        .drop("__gap_group")
    )
    return monthly, gaps


def prepare_hourly_silver(tables, manifest, *, environment):
    """Return six prepared DataFrames, mapping evidence and consistency checks for one source release."""
    if (
        manifest["kind"] != "source_silver"
        or manifest["status"] != "published"
        or manifest["environment"] != environment
        or manifest["validation"]["status"] != "validated"
    ):
        raise ValueError("A validated source Silver publication is required.")
    if tables["ozone_observations"].sparkSession.conf.get("spark.sql.session.timeZone") != "UTC":
        raise ValueError("Hourly preparation requires UTC.")

    # Use a copy of the published profile and require the supported hourly-selection policy.
    profile = json.loads(json.dumps(manifest["profile"]))
    policy = profile["temperature_policy"]
    if (
        policy["version"] != "temperature-hourly-v1"
        or policy["hourly_selection"] != "latest_within_utc_hour"
    ):
        raise ValueError("Unsupported hourly temperature policy.")

    def require_counts(actual, expected, label):
        """Raise an error showing which measured counts differ from the expected totals."""
        differences = {
            name: {"actual": actual.get(name), "expected": value}
            for name, value in expected.items()
            if actual.get(name) != value
        }
        if differences:
            raise ValueError(f"{label}: {differences}")

    # Build the mapping, select weather, join ozone and make the calendar and summaries.
    inputs = inspect_hourly_inputs(tables, profile)
    mapping, mapping_evidence = build_yearly_mapping(
        tables, profile, manifest["publication_id"]
    )
    weather = select_hourly_temperature(
        tables["temperature_reports"], policy_version=policy["version"]
    )
    enriched, enrichment_checks = enrich_hourly_ozone(
        tables["ozone_observations"], weather, mapping
    )
    coverage, audit_assignment = build_calendar_coverage(
        tables, enriched, weather, profile
    )
    monthly, gaps = summarise_hourly_coverage(coverage)

    # Require one selected weather report per hour and account for every eligible report.
    weather_checks = weather.agg(
        F.count("*").alias("hourly_records"),
        F.countDistinct("station_id", "hour_start_utc").alias("distinct_hour_keys"),
        F.coalesce(F.sum("qualifying_report_count"), F.lit(0)).alias("eligible_reports"),
        F.count(F.when(
            (F.col("report_timestamp_utc") < F.col("hour_start_utc"))
            | (F.col("report_timestamp_utc") >= F.expr("hour_start_utc + INTERVAL 1 HOUR")),
            1,
        )).alias("reports_outside_selected_hour"),
    ).first().asDict()
    require_counts(weather_checks, {
        "hourly_records": inputs["temperature_reports"]["clock_hours"],
        "distinct_hour_keys": inputs["temperature_reports"]["clock_hours"],
        "eligible_reports": inputs["temperature_reports"]["records"],
        "reports_outside_selected_hour": 0,
    }, "Hourly temperature")

    conditions = {
        "observed_ozone_hours": F.col("ozone_source_records") > 0,
        "eligible_ozone_hours": F.col("ozone_available"),
        "excluded_ozone_hours": F.col("ozone_status") == "excluded",
        "observed_temperature_hours": F.col("temperature_source_records") > 0,
        "numeric_temperature_hours": F.col("numeric_temperature_available"),
        "eligible_temperature_hours": F.col("temperature_available"),
        "shared_hours": F.col("shared_available"),
    }
    # Reconcile calendar totals with observations, matched weather and source evidence.
    coverage_checks = coverage.agg(
        F.count("*").alias("calendar_hours"),
        F.countDistinct("monitor_id", "hour_start_utc").alias("distinct_hour_keys"),
        F.sum("qualifying_report_count").alias("eligible_temperature_reports"),
        F.sum("ozone_source_records").alias("assigned_ozone_source_records"),
        F.sum("temperature_source_records").alias("assigned_temperature_source_records"),
        F.count(F.when(
            (F.col("ozone_available") & (F.col("ozone_source_records") == 0))
            | (F.col("temperature_available") & ~F.col("numeric_temperature_available")),
            1,
        )).alias("eligible_hours_without_source_evidence"),
        *[F.count(F.when(condition, 1)).alias(name)
          for name, condition in conditions.items()],
    ).first().asDict()
    # Include leap day when checking the full-year calendar length.
    calendar_hours = (366 if calendar.isleap(profile["year"]) else 365) * 24
    require_counts(coverage_checks, {
        "calendar_hours": calendar_hours,
        "distinct_hour_keys": calendar_hours,
        "eligible_ozone_hours": inputs["ozone_observations"]["records"],
        "eligible_temperature_hours": weather_checks["hourly_records"],
        "eligible_temperature_reports": weather_checks["eligible_reports"],
        "shared_hours": enrichment_checks["matched_temperature"],
        "eligible_hours_without_source_evidence": 0,
    }, "Calendar coverage")

    # Assigned plus unassignable audit counts must equal the source release's input counts.
    for source, dataset in (
        ("ozone", "ozone_observations"),
        ("temperature", "temperature_reports"),
    ):
        expected = manifest["validation"]["audits"][dataset]["input_records"]
        assignment = audit_assignment[source]
        assigned = coverage_checks[f"assigned_{source}_source_records"]
        require_counts({
            "source_records": assignment["source_records"],
            "accounted_records": assigned + assignment["unassigned_source_records"],
        }, {
            "source_records": expected, "accounted_records": expected,
        }, f"{source} audit")

    # All twelve monthly summaries must add back to the full calendar totals.
    monthly_metrics = ["calendar_hours", *conditions]
    monthly_checks = monthly.agg(
        F.count("*").alias("months"),
        *[F.sum(name).alias(name) for name in monthly_metrics],
    ).first().asDict()
    require_counts(monthly_checks, {
        "months": 12,
        **{name: coverage_checks[name] for name in monthly_metrics},
    }, "Monthly coverage")

    # Check interval lengths and require total gap hours to match the missing-hour counts.
    gap_rows = gaps.groupBy("gap_kind").agg(
        F.count("*").alias("gap_count"),
        F.sum("gap_hours").alias("total_gap_hours"),
        F.max("gap_hours").alias("longest_gap_hours"),
        F.count(F.when(
            F.unix_timestamp("gap_end_utc") - F.unix_timestamp("gap_start_utc")
            != F.col("gap_hours") * 3600,
            1,
        )).alias("invalid_intervals"),
    ).collect()
    gap_checks = {row["gap_kind"]: row.asDict() for row in gap_rows}
    expected_gaps = {
        "no_eligible_ozone": calendar_hours - coverage_checks["eligible_ozone_hours"],
        "no_eligible_temperature": calendar_hours - coverage_checks["eligible_temperature_hours"],
        "ozone_without_temperature": (
            coverage_checks["eligible_ozone_hours"] - coverage_checks["shared_hours"]
        ),
    }
    for kind, expected in expected_gaps.items():
        actual = gap_checks.get(kind, {
            "gap_count": 0, "total_gap_hours": 0,
            "longest_gap_hours": 0, "invalid_intervals": 0,
        })
        require_counts(actual, {
            "total_gap_hours": expected, "invalid_intervals": 0,
        }, f"Gap intervals: {kind}")
        gap_checks[kind] = actual

    # Return prepared tables and evidence. Acceptance, staging and publication happen in the notebook.
    return {
        "status": "prepared_unpublished",
        "profile": profile,
        "source_silver_publication_id": manifest["publication_id"],
        "rules": {
            "version": "hourly-integration-v1",
            "timezone": "UTC",
            "hour_interval": "[hour_start_utc, hour_start_utc + 1 hour)",
            "ozone_timestamp_meaning": "sample_start",
            "temperature_policy_version": policy["version"],
            "temperature_selection": policy["hourly_selection"],
            "missing_temperature": "retain_ozone_with_null_temperature",
        },
        "mapping_evidence": mapping_evidence,
        "tables": {
            "yearly_mapping": mapping,
            "temperature_hourly": weather,
            "ozone_enriched": enriched,
            "hourly_coverage": coverage,
            "monthly_coverage": monthly,
            "hourly_gaps": gaps,
        },
        "checks": {
            "scope": "in_memory_reconciliation",
            "status": "passed",
            "inputs": inputs,
            "temperature": weather_checks,
            "enrichment": enrichment_checks,
            "coverage": coverage_checks,
            "audit_assignment": audit_assignment,
            "monthly": monthly_checks,
            "gaps": gap_checks,
        },
    }
