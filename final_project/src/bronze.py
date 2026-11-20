"""Keep the original downloads and prepare their Bronze Parquet tables."""

import csv
import json
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import urlsplit
from urllib.request import Request, urlopen
from zipfile import ZipFile

from pipeline_files import child_path, file_sha256, verify_file_inventory, write_json
from source_columns import check_columns


# Changes to parsing or lineage policy require a new reader version.
BRONZE_READER_VERSION = "spark-csv-download-v2"


def prepare_bronze(spark, sources, context, provenance):
    """Prepare all sources and return their details and raw Spark tables.

    Both returned dictionaries use source keys such as "epa/hourly_ozone_2025".
    """
    started = time.perf_counter()
    report = {
        "scope": "bronze",
        "run_id": context.run_id,
        "environment": context.environment,
        "status": "failure",
        "provenance": provenance,
        "results": [],
    }
    # Use the same source names for the file details and Spark tables.
    selected_sources, raw_tables = {}, {}
    root = context.environment_root
    root.mkdir(parents=True, exist_ok=True)
    # An existing lock stops this run from writing shared Bronze files.
    lock = root / ".bronze-publication.lock"
    locked = False
    source_key = None

    try:
        lock.mkdir()
        locked = True
        for source_config in sources:
            source_key = f"{source_config['source']}/{source_name(source_config)}"
            source, records = _prepare_source(spark, source_config, context, provenance)
            selected_sources[source_key] = source
            raw_tables[source_key] = records
            report["results"].append({
                "source": source_key,
                "outcome": source["outcome"],
                "records": source["records"],
                "manifest_path": source["manifest_path"].relative_to(root).as_posix(),
            })
            print(f"{source_key}: {source['outcome']} ({source['records']} rows)")
            source_key = None
        report["status"] = "success"
    except Exception as error:
        report["failed_source"] = source_key
        report["error"] = str(error)
        raise
    finally:
        if locked:
            lock.rmdir()
        report["elapsed_seconds"] = round(time.perf_counter() - started, 3)
        report_path = context.reports_dir / "run.json"
        # Try to save the run report even if preparing a source failed.
        try:
            write_json(report_path, report)
        except Exception as report_error:
            # Keep the processing error if saving its report also fails.
            if report["status"] == "success":
                raise
            print(f"Could not save Bronze report: {report_error}")
        else:
            print("Bronze report:", report_path)

    return selected_sources, raw_tables


def _prepare_source(spark, source_config, context, provenance):
    """Reuse completed Parquet or build it from the accepted download.

    Return the source details and a Spark DataFrame read from the Parquet files.
    """
    root = context.environment_root
    key = f"{source_config['source']}/{source_name(source_config)}"
    checksum = source_config["expected_sha256"]
    # Keep different downloads and reader versions in separate Bronze directories.
    version = f"bronze_parquet/{key}/{checksum}/{BRONZE_READER_VERSION}"
    parquet_path = child_path(root, f"data/{version}")
    manifest_path = child_path(root, f"published/{version}/manifest.json")

    # A completed manifest lets us reuse Parquet without reopening the original download.
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if (
            manifest["manifest_version"] != 2
            or manifest["source"] != key
            or manifest["download_sha256"] != checksum
            or manifest["reader_version"] != BRONZE_READER_VERSION
        ):
            raise ValueError("Bronze Parquet differs from the selected source or reader.")
        files = verify_file_inventory(parquet_path, manifest["datasets"])["raw"]
        outcome = "reused"
    else:
        # Parquet without a manifest may be left over from an interrupted build.
        if parquet_path.exists():
            raise FileExistsError(f"Incomplete Bronze remains at {parquet_path}; review it before retrying.")
        name = Path(urlsplit(source_config["url"]).path).name
        download_path = child_path(root, f"data/bronze/{key}/{checksum}/{name}")
        context.staging_root.mkdir(parents=True, exist_ok=True)

        # Build in this run's staging directory before publishing the finished table.
        with TemporaryDirectory(prefix="bronze-", dir=context.staging_root) as temporary:
            scratch = Path(temporary)
            acquisition = _acquire_download(source_config, download_path, scratch)
            table_info = _write_bronze_parquet(spark, source_config, download_path, scratch)
            manifest = {
                "manifest_version": 2,
                "source": key,
                "reader_version": BRONZE_READER_VERSION,
                "download_path": download_path.relative_to(root).as_posix(),
                "download_sha256": checksum,
                "parquet_path": parquet_path.relative_to(root).as_posix(),
                "acquisition": acquisition,
                "datasets": {"raw": table_info},
                "provenance": provenance,
            }
            # Move the finished table first. The manifest marks it as ready for reuse.
            parquet_path.parent.mkdir(parents=True, exist_ok=True)
            (scratch / "table").rename(parquet_path)
            write_json(manifest_path, manifest)

        files = [str(parquet_path / item["path"]) for item in table_info["files"]]
        outcome = "created"

    source = {
        "provider": source_config["source"],
        "dataset": source_config["dataset"],
        "download_path": child_path(root, manifest["download_path"]),
        "download_sha256": checksum,
        "manifest_path": manifest_path,
        "records": manifest["datasets"]["raw"]["records"],
        "outcome": outcome,
    }
    return source, spark.read.parquet(*files)


def _acquire_download(source_config, destination, scratch):
    """Reuse or download the original file and check its checksum."""
    if destination.exists():
        downloaded = destination
        acquisition = {"outcome": "reused"}
    else:
        downloaded = scratch / destination.name
        acquisition = download_file(source_config["url"], downloaded)
        acquisition["outcome"] = "downloaded"

    # Check cached and newly downloaded originals against the same accepted checksum.
    if file_sha256(downloaded) != source_config["expected_sha256"]:
        raise ValueError("Downloaded source does not match the accepted checksum.")
    if downloaded != destination:
        destination.parent.mkdir(parents=True, exist_ok=True)
        downloaded.rename(destination)
    return acquisition


def _write_bronze_parquet(spark, source_config, downloaded, scratch):
    """Write Parquet in staging and return the row count and file details."""
    data_path = downloaded
    archive_member = None
    # Extract the CSV in staging. Keep the original ZIP as the source file.
    if downloaded.suffix == ".zip":
        archive_member = downloaded.with_suffix(".csv").name
        data_path = scratch / archive_member
        with ZipFile(downloaded) as archive:
            with archive.open(archive_member) as reader, data_path.open("xb") as writer:
                shutil.copyfileobj(reader, writer)

    delimiter = "|" if data_path.suffix == ".psv" else ","
    raw = read_source(spark, data_path, delimiter)
    if check_columns(source_config, raw.columns)["status"] != "compatible":
        raise ValueError("The source header is missing required columns.")
    staged = scratch / "table"
    (
        raw.write.mode("errorifexists")
        .option("compression", "snappy")
        .parquet(str(staged))
    )

    # Count the saved rows and record the files needed to verify this table later.
    saved = spark.read.parquet(str(staged))
    row_count = saved.count()
    files = [
        {
            "path": path.relative_to(staged).as_posix(),
            "bytes": path.stat().st_size,
            "sha256": file_sha256(path),
        }
        for path in sorted(staged.glob("*.parquet"))
    ]
    return {"records": row_count, "files": files, "archive_member": archive_member}


def download_file(url, destination):
    """Download into staging and return the URLs and download time."""
    # Keep unfinished transfers under a temporary name.
    partial = destination.with_name(destination.name + ".part")
    request = Request(url, headers={"Accept-Encoding": "identity"})
    with urlopen(request, timeout=60) as response:
        resolved_url = response.geturl()
        with partial.open("wb") as handle:
            shutil.copyfileobj(response, handle, length=1024 * 1024)
    partial.rename(destination)
    return {
        "requested_url": url,
        "resolved_url": resolved_url,
        "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def read_source(spark, path, delimiter):
    """Read the source with Spark, keeping all columns as strings."""
    from pyspark.sql import types as T

    columns = read_header(path)
    # Keep source values as text. Silver handles cleaning and type conversion.
    schema = T.StructType([
        T.StructField(name, T.StringType(), True)
        for name in columns
    ])

    return (
        spark.read
        .schema(schema)
        .option("header", True)
        # Check the file header against the column names read above.
        .option("enforceSchema", False)
        .option("encoding", "UTF-8")
        .option("sep", delimiter)
        .option("quote", '"')
        .option("escape", '"')
        .option("unescapedQuoteHandling", "RAISE_ERROR")
        .option("multiLine", False)
        # Keep surrounding whitespace for Silver to handle.
        .option("ignoreLeadingWhiteSpace", False)
        .option("ignoreTrailingWhiteSpace", False)
        .option("mode", "FAILFAST")
        .csv(str(path))
    )


def source_name(source):
    """Include the year and station where the configured source has them."""
    name = source["dataset"]
    for field in ("year", "station_id"):
        if field in source:
            name += "_" + str(source[field])
    return name


def read_header(path):
    """Read the CSV or PSV header. Spark reads the data rows."""
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return next(csv.reader(handle, delimiter="|" if path.suffix == ".psv" else ","), [])


def selected_bronze_lineage(source, environment_root):
    """Return the source details for the original download."""
    # Both checksum fields identify the original download, including EPA's ZIP files.
    return {
        "source_provider": source["provider"],
        "source_dataset": source["dataset"],
        "source_version_sha256": source["download_sha256"],
        "source_file_path": source["download_path"].relative_to(environment_root).as_posix(),
        "source_file_sha256": source["download_sha256"],
        "source_manifest_path": source["manifest_path"].relative_to(environment_root).as_posix(),
        "source_record_hash_scheme": "sha256-json-raw-fields-v1",
    }


def attach_bronze_lineage(records, source, environment_root, source_columns):
    """Add download details and a hash of the parsed source fields to each row."""
    from pyspark.sql import functions as F
    metadata = selected_bronze_lineage(source, environment_root)
    # Hash the parsed source fields in a fixed order, including null values.
    raw_json = F.to_json(
        F.struct(*[F.col(name) for name in sorted(source_columns)]),
        options={"ignoreNullFields": "false"},
    )
    # Keep the source columns and add download details and a hash for each row.
    return records.select(
        "*", *[F.lit(value).alias(name) for name, value in metadata.items()],
        F.sha2(raw_json, 256).alias("source_record_sha256"),
    )
