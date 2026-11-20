"""Configure Python and start Spark on the VM or locally."""

import os
from pathlib import Path
import shlex
import sys


def configure_vm_python():
    """Check the VM interpreter and load its bundled PySpark 3.5.0 libraries."""
    if (
        sys.version_info[:2] != (3, 11)
        or Path(sys.prefix) != Path("/home/ubuntu/spark-env")
    ):
        raise RuntimeError("Select the VM's spark-env kernel, then restart it.")

    # Use the VM's installed Spark and the same Python interpreter for its workers.
    spark_home = Path("/opt/spark")
    os.environ["SPARK_HOME"] = str(spark_home)
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
    os.environ["PYSPARK_PYTHON"] = "/home/ubuntu/spark-env/bin/python"

    # Search Spark's bundled libraries before the environment's site-packages.
    spark_archives = sorted((spark_home / "python" / "lib").glob("*.zip"))
    if not spark_archives:
        raise RuntimeError("The installed Spark Python libraries were not found.")

    sys.path[:0] = [str(path) for path in spark_archives]

    # Set the library paths before importing PySpark.
    import pyspark

    if (
        pyspark.__version__ != "3.5.0"
        or not pyspark.__file__.startswith("/opt/spark/")
    ):
        raise RuntimeError(
            "Expected bundled PySpark 3.5.0. Restart the kernel and run Setup again."
        )


def configure_runtime(runtime):
    """Select one runtime before importing the Spark-dependent project modules."""
    if runtime == "vm":
        configure_vm_python()
    elif runtime == "local":
        # Clear inherited Spark settings before using the local environment.
        for name in ("SPARK_HOME", "SPARK_CONF_DIR", "PYSPARK_SUBMIT_ARGS"):
            os.environ.pop(name, None)
        # Use the current Python interpreter for both the driver and workers.
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
        import pyspark
        if sys.version_info[:2] != (3, 11) or pyspark.__version__ != "3.5.0":
            raise RuntimeError("Local runs require Python 3.11 and PySpark 3.5.0.")
    else:
        raise ValueError("Runtime must be vm or local.")


def start_session(project_dir, context, *, runtime="vm"):
    """Start or reuse a UTC Spark session and check its version and master."""
    from pyspark.sql import SparkSession
    import pyspark

    # Use the VM's master, Python and resource settings from spark.properties.
    if runtime == "vm":
        spark_properties = project_dir / "config" / "spark.properties"
        os.environ["PYSPARK_SUBMIT_ARGS"] = shlex.join([
            "--properties-file", str(spark_properties), "pyspark-shell",
        ])

    # Keep Spark's table warehouse in this run's staging folder.
    builder = (
        SparkSession.builder
        .appName(f"DAT535-{context.environment}")
        # Use UTC when Spark parses timestamps and groups hours.
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.sql.warehouse.dir",
            str(context.staging_root / "spark_warehouse"),
        )
    )
    if runtime == "local":
        # Run locally with two threads and two shuffle partitions.
        builder = (builder.master("local[2]")
                   .config("spark.sql.shuffle.partitions", "2")
                   .config("spark.pyspark.python", sys.executable)
                   .config("spark.pyspark.driver.python", sys.executable))
    spark = builder.getOrCreate()

    try:
        if spark.version != pyspark.__version__:
            raise RuntimeError("Spark engine and PySpark versions differ.")

        expected_master = "local[2]" if runtime == "local" else "spark://sparkvm-test:7077"
        if spark.sparkContext.master != expected_master:
            raise RuntimeError("Spark is not using the intended standalone master.")

    except Exception:
        # Stop the session if a startup check fails.
        spark.stop()
        raise
    return spark
