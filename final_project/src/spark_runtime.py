"""Start the project on the configured Spark VM."""

import os
from pathlib import Path
import shlex
import sys


def configure_vm_python():
    """Require the VM spark-env interpreter and select the installed Spark 3.5.0 Python libraries."""
    if (
        sys.version_info[:2] != (3, 11)
        or Path(sys.prefix) != Path("/home/ubuntu/spark-env")
    ):
        raise RuntimeError("Select the VM's spark-env kernel, then restart it.")

    # Select the installed Spark distribution, Java and Python worker interpreter.
    spark_home = Path("/opt/spark")
    os.environ["SPARK_HOME"] = str(spark_home)
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
    os.environ["PYSPARK_PYTHON"] = "/home/ubuntu/spark-env/bin/python"

    # Search Spark's bundled libraries before the environment's site-packages.
    spark_archives = sorted((spark_home / "python" / "lib").glob("*.zip"))
    if not spark_archives:
        raise RuntimeError("The installed Spark Python libraries were not found.")

    sys.path[:0] = [str(path) for path in spark_archives]

    # These imports must come after the library-path setup.
    import pyspark

    if (
        pyspark.__version__ != "3.5.0"
        or not pyspark.__file__.startswith("/opt/spark/")
    ):
        raise RuntimeError(
            "Expected bundled PySpark 3.5.0. Restart the kernel and run Setup again."
        )


def start_silver_session(project_dir, context):
    """Start the configured UTC Spark session, verify it can run a job and return it."""
    from pyspark.sql import SparkSession
    import pyspark

    # Load the configured Spark master, Python and resource settings.
    spark_properties = project_dir / "config" / "spark.properties"
    os.environ["PYSPARK_SUBMIT_ARGS"] = shlex.join([
        "--properties-file", str(spark_properties),
        "pyspark-shell",
    ])

    # Use a warehouse inside this run's staging directory instead of writing into the repository.
    spark = (
        SparkSession.builder
        .appName(f"DAT535-Silver-{context.environment}")
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.sql.warehouse.dir",
            str(context.staging_root / "spark_warehouse"),
        )
        .getOrCreate()
    )

    try:
        if spark.version != pyspark.__version__:
            raise RuntimeError("Spark engine and PySpark versions differ.")

        if spark.sparkContext.master != "spark://sparkvm-test:7077":
            raise RuntimeError("Spark is not using the intended standalone master.")

        # Independent expectation: 0 + 1 + 2 + 3 + 4 = 10.
        total = spark.range(5).selectExpr("sum(id) AS total").first()["total"]
        if total != 10:
            raise RuntimeError(f"Startup check expected 10, received {total}.")

    # Close a session that failed startup validation before passing the error to the caller.
    except Exception:
        spark.stop()
        raise
    return spark
