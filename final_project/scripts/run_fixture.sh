#!/usr/bin/env bash

# Stop on a failed command, an unset variable, or a failed command in a pipe.
set -euo pipefail

# Find this script's folder, even if we call it from somewhere else.
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# The project folder is one level above scripts/.
project_dir="$(cd -- "$script_dir/.." && pwd)"

# Use the Spark installation already on the VM.
export SPARK_HOME=/opt/spark
# Avoid creating Python __pycache__ files during the run.
export PYTHONDONTWRITEBYTECODE=1

# Check that spark-submit exists and can be executed; fi ends this check.
if [[ ! -x "$SPARK_HOME/bin/spark-submit" ]]; then
    printf '%s\n' \
        "Cannot execute /opt/spark/bin/spark-submit. Run this launcher on the Spark VM." >&2
    exit 1
fi

# exec replaces this shell with Spark, keeping Spark's exit status.
# --properties-file loads our Spark settings.
# The Python path selects the CP1 fixture entry point.
# "$@" passes on arguments such as --environment dev without changing them.
# Each backslash continues the same command on the next line.
exec "$SPARK_HOME/bin/spark-submit" \
    --properties-file "$project_dir/config/spark.properties" \
    "$project_dir/src/run_fixture.py" \
    "$@"
