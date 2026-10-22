#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
project_dir="$(cd -- "$script_dir/.." && pwd)"

export SPARK_HOME=/opt/spark
export PYTHONDONTWRITEBYTECODE=1

if [[ ! -x "$SPARK_HOME/bin/spark-submit" ]]; then
    printf '%s\n' \
        "Cannot execute /opt/spark/bin/spark-submit. Run this launcher on the Spark VM." >&2
    exit 1
fi

exec "$SPARK_HOME/bin/spark-submit" \
    --properties-file "$project_dir/config/spark.properties" \
    "$project_dir/src/run_fixture.py" \
    "$@"
