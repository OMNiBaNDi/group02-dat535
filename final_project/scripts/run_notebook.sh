#!/usr/bin/env bash

# Stop if a command fails or a required variable is missing.
set -euo pipefail

# Locate the project relative to this script, regardless of the terminal folder.
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
project_dir="$(cd -- "$script_dir/.." && pwd)"

# Avoid creating Python __pycache__ files during the run.
export PYTHONDONTWRITEBYTECODE=1

# Start our runner with the VM's spark-env Python.
# exec keeps its exit status so a notebook failure also fails the workflow.
# "$@" forwards all options, including --environment and --mode.
# The Python runner uses Full when no mode is supplied.
exec /home/ubuntu/spark-env/bin/python "$project_dir/src/run_notebook.py" "$@"
