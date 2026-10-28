#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
project_dir="$(cd -- "$script_dir/.." && pwd)"

# Avoid creating Python __pycache__ files during the run.
export PYTHONDONTWRITEBYTECODE=1

# Start our runner with the VM's spark-env Python.
# exec keeps its exit status; "$@" forwards arguments such as --environment dev.
exec /home/ubuntu/spark-env/bin/python "$project_dir/src/run_notebook.py" "$@"
