"""Offline check of notebook syntax, source columns and configuration."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys

PROJECT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_DIR / "src"))
from run_context import create_run_context
from source_columns import check_columns, required_columns


def check_schema():
    """Check that a complete header passes and a missing required column blocks the source."""
    source = {"source": "epa", "dataset": "sites"}
    columns = required_columns[("epa", "sites")]
    valid = check_columns(source, columns)
    # Drop one required column to check the blocked case as well.
    missing = check_columns(source, columns[:-1])
    assert valid["status"] == "compatible", "Valid columns were not accepted."
    assert missing["status"] == "blocked", "Missing columns were not blocked."
    return {"valid_columns": valid["status"], "missing_column": missing["status"]}


def main():
    """Check syntax and configuration without running Spark, then save a report for the PR."""
    parser = argparse.ArgumentParser(description="Check notebook syntax, input columns and configuration offline.")
    parser.add_argument("--test-run-id", required=True)
    parser.add_argument("--environments-file", type=Path,
                        default=PROJECT_DIR / "config/environments.json")
    args = parser.parse_args()
    # Always use isolated test storage, even when running this from WSL.
    context = create_run_context(args.environments_file, "test", test_run_id=args.test_run_id)
    context.reports_dir.mkdir(parents=True)
    report_path = context.reports_dir / "notebook_checks.json"
    report = {
        "scope": "offline synthetic checks", "status": "failure", "environment": "test",
        "test_run_id": args.test_run_id, "run_id": context.run_id,
        "code_revision": os.environ.get("GITHUB_SHA"), "checks": [],
    }
    current_check = "Load notebook and configuration"
    try:
        # Let the workflow find this run's report for the artifact upload.
        github_output = os.environ.get("GITHUB_OUTPUT")
        if github_output:
            outputs = {"run_id": context.run_id, "run_root": str(context.run_root)}
            if any("\n" in value or "\r" in value for value in outputs.values()):
                raise ValueError("GitHub output values must be single-line.")
            with Path(github_output).open("a", encoding="utf-8") as handle:
                for name, value in outputs.items():
                    handle.write(f"{name}={value}\n")

        notebook_path = PROJECT_DIR / "notebooks/pipeline.ipynb"
        report["notebook_sha256"] = hashlib.sha256(notebook_path.read_bytes()).hexdigest()
        # Compile checks Python syntax only. It does not download data or execute these cells.
        for cell in json.loads(notebook_path.read_text(encoding="utf-8"))["cells"]:
            if cell["cell_type"] == "code":
                compile("".join(cell["source"]), str(notebook_path), "exec")
        # Check that the input configuration files are valid JSON too.
        for filename in ("sources.json", "alabama_profile.json", "georgia_profile.json"):
            json.loads((PROJECT_DIR / "config" / filename).read_text(encoding="utf-8"))
        current_check = "Required columns"
        details = check_schema()
        report["checks"].append({"name": current_check, "status": "success", "details": details})
        print(f"PASS: {current_check}")
        report["status"] = "success"
    except Exception as error:
        message = f"{type(error).__name__}: {error}"
        report["checks"].append({"name": current_check, "status": "failure", "error": message})
        print(f"FAIL: {current_check}: {message}", file=sys.stderr)
    finally:
        # Save the results even if a check fails.
        report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print("Check report:", report_path)
    # A nonzero exit status makes the GitHub Actions step fail.
    return 0 if report["status"] == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
