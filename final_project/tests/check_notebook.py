"""Two small offline checks for the CP2 notebook."""

import argparse
import ast
import calendar
from collections import Counter
import contextlib
import csv
from datetime import datetime
import hashlib
import io
import json
import math
import os
from pathlib import Path
import sys

PROJECT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_DIR / "src"))
from run_context import create_run_context


def load_helpers(path):
    notebook = json.loads(path.read_text(encoding="utf-8"))
    names = {"check_columns", "check_region"}
    definitions = []

    # The notebook functions need the same imports as this script.
    namespace = globals().copy()
    for cell in notebook["cells"]:
        if cell["cell_type"] != "code":
            continue
        source = "".join(cell["source"])
        compile(source, str(path), "exec")  # Check syntax without running the cell.

        # Read the code structure so we can skip downloads and other cell commands.
        for node in ast.parse(source).body:
            if isinstance(node, ast.FunctionDef) and node.name in names:
                definitions.append(node)
            elif isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id == "required_columns":
                        namespace["required_columns"] = ast.literal_eval(node.value)

    # Load the two functions together. None of them is called at this point.
    code = ast.Module(body=definitions, type_ignores=[])
    exec(compile(code, str(path), "exec"), namespace)
    if not names.issubset(namespace) or "required_columns" not in namespace:
        raise ValueError("The notebook is missing a helper or its column lists.")
    return namespace


def check_schema(helpers, folder):
    source = {"source": "epa", "dataset": "sites"}
    columns = helpers["required_columns"][("epa", "sites")]
    valid = helpers["check_columns"](source, columns)
    # Drop one required column to check the blocked case as well.
    missing = helpers["check_columns"](source, columns[:-1])
    assert valid["status"] == "compatible", "Valid columns were not accepted."
    assert missing["status"] == "blocked", "Missing columns were not blocked."
    return {"valid_columns": valid["status"], "missing_column": missing["status"]}


def write_rows(path, fields, rows, delimiter=","):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, delimiter=delimiter)
        writer.writeheader()
        writer.writerows(rows)


def check_availability(helpers, folder):
    # Use made-up IDs and measurements instead of reading the real datasets.
    profile = {
        "region": "Synthetic example", "status": "test", "year": 2025,
        "ozone_monitor": {
            "state_code": "01", "county_code": "001", "site_number": "0001",
            "parameter_code": "44201", "poc": "1",
        },
        "weather_mapping": {"epa_site_id": "01-001-0001", "noaa_station_id": "TEST"},
        "temperature_policy": {"source_quality_pairs": [["223", "1"], ["343", "5"]]},
    }
    ozone_path = folder / "ozone.csv"
    weather_path = folder / "weather.psv"
    # All three ozone readings are on the same day, one in each hour.
    base = {
        "State Code": "01", "County Code": "001", "Site Num": "0001",
        "Parameter Code": "44201", "POC": "1", "Date GMT": "2025-01-01",
        "Units of Measure": "Parts per million",
    }
    ozone_rows = [
        {**base, "Time GMT": "00:00", "Sample Measurement": "0.05"},
        {**base, "Time GMT": "01:00", "Sample Measurement": "0.06"},
        {**base, "Time GMT": "02:00", "Sample Measurement": "-0.001"},
    ]
    write_rows(ozone_path, helpers["required_columns"][("epa", "hourly_ozone")], ozone_rows)
    # Two accepted reports share hour 00. They should count as one hour.
    # Hour 01 has an unaccepted code pair; hour 02 has no temperature.
    weather_rows = []
    for stamp, value, source, quality in [
        ("2025-01-01T00:10:00", "10", "223", "1"),
        ("2025-01-01T00:50:00", "11", "343", "5"),
        ("2025-01-01T01:20:00", "12", "999", ""),
        ("2025-01-01T02:15:00", "", "223", "1"),
    ]:
        weather_rows.append({
            "STATION": "TEST", "DATE": stamp, "temperature": value,
            "temperature_Source_Code": source, "temperature_Quality_Code": quality,
            "temperature_Report_Type": "FM15",
        })
    write_rows(weather_path, helpers["required_columns"][("noaa", "hourly_weather")],
               weather_rows, delimiter="|")
    # Use the same input layout as acquisition, pointing at our tiny files.
    inputs = [
        {"request": {"source": "epa", "dataset": "hourly_ozone", "year": 2025},
         "data_path": ozone_path, "schema": {"status": "compatible"}, "manifest_path": None},
        {"request": {"source": "noaa", "dataset": "hourly_weather", "year": 2025,
                     "station_id": "TEST"},
         "data_path": weather_path, "schema": {"status": "compatible"}, "manifest_path": None},
    ]
    # Keep the terminal output short; save the counts in the report below.
    with contextlib.redirect_stdout(io.StringIO()):
        result = helpers["check_region"](profile, inputs)
    months = result["monthly"]
    january = months[0]
    # Only hour 00 has qualifying temperature, so two ozone hours are missing it.
    expected = {
        "ozone_hours": 3, "numeric_temperature_hours": 2,
        "trial_temperature_hours": 1, "shared_hours": 1, "ozone_without_temperature": 2,
    }
    for field, value in expected.items():
        assert january[field] == value, f"{field}: expected {value}, got {january[field]}"
    assert math.isclose(january["overlap_percent"], 100 / 3), "Overlap percentage differs."
    assert [month["month"] for month in months] == list(range(1, 13)), "Missing months."
    for month in months[1:]:
        assert month["ozone_hours"] == 0, "Unexpected ozone in an empty month."
        assert month["overlap_percent"] is None, "Empty months should have no percentage."
    assert sum(result["temperature_codes"].values()) == 4, "Report count differs."
    return {"january": january, "weather_reports": 4, "empty_months": 11}


def main():
    parser = argparse.ArgumentParser(description="Run two offline CP2 notebook checks.")
    parser.add_argument("--test-run-id", required=True)
    parser.add_argument("--environments-file", type=Path,
                        default=PROJECT_DIR / "config/environments.json")
    args = parser.parse_args()
    # Always use isolated test storage, even when running this from WSL.
    context = create_run_context(args.environments_file, "test", test_run_id=args.test_run_id)
    context.reports_dir.mkdir(parents=True)
    folder = context.staging_root / "cp2_checks"
    folder.mkdir(parents=True)
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
        helpers = load_helpers(notebook_path)
        # Check that the input configuration files are valid JSON too.
        for filename in ("sources.json", "alabama_profile.json", "georgia_profile.json"):
            json.loads((PROJECT_DIR / "config" / filename).read_text(encoding="utf-8"))
        for current_check, check in [
            ("Required columns", check_schema),
            ("Availability counts", check_availability),
        ]:
            details = check(helpers, folder)
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
