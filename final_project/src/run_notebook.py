"""Run the saved notebook on the VM and collect its reports.

Full runs Bronze, Silver and Gold. Prepare stops after hourly Silver. Analyse
loads a selected hourly Silver release and runs Gold without preparing inputs.
A completion receipt is the small JSON report that identifies a finished release."""

import argparse
import json
import os
from pathlib import Path

def build_parser():
    """Define runner options; Full without source refresh remains the default."""
    parser = argparse.ArgumentParser(description="Run the project notebook on the VM.")
    parser.add_argument("--environment", required=True, choices=("dev", "test", "prod"))
    parser.add_argument("--test-run-id", help="Required for the test environment.")
    parser.add_argument("--refresh-sources", action="store_true")
    parser.add_argument("--mode", choices=("full", "prepare", "analyse"), default="full")
    parser.add_argument("--hourly-receipt", type=Path)
    parser.add_argument("--gold-rules", type=Path, help="Use a saved Gold rule in Analyse mode.")
    return parser


def main():
    """Execute the selected notebook mode, save its executed copy and verify completion receipts."""
    parser = build_parser()
    args = parser.parse_args()

    # Reject inconsistent options before starting a notebook kernel.
    if args.mode == "analyse" and not args.hourly_receipt:
        parser.error("analyse requires --hourly-receipt")
    if args.mode != "analyse" and args.hourly_receipt:
        parser.error("--hourly-receipt is for analyse")
    if args.gold_rules and args.mode != "analyse":
        parser.error("--gold-rules is for analyse")
    if args.mode == "analyse" and args.refresh_sources:
        parser.error("analyse reads saved Silver; source refresh belongs to full")
    if args.environment == "test" and not args.test_run_id:
        parser.error("The test environment needs --test-run-id.")

    # Import after parsing so --help works without the notebook packages.
    import nbformat
    from nbclient import NotebookClient

    project_dir = Path(__file__).resolve().parents[1]
    # Pass explicit settings to the fresh kernel. Clear optional values when not selected
    # so old shell settings cannot accidentally choose another receipt or rule.
    os.environ.update(
        PIPELINE_TARGET="vm",
        PIPELINE_ENVIRONMENT=args.environment,
        PIPELINE_REFRESH_SOURCES=str(args.refresh_sources).lower(),
    )
    if args.mode == "analyse":
        os.environ["PIPELINE_HOURLY_RECEIPT"] = str(args.hourly_receipt.resolve(strict=True))
    else:
        os.environ.pop("PIPELINE_HOURLY_RECEIPT", None)
    if args.gold_rules:
        os.environ["PIPELINE_GOLD_RULES"] = str(args.gold_rules.resolve(strict=True))
    else:
        os.environ.pop("PIPELINE_GOLD_RULES", None)
    if args.test_run_id:
        os.environ["PIPELINE_TEST_RUN_ID"] = args.test_run_id
    else:
        os.environ.pop("PIPELINE_TEST_RUN_ID", None)

    # IDs identify the required cells even if their position changes. Keep these IDs
    # when editing the notebook, since Prepare and Analyse depend on them.
    notebook = nbformat.read(project_dir / "notebooks/pipeline.ipynb", as_version=4)
    if args.mode == "analyse":
        cells = notebook.cells
        ids = [cell.id for cell in cells]
        gold_start = ids.index("68585365")   # Section 10 heading
        gold_end = ids.index("8f313e77")     # Publish and reload Gold

        # Analyse needs only setup and Gold. Bronze and Silver preparation are omitted.
        setup_ids = [
            "configuration",
            "8f038332",           # Start Spark and create this run's context
            "metadata-helpers",   # Defines write_json, used by the Gold receipt
            "alabama-profile-load",
        ]
        notebook.cells = [cells[ids.index(cell_id)] for cell_id in setup_ids]
        notebook.cells += cells[gold_start:gold_end + 1]
    # Prepare keeps the notebook through the successful hourly Silver receipt and stops before Gold.
    elif args.mode == "prepare":
        ids = [cell.id for cell in notebook.cells]
        silver_end = ids.index("483e66f4")  # Section 9.4d: reload hourly Silver
        notebook.cells = notebook.cells[:silver_end + 1]

    # Remove old outputs from the execution copy so results shown in this run are newly produced.
    for cell in notebook.cells:
        if cell.cell_type == "code":
            cell.outputs = []
            cell.execution_count = None

    # Start the existing spark-env kernel with final_project as its working directory.
    client = NotebookClient(
        notebook,
        kernel_name="spark-env",
        timeout=600,
        resources={"metadata": {"path": str(project_dir)}},
    )
    report_path = None
    completion_paths = {}
    completion_names = {
        "source_silver_completion.json",
        "hourly_silver_completion.json",
        "gold_completion.json",
    }
    print(f"Starting notebook: {args.environment}", flush=True)
    try:
        client.execute()
    finally:
        # The notebook prints report paths. Collect them even after a failure,
        # so an executed copy can be saved when a report location is available.
        for cell in notebook.cells:
            stream_parts = []
            for output in cell.get("outputs", []):
                if output.output_type == "stream":
                    print(output.text, end="")
                    if output.name == "stdout":
                        stream_parts.append(output.text)

            for line in "".join(stream_parts).splitlines():
                if cell.get("id") == "execute-bronze" and line.startswith("Run report: "):
                    report_path = Path(line.removeprefix("Run report: "))
                elif line.startswith("Completion report: "):
                    completion_path = Path(line.removeprefix("Completion report: "))
                    if completion_path.name in completion_names:
                        completion_paths[completion_path.name] = completion_path
        # Analyse has no Bronze report; use its new Gold receipt as the execution-output location.
        if args.mode == "analyse":
            report_path = completion_paths.get("gold_completion.json")
        if report_path is not None:
            executed_path = report_path.parent / "pipeline.executed.ipynb"
            nbformat.write(notebook, executed_path)
            print("Execution copy saved:", executed_path)

            # Silver and Gold receipts from this execution must belong to one report directory.
            silver_report_dirs = {
                path.parent for path in completion_paths.values()
            }
            if len(silver_report_dirs) > 1:
                raise RuntimeError("Silver and Gold completion reports belong to different runs.")
            silver_reports_dir = next(iter(silver_report_dirs), None)
            if silver_reports_dir is not None:
                if not silver_reports_dir.is_dir():
                    raise FileNotFoundError(silver_reports_dir)
                print("Silver reports:", silver_reports_dir)

            # Tell Actions where to find this run's reports for artifact uploads.
            # silver_reports_dir also contains Gold reports when Gold has run.
            github_output = os.environ.get("GITHUB_OUTPUT")
            if github_output:
                run_root = report_path.parent.parent
                outputs = {"run_id": run_root.name, "run_root": str(run_root)}
                if silver_reports_dir is not None:
                    outputs["silver_reports_dir"] = str(silver_reports_dir)
                if any("\n" in value or "\r" in value for value in outputs.values()):
                    raise ValueError("GitHub output values must be single-line.")
                with Path(github_output).open("a", encoding="utf-8") as handle:
                    for name, value in outputs.items():
                        handle.write(f"{name}={value}\n")
        else:
            print("No acquisition run report was produced.")
    # Verify that the new Gold receipt refers to the exact selected Silver release.
    if args.mode == "analyse":
        if report_path is None or not report_path.is_file():
            raise RuntimeError("Analyse finished without a Gold completion report.")

        gold = json.loads(report_path.read_text(encoding="utf-8"))
        hourly = json.loads(args.hourly_receipt.read_text(encoding="utf-8"))
        expected_parent = {
            key: hourly[key] for key in ("publication_id", "manifest_path", "manifest_sha256")
        }
        if (
            gold["status"] != "success"
            or gold["environment"] != args.environment
            or gold["run_id"] != report_path.parent.parent.name
            or gold["parent_hourly"] != expected_parent
        ):
            raise RuntimeError("Gold completion does not match the selected Silver release.")

        print("Analyse finished successfully.")
        return 0
    if report_path is None:
        raise RuntimeError("Notebook finished without an acquisition run report.")
    # Prepare must produce both Silver receipts and no Gold receipt.
    if args.mode == "prepare":
        source_path = completion_paths.get("source_silver_completion.json")
        hourly_path = completion_paths.get("hourly_silver_completion.json")
        if source_path is None or hourly_path is None:
            raise RuntimeError("Prepare finished without both Silver receipts.")
        if "gold_completion.json" in completion_paths:
            raise RuntimeError("Prepare unexpectedly ran Gold.")

        source = json.loads(source_path.read_text(encoding="utf-8"))
        hourly = json.loads(hourly_path.read_text(encoding="utf-8"))
        if (
            source["status"] != "success" or hourly["status"] != "success"
            or source["environment"] != args.environment or hourly["environment"] != args.environment
            or source["run_id"] != hourly["run_id"]
        ):
            raise RuntimeError("The Silver receipts do not describe one successful preparation.")

        print("Hourly Silver receipt:", hourly_path)
        print("Prepare finished successfully.")
        return 0
    # Full must finish every layer and link Gold to this execution's hourly Silver release.
    missing_reports = completion_names - completion_paths.keys()
    if missing_reports:
        raise RuntimeError(
            f"Notebook finished without required completion reports: {sorted(missing_reports)}"
        )
    gold = json.loads(completion_paths["gold_completion.json"].read_text(encoding="utf-8"))
    hourly = json.loads(completion_paths["hourly_silver_completion.json"].read_text(encoding="utf-8"))
    if (
        gold["status"] != "success" or hourly["status"] != "success"
        or gold["environment"] != args.environment or hourly["environment"] != args.environment
        or gold["run_id"] != hourly["run_id"]
        or gold["run_id"] != completion_paths["gold_completion.json"].parent.parent.name
        or gold["parent_hourly"] != {key: hourly[key] for key in (
            "publication_id", "manifest_path", "manifest_sha256"
        )}
    ):
        raise RuntimeError("Gold completion does not match this run's hourly Silver release.")
    if not completion_paths["source_silver_completion.json"].is_file():
        raise FileNotFoundError(completion_paths["source_silver_completion.json"])
    print("Notebook finished successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
