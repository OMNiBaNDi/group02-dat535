"""Run the saved notebook in its selected runtime and collect its reports.

Full runs Bronze, Silver and Gold. Prepare stops after hourly Silver. Analyse
loads a selected hourly Silver release and runs Gold without preparing inputs.
A completion receipt is the small JSON report that identifies a finished release."""

import argparse
import json
import os
import sys
from pathlib import Path

def build_parser():
    """Define runner options. Full on the VM is the default."""
    parser = argparse.ArgumentParser(description="Run the project notebook with pinned source inputs.")
    parser.add_argument("--environment", required=True, choices=("dev", "test", "prod"))
    parser.add_argument("--test-run-id", help="Required for the test environment.")
    parser.add_argument("--runtime", choices=("vm", "local"), default="vm")
    parser.add_argument("--environments-file", type=Path)
    parser.add_argument("--mode", choices=("full", "prepare", "analyse"), default="full")
    parser.add_argument("--hourly-receipt", type=Path)
    parser.add_argument("--gold-rules", type=Path, help="Use a saved Gold rule in Analyse mode.")
    return parser


def main():
    """Run the selected cells, save an executed copy and check the completion reports."""
    parser = build_parser()
    args = parser.parse_args()

    # Reject inconsistent options before starting a notebook kernel.
    if args.mode == "analyse" and not args.hourly_receipt:
        parser.error("analyse requires --hourly-receipt")
    if args.mode != "analyse" and args.hourly_receipt:
        parser.error("--hourly-receipt is for analyse")
    if args.gold_rules and args.mode != "analyse":
        parser.error("--gold-rules is for analyse")
    if args.environment == "test" and not args.test_run_id:
        parser.error("The test environment needs --test-run-id.")

    if args.runtime == "local" and (args.environment != "test" or not args.environments_file):
        parser.error("--runtime local requires --environment test and --environments-file")
    if args.environment != "test" and args.test_run_id:
        parser.error("--test-run-id is only valid for test")

    # Import after parsing so --help works without the notebook packages.
    import nbformat
    from nbclient import NotebookClient

    project_dir = Path(__file__).resolve().parents[1]
    # Pass the selected options to the notebook kernel.
    # Clear unused options so values from the shell do not affect this run.
    os.environ.update(
        PIPELINE_RUNTIME=args.runtime,
        PIPELINE_MODE=args.mode,
        PIPELINE_ENVIRONMENT=args.environment,
    )
    if args.environments_file:
        os.environ["PIPELINE_ENVIRONMENTS_FILE"] = str(args.environments_file.resolve(strict=True))
    else:
        os.environ.pop("PIPELINE_ENVIRONMENTS_FILE", None)
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

    # Use cell IDs to find the sections needed by Prepare and Analyse.
    # Keep these IDs when editing the notebook.
    notebook = nbformat.read(project_dir / "notebooks/pipeline.ipynb", as_version=4)
    ids = [cell.id for cell in notebook.cells]
    if len(ids) != len(set(ids)):
        raise ValueError("Notebook cell IDs must be unique.")
    if args.mode == "analyse":
        cells = notebook.cells
        gold_start = ids.index("68585365")   # Gold heading
        gold_end = ids.index("8f313e77")     # Publish and reload Gold

        # Analyse needs only setup and Gold. Bronze and Silver preparation are omitted.
        setup_ids = [
            "configuration",
            "8f038332",           # Start Spark and create this run's context
            "alabama-profile-load",
        ]
        notebook.cells = [cells[ids.index(cell_id)] for cell_id in setup_ids]
        notebook.cells += cells[gold_start:gold_end + 1]
    # Prepare stops after hourly Silver is reloaded and its completion report is written.
    elif args.mode == "prepare":
        silver_end = ids.index("483e66f4")  # Reload hourly Silver
        notebook.cells = notebook.cells[:silver_end + 1]

    # Clear saved outputs and execution counts before running this copy.
    for cell in notebook.cells:
        if cell.cell_type == "code":
            cell.outputs = []
            cell.execution_count = None

    # Run the selected cells with the project folder as the working directory.
    client = NotebookClient(
        notebook,
        kernel_name="dat535-local" if args.runtime == "local" else "spark-env",
        timeout=600,
        resources={"metadata": {"path": str(project_dir)}},
    )
    reports_dir = None
    completion_paths = {}
    completion_names = {"source_silver_completion.json", "hourly_silver_completion.json", "gold_completion.json"}
    execution_error = None
    print(f"Starting notebook: {args.environment}, {args.runtime}, {args.mode}", flush=True)
    try:
        client.execute()
    except Exception as error:
        execution_error = error
        raise
    finally:
        # Collect report paths from the notebook output, including after a failure.
        for cell in notebook.cells:
            parts = []
            for output in cell.get("outputs", []):
                if output.output_type == "stream":
                    print(output.text, end="")
                    if output.name == "stdout":
                        parts.append(output.text)
            # Join stdout fragments before looking for the printed report paths.
            for line in "".join(parts).splitlines():
                if cell.get("id") == "8f038332" and line.startswith("Reports directory: "):
                    reports_dir = Path(line.removeprefix("Reports directory: "))
                elif line.startswith("Completion report: "):
                    path = Path(line.removeprefix("Completion report: "))
                    if path.name in completion_names:
                        completion_paths[path.name] = path
        # Try to save an executed copy if setup printed its reports folder.
        if reports_dir is not None:
            try:
                if any(path.parent != reports_dir for path in completion_paths.values()):
                    raise RuntimeError("Completion reports belong to a different run.")
                executed_path = reports_dir / "pipeline.executed.ipynb"
                nbformat.write(notebook, executed_path)
                print("Execution copy saved:", executed_path)
                # Pass the run ID and folder to later GitHub Actions steps.
                github_output = os.environ.get("GITHUB_OUTPUT")
                if github_output:
                    outputs = {"run_id": reports_dir.parent.name, "run_root": str(reports_dir.parent)}
                    if any("\n" in value or "\r" in value for value in outputs.values()):
                        raise ValueError("GitHub output values must be single-line.")
                    with Path(github_output).open("a", encoding="utf-8") as handle:
                        for name, value in outputs.items():
                            handle.write(f"{name}={value}\n")
            except Exception as save_error:
                # Keep the notebook error if saving its run details also fails.
                if execution_error is None:
                    raise
                print(f"Could not save execution evidence: {save_error}", file=sys.stderr)
        else:
            print("No report directory was created before failure.")
    # Check that the Gold report names the Silver release selected by --hourly-receipt.
    if args.mode == "analyse":
        report_path = completion_paths.get("gold_completion.json")
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
    if reports_dir is None:
        raise RuntimeError("Notebook finished without a reports directory.")
    # Prepare needs both Silver reports from this run and no Gold report.
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
            or source["run_id"] != reports_dir.parent.name
        ):
            raise RuntimeError("The Silver receipts do not describe one successful preparation.")

        print("Hourly Silver receipt:", hourly_path)
        print("Prepare finished successfully.")
        return 0
    # Full needs all three reports from this run, with matching Silver and Gold inputs.
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
    # Confirm hourly Silver points back to this run's source Silver release.
    source = json.loads(completion_paths["source_silver_completion.json"].read_text(encoding="utf-8"))
    if (source["status"] != "success" or source["environment"] != args.environment
            or source["run_id"] != gold["run_id"]
            or hourly["source_silver_publication_id"] != Path(source["manifest_path"]).parent.name):
        raise RuntimeError("Source and hourly Silver completion reports do not describe this run.")
    print("Notebook finished successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
