"""Run the project notebook and expose its Bronze, Silver and Gold report paths."""

import argparse
import json
import os
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Run the project notebook on the VM.")
    parser.add_argument("--environment", required=True, choices=("dev", "test", "prod"))
    parser.add_argument("--test-run-id", help="Required for the test environment.")
    parser.add_argument("--refresh-sources", action="store_true")
    args = parser.parse_args()
    if args.environment == "test" and not args.test_run_id:
        parser.error("The test environment needs --test-run-id.")

    # Import after parsing so --help works without the notebook packages.
    import nbformat
    from nbclient import NotebookClient

    project_dir = Path(__file__).resolve().parents[1]
    os.environ.update(
        PIPELINE_TARGET="vm",
        PIPELINE_ENVIRONMENT=args.environment,
        PIPELINE_REFRESH_SOURCES=str(args.refresh_sources).lower(),
    )
    if args.test_run_id:
        os.environ["PIPELINE_TEST_RUN_ID"] = args.test_run_id
    else:
        os.environ.pop("PIPELINE_TEST_RUN_ID", None)

    notebook = nbformat.read(project_dir / "notebooks/pipeline.ipynb", as_version=4)
    for cell in notebook.cells:
        if cell.cell_type == "code":
            cell.outputs = []
            cell.execution_count = None

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
        # Discover this execution's report paths from its captured output.
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

        if report_path is not None:
            executed_path = report_path.parent / "pipeline.executed.ipynb"
            nbformat.write(notebook, executed_path)
            print("Execution copy saved:", executed_path)

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

            # Keep the Bronze outputs and expose the separate Silver report directory.
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

    if report_path is None:
        raise RuntimeError("Notebook finished without an acquisition run report.")
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
