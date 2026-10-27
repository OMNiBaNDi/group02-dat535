"""Run the project notebook on the VM and keep its output with the run report."""

import argparse
import os
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Run the CP2 notebook on the VM.")
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
    print(f"Starting notebook: {args.environment}", flush=True)
    try:
        client.execute()
    finally:
        # The notebook prints its own run report path, including on acquisition failure.
        for cell in notebook.cells:
            for output in cell.get("outputs", []):
                if output.output_type == "stream":
                    print(output.text, end="")
                    if cell.get("id") == "execute-bronze":
                        for line in output.text.splitlines():
                            if line.startswith("Run report: "):
                                report_path = Path(line.removeprefix("Run report: "))

        if report_path is not None:
            executed_path = report_path.parent / "pipeline.executed.ipynb"
            nbformat.write(notebook, executed_path)
            print("Execution copy saved:", executed_path)

            # Give Actions the same run paths that the CP1 fixture provides.
            github_output = os.environ.get("GITHUB_OUTPUT")
            if github_output:
                run_root = report_path.parent.parent
                outputs = {"run_id": run_root.name, "run_root": str(run_root)}
                if any("\n" in value or "\r" in value for value in outputs.values()):
                    raise ValueError("GitHub output values must be single-line.")
                with Path(github_output).open("a", encoding="utf-8") as handle:
                    for name, value in outputs.items():
                        handle.write(f"{name}={value}\n")
        else:
            print("No acquisition run report was produced.")

    if report_path is None:
        raise RuntimeError("Notebook finished without an acquisition run report.")
    print("Notebook finished successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
