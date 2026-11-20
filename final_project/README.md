# DAT535 final project

We use PySpark to find periods of elevated ozone at McAdory, Alabama, in 2025.
We pair EPA ozone readings with NOAA temperature reports from Alabaster and
record how much of the year has usable data. Temperature provides context for
the periods, without testing a causal relationship.

## Pipeline

- **Bronze:** Keep the five original EPA and NOAA downloads and store all raw
  columns as strings in reusable Parquet tables.
- **Silver:** Clean the readings and reference data, select hourly temperature
  reports, join them to ozone and calculate coverage.
- **Gold:** Find periods with ozone at or above 0.045 ppm for at least three
  consecutive UTC hours. Missing ozone breaks a period. Missing temperature does
  not. The rule and its calibration are saved in [gold_rules.json](config/gold_rules.json).

| Final table | Contents |
| --- | --- |
| `period_catalogue` | Period start, end, duration, ozone summaries and accompanying temperatures. |
| `monthly_coverage` | Monthly data availability, elevated hours and period counts. |

## Run the project

Run all commands below from the repository root. The VM setup expects
Spark/PySpark 3.5.0 at `/opt/spark`, Python 3.11 at
`/home/ubuntu/spark-env/bin/python`, Java 8 and the `spark-env` Jupyter kernel.
Spark uses the standalone master `spark://sparkvm-test:7077` with two cores.

Open [pipeline.ipynb](notebooks/pipeline.ipynb) with a fresh `spark-env` kernel
for an interactive run, or run the full pipeline with:

```bash
bash final_project/scripts/run_notebook.sh --environment dev
```

| Mode | What it runs |
| --- | --- |
| `full` (default) | Bronze, Silver and Gold. |
| `prepare` | Bronze and Silver, stopping after the hourly Silver completion report. |
| `analyse` | Gold using a selected hourly Silver release. |

For example, prepare a test release and then analyse it:

```bash
bash final_project/scripts/run_notebook.sh --environment test --test-run-id example --mode prepare
bash final_project/scripts/run_notebook.sh --environment test --test-run-id example \
  --mode analyse --hourly-receipt /path/from/prepare/hourly_silver_completion.json
```

Use the report path printed by Prepare. Keep the same environment and, for test
runs, the same test ID. Add `--gold-rules final_project/config/gold_threshold_demo.json`
to Analyse to try the saved 0.046 ppm threshold.

## Storage and reports

[sources.json](config/sources.json) lists the five input URLs and checksums.
The selected monitor is `01-073-1005-44201-1` and the station is `USW00053864`.
Bronze reuses verified Parquet. When conversion is needed, it checks a cached
download or downloads the configured URL. A checksum mismatch stops the run.

Data stays outside the repository. [environments.json](config/environments.json)
defines the dev, test and prod roots. Test runs add a subfolder for `--test-run-id`.

Each run has a `runs/<run-id>/reports/` folder for settings, Git/runtime details,
validation results and completion reports. The runner also saves the executed
notebook and can save it after a failure when the reports folder is available.
Saved releases have manifests describing their files and inputs.
See [storage-layout.md](docs/storage-layout.md) for the full layout.

## Local runs

Use a separate environment with Python 3.11, Java 17, PySpark 3.5.0, `nbformat`,
`nbclient` and `ipykernel`. Set `JAVA_HOME` to Java 17 and register the kernel
with that environment's Python:

```bash
/path/to/local/python -m ipykernel install --sys-prefix --name dat535-local
```

Provide an environments file with separate absolute dev, test and prod paths
outside the checkout. Local runs use the test environment and an explicit file:

```bash
/path/to/local/python -B final_project/src/run_notebook.py \
  --runtime local --environments-file /path/to/local-environments.json \
  --environment test --test-run-id local-example --mode full
```

## Checks

The unit tests cover configuration loading, storage selection and runner defaults:

```bash
python3 -B -m unittest discover -s final_project/tests -p 'test_*.py' -v
```

On the VM, run the Gold examples and offline notebook check with:

```bash
/home/ubuntu/spark-env/bin/python -B final_project/tests/gold_checks.py
/home/ubuntu/spark-env/bin/python -B final_project/tests/check_notebook.py --test-run-id offline-example
```

Gold checks use small test tables. Local runs can import the four check functions
and pass in a local Spark session. The offline check compiles notebook cells,
parses configuration JSON and tests a column example. Silver checks run in the notebook.

## GitHub Actions

- Pushes to `dev` run the unit tests and Full mode using dev storage.
- Same-repository `dev` pull requests into `main` run the unit tests, Gold checks
  and offline notebook check.
- Pushes to `main` run the unit tests and Full mode using prod storage. The
  production job uses the `main` GitHub environment.

Workflows use the self-hosted runner and upload report artifacts with 30-day
retention. Generated datasets stay in the configured storage folders.
