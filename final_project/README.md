# DAT535 final project

The project will combine EPA ozone observations with NOAA temperature
to produce an elevated-ozone catalogue and an independent coverage
summary. The core covers one historical year, one development region
and one reuse region. Exact regions and numerical policies remain
decisions for their assigned checkpoints.

## Current scope: CP1 foundation

Implemented:
- Environment configuration and isolated storage paths.
- Run identities and shared stage interfaces.
- A synthetic Spark fixture with result checks and Parquet read-back.
- Development, PR and controlled-production workflow definitions.

Bronze, Silver and Gold entry points currently raise
`NotImplementedError`. Real ingestion, preparation, analysis and
publication are not implemented.

Earlier manual VM runs verified fixture success and an intentional
validation failure with a nonzero launcher exit status.

The [development workflow run](https://github.com/OMNiBaNDi/group02-dat535/actions/runs/38471697171)
for commit `44fc15d` passed on 13 October 2026, including all eight
configuration tests, standalone Spark fixture execution and artifact
upload. The downloaded reports confirmed development storage,
Spark/PySpark 3.5.0, expected fixture results and successful Parquet
read-back. Fixture output remained unpublished.

PR workflow execution, peer review and controlled-production
verification remain pending. CP1 is not yet accepted.

## Project layout

| Location | Purpose |
| --- | --- |
| `config/` | Spark settings and environment storage roots |
| `src/project_config.py` | Configuration validation and environment selection |
| `src/run_context.py` | Run ID and derived storage paths |
| `src/stages.py` | Stage result contract and unfinished stage entry points |
| `src/fixture_processing.py` | Fixture transformations and result checks |
| `src/fixture_stage.py` | Parquet round-trip verification and fixture report |
| `src/run_fixture.py` | Command-line entry point, logging and run report |
| `scripts/run_fixture.sh` | Launch through the installed Spark distribution |
| `tests/` | Configuration tests and tiny synthetic input |
| `docs/` | Maintained project documentation |

GitHub workflows live at repository root in `.github/workflows/`.
Historical labs and their `.yml.example` workflows remain inactive.

## Runtime

Use the existing Spark VM and runner:
- Spark 3.5.0 at `/opt/spark`.
- Python 3.11 at `/home/ubuntu/spark-env/bin/python`.
- Java 8 at `/usr/lib/jvm/java-8-openjdk-amd64`.
- Standalone master: `spark://sparkvm-test:7077`.

The launcher uses `spark-submit` with `config/spark.properties`.
The driver runs on the VM in client mode; work is submitted to the
existing standalone master and worker.

Initial fixture allocation is two executor cores, 1 GiB executor memory
and 1 GiB driver memory. This is a single-VM setup, not evidence of
multi-machine scaling. The entry point checks that Spark and imported
PySpark versions match.

## Configuration tests

From the repository root in WSL:

```bash
python3 -B -m unittest discover \
  -s final_project/tests \
  -p 'test_*.py' \
  -v
```

These standard-library tests do not require Spark. They check invalid
configuration, overlapping roots and test-invocation isolation.

## Manual fixture execution

On the Spark VM, use a manual code snapshot outside the runner's
managed `_work` directory. From that snapshot's root directory,
which contains `final_project/`:

```bash
bash final_project/scripts/run_fixture.sh \
  --environment test \
  --test-run-id cp1-manual-001
```

The fixture contains six synthetic records. It checks three records
per group and sums of 60 for `alpha` and 45 for `beta`, writes Parquet,
then reads it back and checks the stored results.

A test-run ID identifies the test invocation. Each launch also creates
a fresh pipeline run ID, so repeated launches have separate run and
staging directories.

The launcher returns zero on success and nonzero on failure.
For runs that reach reporting setup, inspect:
- `runs/<run-id>/reports/run.json` for overall status and runtime details.
- `runs/<run-id>/reports/fixture.json` for successful fixture verification.
- `runs/<run-id>/logs/run.log` for Python application logging.

Full Spark console output appears in the terminal or Actions log.
Very early setup failures may occur before a run report is created.

Fixture Parquet files remain under
`staging/<run-id>/fixture/group_totals/`. They are not published datasets,
including when the fixture runs in the production environment.

## Storage

Persistent output is stored on the VM beneath
`/home/ubuntu/dat535-final-project-data/`, outside Git and runner checkouts.

See [storage-layout.md](docs/storage-layout.md) for environment roots,
directory purposes and the distinction between staging and publication.

## GitHub Actions

| Workflow file | Trigger | Fixture storage |
| --- | --- | --- |
| `final-project-dev.yml` | Push to `dev` | `dev/` |
| `final-project-pr.yml` | PR from this repository's `dev` to `main` | `test/pr-<PR-number>-<GitHub-run-id>-<attempt>/` |
| `final-project-prod.yml` | Push to `main` | `prod/` |

All three use the existing self-hosted runner, run configuration tests
and then execute the fixture. PR checks test GitHub's proposed merge
revision.

The production job references the existing GitHub Environment named
`main`. Its approval settings are separate from the application's
`prod` storage selection. Either listed group member can approve;
self-review prevention is disabled, administrator bypass is enabled,
and the Environment has no branch restrictions. The production workflow
itself triggers only on pushes to `main`.

The agreed delivery process is peer-reviewed `dev` to `main` PRs,
ordinary merge commits and subsequent synchronisation of persistent
`dev`. These workflow definitions do not themselves enforce PR review.

When running in Actions, the entry point supplies its run ID and run
directory through `GITHUB_OUTPUT`. Each workflow uploads that run's logs
and reports, including after a fixture failure when the run path is
available and the job has not been cancelled.

GitHub artifact copies have 30-day retention. This does not remove
persistent VM files. Dataset directories are not uploaded by these
CP1 workflows.
