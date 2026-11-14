# DAT535 final project

> This README still contains historical CP2 notes and needs a final project update.
> The separate CP1 application has been retired. Use the main notebook for the
> Bronze, Silver and Gold pipeline. The current workflow summary is below.

The project will combine EPA ozone observations with NOAA temperature
to produce an elevated-ozone catalogue and an independent coverage
summary. The core covers one historical year, one development region
and one reuse region. The current provisional profiles use 2025:
McAdory–Alabaster in Alabama for development and South DeKalb–Atlanta
in Georgia for reuse, with one site/station pairing per region.
Final profile validation and Gold rules remain for later checkpoints.

## Current scope: CP2

Implemented:
- Environment configuration and isolated storage paths.
- Run identities and shared stage interfaces.
- A synthetic Spark fixture with result checks and Parquet read-back.
- Development, PR and controlled-production workflow definitions.
- Notebook acquisition of EPA/NOAA inputs with source manifests and reuse.
- One shared availability check for the Alabama and Georgia profiles.

The stage entry points in src/stages.py remain unfinished. The main
notebook now implements Bronze acquisition and manifests; Silver and Gold
processing are not implemented.

Earlier manual VM runs verified fixture success and an intentional
validation failure with a nonzero launcher exit status.

The [development workflow run](https://github.com/OMNiBaNDi/group02-dat535/actions/runs/38471697171)
for commit `44fc15d` passed on 13 October 2026, including all eight
configuration tests, standalone Spark fixture execution and artifact
upload. The downloaded reports confirmed development storage,
Spark/PySpark 3.5.0, expected fixture results and successful Parquet
read-back. Fixture output remained unpublished.

Amund merged CP1 at b40b6b8 and verified production and synchronised
development execution. CP2 acquisition, reuse and both availability checks
have been verified locally and in a manual VM copy. CP2 is not yet accepted. Terminal notebook execution with NBClient has also
been checked on the VM. The development workflow also runs the notebook and has passed at commit
`9f51e18`. The PR workflow adds two offline notebook checks using tiny
generated files in isolated test storage. Production is configured to run the
notebook with refresh disabled; at that checkpoint CP2 production verification
is still pending.

## Project layout

| Location | Purpose |
| --- | --- |
| `config/` | Environment roots, Spark settings, source URLs and region profiles |
| `notebooks/pipeline.ipynb` | Main notebook for acquisition, checks and later Silver/Gold work |
| `src/project_config.py` | Configuration validation and environment selection |
| `src/run_context.py` | Run ID and derived storage paths |
| `src/stages.py` | Stage result contract and unfinished stage entry points |
| `scripts/run_notebook.sh` | Run the notebook on the VM using NBClient |
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

The notebook runtime loads `config/spark.properties` when starting Spark.
The driver runs on the VM in client mode; work is submitted to the
existing standalone master and worker.

The configured allocation is two executor cores, 1 GiB executor memory
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

## Retired CP1 example

The separate CP1 aggregation application, launcher and input files have been
removed. They remain available in Git history. The real notebook and Gold tests
now provide the Spark processing and Parquet checks used by the workflows.
Existing VM data and earlier Actions artifacts are not deleted by this cleanup.

## Storage

Persistent output is stored on the VM beneath
`/home/ubuntu/dat535-final-project-data/`, outside Git and runner checkouts.

See [storage-layout.md](docs/storage-layout.md) for environment roots,
directory purposes and the distinction between staging and publication.

## GitHub Actions

| Workflow file | Trigger | Run storage |
| --- | --- | --- |
| `final-project-dev.yml` | Push to `dev` | `dev/` |
| `final-project-pr.yml` | PR from this repository's `dev` to `main` | `test/pr-<PR-number>-<GitHub-run-id>-<attempt>/` |
| `final-project-prod.yml` | Push to `main` | `prod/` |

All three use the existing self-hosted runner and run configuration tests.
Development and production execute the full Bronze, Silver and Gold notebook.
They upload the notebook reports and the Silver/Gold reports; source datasets
stay on the VM. PR checks run isolated Gold Spark tests and offline notebook
checks against GitHub's proposed merge revision. The PR artifact contains the
offline check report, while Gold test results appear in the Actions log.

The production job references the existing GitHub Environment named
`main`. Its approval settings are separate from the application's
`prod` storage selection. Either listed group member can approve;
self-review prevention is disabled, administrator bypass is enabled,
and the Environment has no branch restrictions. The production workflow
itself triggers only on pushes to `main`.

The agreed delivery process is peer-reviewed `dev` to `main` PRs,
ordinary merge commits and subsequent synchronisation of persistent
`dev`. These workflow definitions do not themselves enforce PR review.

The notebook runner supplies report locations through `GITHUB_OUTPUT` for
artifact uploads. Uploads can still run after failure when a report path is
available and the job has not been cancelled. CP1 artifacts are no longer created.

GitHub artifact copies have 30-day retention. This does not remove persistent
VM files. Dataset directories are not uploaded by these workflows.


## Main notebook

Use `notebooks/pipeline.ipynb` for acquisition and the shared availability
checks. It will also hold the later Spark Silver and Gold work. Save it,
restart the Python kernel and run top to bottom from inside the project folder.
Normal runs reuse published files after checking existence and size. Set
`PIPELINE_REFRESH_SOURCES=true` in the kernel environment for an explicit
provider check. Multiple saved versions require a checksum in `source_versions`.

Local WSL uses config/environments.local.json (ignored by Git); configure
absolute dev/test/prod paths outside the checkout. On the VM, set
PIPELINE_TARGET=vm in the notebook kernel environment to use
config/environments.json. PIPELINE_ENVIRONMENT defaults to dev;
test needs PIPELINE_TEST_RUN_ID.
The notebook currently uses standard-library Python. Future Spark stages
must use the documented Spark runtime. The development workflow also runs
the notebook. See the workflow section above for the current PR and production checks.

For terminal execution on the VM, from the repository root:

```bash
bash final_project/scripts/run_notebook.sh --environment dev
```

This uses the registered `spark-env` kernel. The executed notebook is saved
beside that run's report, leaving the source notebook unchanged. The launcher
also accepts `--test-run-id` for `test` and `--refresh-sources` for an explicit
provider refresh.

Each run writes reports/run.json under its run directory, including on
acquisition failure. Source manifests retain their original provenance.
Fresh duplicates of verified stored versions are removed only after
successful publication; historical staging and failed attempts remain.


Normal reuse trusts the recorded header assessment and does not rehash stored
inputs; treat snapshots/manifests as read-only. New downloads receive SHA-256
hashes and required-column checks (bronze-columns-v2). Existing v1 manifests
remain readable. Runs save the effective configuration and record the Git
revision and uncommitted-change status when available. A copied folder without
Git metadata is labelled `manual_copy`, with those Git fields left unknown.
Use committed reviewed code for final reproducibility evidence.
