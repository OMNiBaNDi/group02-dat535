# Project storage layout

This document defines the agreed storage conventions.
It does not imply that all pipeline stages or publication safeguards
have been implemented.

## Storage locations

Code, safe configuration, documentation and tiny test fixtures belong
in the Git repository.

Persistent project data belongs on the Spark VM under:
`/home/ubuntu/dat535-final-project-data/`

Generated datasets must remain outside Git and the runner's managed
checkout.

## Environment roots

| Environment | Root on the VM |
| --- | --- |
| Development | `/home/ubuntu/dat535-final-project-data/dev/` |
| Production | `/home/ubuntu/dat535-final-project-data/prod/` |
| Test invocation | `/home/ubuntu/dat535-final-project-data/test/<test-run-id>/` |

Each test invocation receives its own root. Development, test and
production must not write into each other's locations.

## Contents of each environment root

| Relative path | Purpose |
| --- | --- |
| `data/bronze/` | Preserved source snapshots |
| `data/silver/` | Completed Silver dataset versions |
| `data/gold/` | Completed Gold dataset versions |
| `staging/<run-id>/<stage>/` | Unfinished downloads or processing outputs |
| `published/bronze/` | Manifests identifying published Bronze snapshots |
| `published/silver/` | Manifests identifying validated Silver preparations |
| `published/gold/` | Manifests identifying validated, paired Gold releases |
| `runs/<run-id>/logs/` | Execution logs |
| `runs/<run-id>/reports/` | Run summaries and verification evidence |

A test-run ID identifies a test invocation, which may contain several
pipeline attempts. A run ID identifies one pipeline attempt.

Create directories when needed. A directory's existence alone does not
mean its contents are complete or approved for downstream use.

## Publication and retention

Staging outputs are not ready for downstream consumption.
Publication manifests identify completed versions that passed the
checks required for their layer.

Failed attempts must not replace a previous successful publication.
Preserve the input and output versions needed for recorded-version replay.
Cleanup rules and ownership must be documented before cleanup is automated.

## GitHub Actions artifacts

Upload selected reports, manifest copies and small output samples for
review. Uploading an artifact creates a separate copy on GitHub; it does
not commit the files to Git or move the VM data.

GitHub artifact retention and VM file retention are independent.
Selected artifacts are not a complete backup of the project datasets.

## Details introduced by later checkpoints

- CP2 defines Bronze source/dataset/year/version paths.
- CP3-CP4 define Silver datasets, rejection evidence and preparation paths.
- CP5 defines Gold dataset and paired-release paths.

Update this document in the same PR as changes to storage conventions.
Clearly distinguish implemented behaviour from planned behaviour.
