# Project storage layout

Code and configuration belong in Git. Generated data, private verification and
runtime environments stay outside the repository and runner checkouts.

## Environment and run roots

VM storage is `/home/ubuntu/dat535-final-project-data/{dev,test,prod}`. Each test
invocation adds `<test-run-id>/` below its test root. Local execution requires an
explicit file with three non-overlapping absolute roots and uses test storage.
Existing WSL dev/prod data is not an acceptance-output location.

One context/run ID covers Bronze, Silver and Gold. A test invocation can contain
several attempts (including Prepare followed by Analyse), each with a new run ID.

| Relative path | Contents |
| --- | --- |
| `data/bronze/<provider>/<source-name>/<download-sha256>/` | Preserved downloads; historical snapshots may also contain extracted CSV |
| `published/bronze/<provider>/<source-name>/<original-sha256>/manifest.json` | Historical raw manifests, preserved unchanged |
| `data/bronze_parquet/<provider>/<source-name>/<download-sha256>/<reader-version>/` | Raw string columns saved as Parquet |
| `published/bronze_parquet/<provider>/<source-name>/<download-sha256>/<reader-version>/manifest.json` | Completed Bronze version-2 manifest linking download identity and Parquet files |
| `data/silver/source/<region>/<year>/<publication-id>/` | Eight source tables, including three audits |
| `data/silver/hourly/<region>/<year>/<publication-id>/` | Six hourly integration/coverage tables |
| `data/gold/<region>/<year>/<publication-id>/` | Period catalogue and monthly coverage |
| `published/<layer>/<region>/<year>/<publication-id>/manifest.json` | Completed release metadata; Silver layers are `silver/source` and `silver/hourly`, Gold is `gold` |
| `staging/<run-id>/` | Incomplete downloads and unpublished Parquet bundles |
| `runs/<run-id>/reports/` | Effective settings, acquisition/validation reports, completion receipts and executed notebook |

## Completion boundaries

The five checksums in `sources.json` select accepted downloaded bytes. Bronze
first opens a completed Parquet version and verifies its listed files. This warm
path does not read or hash raw downloads or extracted CSV files.

If Parquet is missing, Bronze verifies the original ZIP/CSV/PSV, downloading it
into staging if necessary. ZIP members are extracted only in the temporary
conversion directory. Spark completes its Parquet write before that directory
is removed. The table directory moves to its final location before the new
manifest is written. A downloaded original remains available after a failed
conversion. Cached-original acquisition metadata says reused; existing legacy
retrieval metadata remains in its original manifest.

New row lineage identifies the preserved download. Both existing file/version
checksum columns use its one source checksum; no temporary CSV path or checksum
is stored in rows. Parquet files retain their own integrity checksums. Original
raw manifests, extracted files and historical releases are not migrated.
The existing `.bronze-publication.lock` directory coordinates the whole phase.
Abrupt termination can leave a stale lock requiring investigation.

Source/hourly Silver retain their staging, validation and publication contracts.
Gold stages both tables in a temporary `gold-` directory, reopens them, calculates
coverage/counts from saved data and moves the bundle before publishing its
manifest. Its release parent's `.publish.lock` coordinates cooperating publishers.
Staging/final paths must support the same-filesystem rename used by publication.

A directory alone is not completed. Loaders require explicit manifests/checksums
and verify their listed files and schemas. Silver also verifies saved row counts;
Gold calculates counts and coverage on write and uses file hashes on reload.
Unlisted files are ignored rather than discovered and read. Gold fails on an occupied destination or
manifest. An orphan destination stays untouched; a fresh attempt normally has a
new identity. There is no automatic repair or historical-output cleanup.

Preserve original manifests and data. Historical releases are opened using their
saved manifests and parent links. Gold manifests record dataset keys, files and
the selected hourly parent. Original ZIP and extracted CSV hashes identify
distinct files in historical snapshots. Receipts identify exact releases, never
the newest folder.

## Reports and retention

Setup prints the report directory before runtime-dependent operations. All
layers reuse one effective-config/provenance record (Git revision/dirty status,
settings and actual runtime), without per-code-file hashes. Analyse records its selected
receipt and rule; configured sources are not evidence that Bronze ran.

The runner exports `run_id` and `run_root` through `GITHUB_OUTPUT`. Dev/prod each
upload one report artifact from `run_root/reports/`, including the executed copy
after failure when a context exists. The PR offline artifact remains separate.
Artifact retention is 30 days. Reports are not complete dataset backups, and
artifact expiry does not remove persistent VM files.
