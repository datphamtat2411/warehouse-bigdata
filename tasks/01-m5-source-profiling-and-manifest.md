Read `AGENTS.md`.

Plan **M5 Source Profiling & Manifest** for M03 — Data Ingestion.

Start discovery around:
- existing manifest contracts, validators, and contract fixtures;
- local configuration/artifact conventions, CLI entry points, and focused data tests;
- source-level CSV parsing and profiling concerns.

Read only:
- `docs/02_SYSTEM_REQUIREMENTS.md`;
- `docs/04_DATASET.md`;
- `docs/06_DATA_PIPELINE.md`;
- `docs/CONTRACTS.md`;
- `docs/11_TESTING.md`.

Follow only task-related source dependencies. Avoid broad repository discovery.

Plan these capabilities:
- Profile already-acquired, extracted M5 CSVs from an explicit Ubuntu-local directory with supplied, truthful Kaggle provenance; do not download data.
- Require `sales_train_evaluation.csv`, `calendar.csv`, and `sell_prices.csv`; inventory optional `sales_train_validation.csv` and `sample_submission.csv` without combining overlapping sales histories.
- Validate complete evaluation day-column coverage (`d_1`–`d_1941`), required headers, well-formed CSV records, nonnull source identifiers, unique sales series/calendar keys, valid dates, nonnegative integral sales values, unique `(store_id, item_id, wm_yr_wk)` price keys, and positive prices.
- Produce full-source, bounded-memory profiling: actual row counts, byte sizes, SHA-256 hashes, duplicate/invalid counts, and relevant coverage statistics, distinguishing measured results from reference expectations.
- Produce `source_manifests.json` (an array of individually validated `SOURCE_MANIFEST` v1 records) and `source_profile.json` (quality findings and validation outcome) for Task 02.

Task-specific constraints:
- Derive one stable `source_version` from sorted present filenames and their SHA-256 hashes; all per-file manifests share it. Keep `source_snapshot_id` reserved for J02/J03.
- Reuse Contract v1 and `validate_manifest`; do not silently redesign shared contracts. Source manifests describe verified **local** files, not HDFS-validated artifacts.
- Fail clearly on blocking source defects; never publish `VALIDATED` manifests for rejected inputs. Preserve source bytes and valid zero-sales observations.
- Keep memory bounded on the 16 GB host; do not materialize entire CSVs or multi-million-key sets in RAM. Write generated artifacts only to ignored locations.

Out of scope:
- Kaggle acquisition/authentication; HDFS upload, replication, and remote integrity checks;
- Spark normalization, calendar/price joins, curated Parquet, and infrastructure changes.

Return `plan.md` with exactly:
- Task Plan
- Repository Findings
- Proposed Changes
- API Surface
- Configuration
- File Delta
- Focused Tests
- Out of Scope

Keep `plan.md` under roughly 150 lines. Organize Proposed Changes by capability; File Delta lists only Modify / Add / Remove. Do not modify repository files.
