# M05-T03 — Feature Snapshot Pipeline & Publication

Read `AGENTS.md`.

Plan J04's validated, reproducible Spark feature snapshot pipeline for M05. Transform T01's historical features into immutable HDFS artifacts consumable by M06. Do not implement.

**Start discovery around:**
- M04/J03 validated `sales_enriched` snapshots, completion markers, dataset manifests, and source lineage;
- Existing Spark job orchestration, HDFS staging, owner-controlled publication, permissions, and retry mechanisms;
- Frozen `FORECAST_FEATURES` v1.0, distributed validation, and focused pipeline/integration tests.

Start at `src/warehouse_bigdata/processing/enrichment_analytics_snapshot.py`, `src/warehouse_bigdata/jobs/enrich_analytics.py`, `src/warehouse_bigdata/common/hdfs_publication.py`, and T01's feature builder. Follow task-related dependencies only.

**Read only:**
- `docs/02_SYSTEM_REQUIREMENTS.md` — FR-05, DR-06, NFR-04–06;
- `docs/06_DATA_PIPELINE.md` — §§8–9, 11–12;
- `docs/07_MACHINE_LEARNING.md` — §§5–7, 16.1;
- `docs/11_TESTING.md` — §§6.2, 11.1–11.3;
- `docs/CONTRACTS.md` — feature schema, identities, artifact paths, manifests, and lifecycle.

**Plan these capabilities:**

- **J04 execution:** Provide a batch entry point using the existing Spark Standalone submission runtime. Accept explicit source, snapshot, feature version, and execution identities. Do not independently select the latest dataset.

- **Validated upstream consumption:** Require matching J03 `VALIDATED` completion evidence, dataset manifests, immutable `sales_enriched` Parquet, and consistent raw-to-curated lineage. Reject incomplete, conflicting, or unvalidated inputs before feature production.

- **Feature transformation:** Invoke T01's approved Spark DataFrame interface without duplicating feature algorithms. Preserve historical train/validation/test roles, eligibility rules, observed labels, as-of dates, and source/feature lineage. Do not sample history before feature construction.

- **Candidate materialization:** Write historical `forecast_features` Parquet into execution-owned HDFS staging. Use the canonical immutable destination:

  `/data/warehouse/features/demand_forecasting/source_snapshot_id=<source_snapshot_id>/feature_version=<feature_version>/`

  Avoid destructive writes, uncontrolled append retries, excessive small files, and driver-side materialization.

- **Distributed quality validation:** Independently reread persisted Parquet. Validate exact contract schema, types, nullability, domain constraints, scoped uniqueness, temporal boundaries, role/label consistency, lineage, and artifact readability. Reconcile eligible/excluded coverage and reason counts with T01 diagnostics. Block invalid or incomplete snapshots.

- **Lineage and evidence:** Record source/version identities, upstream J03 binding, `pipeline_run_id`, feature version, code/configuration revisions, schema fingerprint, row/role counts, exclusions, quality results, timestamps, and relevant execution metrics. Preserve existing manifest contracts; do not introduce undeclared required fields or fabricated identities.

- **Safe publication:** Reuse or minimally extend existing owner-controlled HDFS promotion. Publish only validated candidates, seal immutable outputs, and write completion evidence last. An HDFS `_SUCCESS` marker alone is insufficient. Do not modify J08's approved serving pointer.

- **Idempotency and recovery:** Reuse existing validated artifacts only after verifying matching identities and metadata. Reject conflicts without overwriting published data. Preserve failure evidence and support safe recovery from interrupted staging or promotion without exposing partial snapshots.

**Task-specific constraints:**
- Respect frozen Contract v1.0 and existing HDFS ownership/security boundaries.
- Use distributed Spark operations and bounded resources appropriate for the 16 GB WSL2 host; avoid unnecessary infrastructure or speculative optimization.
- Reuse T01 unchanged where possible. Keep T02 scoring construction independent.
- Report unresolved contract or publication-lifecycle conflicts rather than silently altering them.

**Focused verification must cover:**
- Invalid upstream lineage, missing manifests, and source mismatches;
- Persisted schema, uniqueness, role coverage, eligibility reconciliation, and readback integrity;
- Successful publication, matching reuse, conflicting retries, partial failures, and recovery;
- Focused offline tests plus explicitly opted-in Spark–HDFS integration verification, with actual execution evidence.

**Out of scope:**
T01 feature algorithm redesign, T02 as-of scoring, model training, recursive inference, forecast evaluation, ClickHouse publication, Streamlit, and cross-module performance benchmarking.

Return `plan.md` with **exactly these sections, in order:**

1. **Task Plan**
2. **Repository Findings**
3. **Proposed Changes**
4. **API Surface**
5. **Configuration**
6. **File Delta**
7. **Focused Tests**
8. **Out of Scope**

Keep `plan.md` within roughly 150 lines.

Organize **Proposed Changes** by capability/concern, not by file. Use **API Surface** for the J04 job interface, transformation integration, artifact contracts, and publication boundaries—not HTTP endpoints. Record runtime/resource parameters in **Configuration**. Keep **File Delta** strictly to Modify / Add / Remove inventory. Include precise verification commands and acceptance criteria under **Focused Tests**.

This task-specific eight-section PLAN format takes precedence over the generic four-section PLAN outline in `AGENTS.md`; retain all other repository rules and report the discrepancy.

Do not modify repository files.
