# Cross-Module Contract Catalog

**Document ID:** `CONTRACTS`  
**Contract catalog version:** `1.0`  
**Status:** Contract v1 approved for FND-06 implementation; cataloged contracts are frozen. Calendar and weekly-price lookup contracts are not defined by Contract v1.  
**Scope:** FND-05 interface metadata only; no Spark, HDFS, ClickHouse, or Streamlit implementation is asserted.

This document is the navigation index for the machine-readable contracts in
[`src/warehouse_bigdata/contracts/`](../src/warehouse_bigdata/contracts/). The catalog uses immutable standard-library dataclasses and records logical interfaces without creating a schema registry. Detailed business meaning remains owned by the domain documents listed in the final section.

## Catalog Rules

- Logical dataset names, field names, grains, keys, provenance classes, and versions are declared in `datasets.py`.
- Manifest categories, run identities, HDFS location templates, publication context, and application read operations are declared in `manifests.py`.
- Contract versions use `major.minor` notation. Every dataset and manifest currently declares `1.0`.
- A compatible addition increments the minor version. Field removal or renaming, required-field addition, nullability tightening, grain/key change, semantic/provenance change, lineage semantic change, or run-type removal increments the major version.
- A physical serving rename is compatible only when logical meaning, precision, nullability, and query behavior remain compatible and the mapping is recorded here and in the owning database document.
- Consumers must declare the contract versions they support. Publication manifests record the exact version for every included dataset.
- No compatibility claim is made without an explicit adapter or migration rule.

## Producer and Consumer Matrix

| Logical contract | Producer -> consumer | Grain and logical key | Provenance |
|---|---|---|---|
| `sales_daily` | J02 -> J03/J04/J08 | `(item_id, store_id, date)` within `source_snapshot_id` | Observed M5 unit sales |
| `sales_enriched` | J03 -> J04/J08 | `(item_id, store_id, date)` within `source_snapshot_id` | Derived from observed M5 |
| `store_daily_sales` | J03 -> J08/Streamlit | `(source_snapshot_id, store_id, date)` | Aggregate of observed M5 |
| `category_monthly_sales` | J03 -> J08/analytics consumers | `(source_snapshot_id, cat_id, month_start)` | Aggregate of observed M5 |
| `forecast_features` | J04 -> J05/J06 | `(item_id, store_id, target_date)` scoped by source, feature version, and role | Leakage-controlled derived features |
| `forecast_daily` | J06 -> J07/J08/Streamlit | `(forecast_run_id, item_id, store_id, forecast_date)` | Predicted unit sales |
| `forecast_metrics` | J05/J06 -> J08/Streamlit | `(evaluation_run_id, holdout_id, method_id, metric_name, horizon)` | Forecast-quality evaluation |
| `inventory_scenario` | J07 -> J07/J08/Streamlit | `(scenario_id, issue_date, item_id, store_id)` | Simulated store inputs |
| `central_supply_scenario` | J07 -> J07/J08/Streamlit | `(scenario_id, issue_date, item_id)` | Simulated product-level supply |
| `replenishment_recommendations` | J07 -> J08/Streamlit | `(decision_run_id, item_id, store_id)` | Calculated recommendation under simulation |
| `allocation_recommendations` | J07 -> J08/Streamlit | `(decision_run_id, policy_id, item_id, store_id)` | Calculated simulated allocation |
| `inventory_policy_evaluation` | J07 -> J08/Streamlit | `(evaluation_run_id, decision_run_id, policy_id, item_id, store_id, issue_date)` | Historical simulated policy outcome |

The catalog preserves the distinction between observed `unit_sales`, predicted `predicted_units`, simulated stock/supply, calculated decisions, and evaluated historical outcomes. No row contract represents M5 sales as verified demand, inventory, orders, or customer transactions.

`CATEGORY_MONTHLY_SALES` Contract v1 (`category_monthly_sales`, version `1.0`) has
exactly four fields: `source_snapshot_id` STRING, `cat_id` STRING,
`month_start` DATE, and `total_units` LONG. Its unique key is
`(source_snapshot_id, cat_id, month_start)`; `total_units` is nonnegative and must
reconcile exactly to the matching source snapshot's observed `unit_sales` total.

## Canonical Storage Mappings

| Logical dataset | Serving dataset | Explicit mapping | Omitted or additional metadata |
|---|---|---|---|
| `sales_daily` | `sales_daily` | `date` -> `sale_date`; `unit_sales` remains `unit_sales` | `units_sold` is a query alias only; `series_id`, `day_key`, and `wm_yr_wk` remain in Parquet/manifest metadata |
| `store_daily_sales` | `store_daily_sales` | `date` -> `sale_date`; `total_units` remains `total_units` | `units_sold` is a query alias only |
| `category_monthly_sales` | `category_monthly_sales` | Logical fields are retained; `total_units` is a validated nonnegative Long | Source snapshot remains in the row and dataset manifest |
| `forecast_daily` | `forecast_daily` | `predicted_units` remains `Float64`; no daily rounding | `forecast_status` is serving completeness metadata; `raw_prediction` remains in Parquet pending explicit serving-retention approval |
| `inventory_scenario` | `inventory_scenario` | Logical and serving field names are unchanged | Scenario-generation metadata remains in its manifest |
| `central_supply_scenario` | `central_supply_scenario` | Logical and serving field names are unchanged | Capacity is one product-level row and must not be summed per store |
| `replenishment_recommendations` | `replenishment` | Logical dataset name maps to the serving table name | The logical name remains the producer/consumer identity |
| `allocation_recommendations` | `allocation` | Logical dataset name maps to the serving table name | The logical name remains the producer/consumer identity |
| `forecast_metrics` | `forecast_metrics` | Logical and serving field names are unchanged | Scored forecast metrics are not a `model_metrics` dataset |
| `inventory_policy_evaluation` | `policy_evaluation` | Logical dataset name maps to the serving table name | Historical policy outcomes remain separate from forecast metrics |

All compatible numeric casts are validated before publication. ClickHouse `ORDER BY` is not logical uniqueness enforcement. Required lineage is authoritative in row fields where declared, otherwise in the manifest or identity-bearing path identified by the catalog.

## Run Identities

| Identifier | Meaning | Owner | Required propagation |
|---|---|---|---|
| `source_version` | Acquired raw source package identity | J01 | Raw path and source manifest |
| `source_snapshot_id` | Validated curated source derivation identity | J02/J03 quality gates | Downstream rows/manifests and publication context |
| `pipeline_run_id` | One execution attempt, including failed attempts | Stage owner | Run manifests, staging paths, quality summaries |
| `feature_version` | Public feature schema and as-of transformation identity | J04 | Feature artifact, model manifest, forecast rows |
| `model_run_id` | One trained model artifact and parameter set | J05 | Model, forecast, decision, publication |
| `forecast_run_id` | One scoring invocation | J06 | Forecast key, decisions, publication |
| `scenario_id` | One immutable simulated input scenario | J07 | Scenario rows, decisions, publication |
| `decision_run_id` | One decision batch bound to forecast, scenario, policy, and issue date | J07 | Replenishment/allocation and publication |
| `evaluation_run_id` | One independently rerunnable evaluation artifact or bundle | J05/J07 | Forecast metrics, policy outcomes, evaluation manifest |
| `publication_id` | One candidate or approved serving release | J08 | Publication manifest, pointer, application copy |
| `policy_id` | Stable policy family | J07 | Allocation and policy-evaluation rows |
| `policy_version` | Immutable policy implementation/configuration revision | J07 | Decision/evaluation rows and manifests |

`feature_run_id`, `scenario_run_id`, and `export_run_id` are not catalog identities. `model_run_id` is model lineage; it is not the forecast batch key. `forecast_run_id` is the uniqueness component for `forecast_daily`. `decision_run_id` is the uniqueness component for decision outputs.

## Manifests and Lifecycle

The catalog declares `source_manifest`, `pipeline_run_manifest`, `model_manifest`, `forecast_manifest`, `scenario_manifest`, `decision_manifest`, `evaluation_manifest`, and `publication_manifest`. Each includes the common manifest schema/version, producer, artifact checksum, schema fingerprint, grain, logical key, row count, null-count summary, code/configuration revisions, timestamps, and applicable lineage.

Lifecycle meanings are separate:

1. `COMPLETED` means the process produced an artifact.
2. `VALIDATED` means schema, quality, lineage, row-count, and uniqueness checks passed.
3. `FAILED` means the artifact is not eligible for promotion.
4. `APPROVED` is a publication-only state after serving reconciliation and application-readiness checks.

`_SUCCESS` or a successful process exit does not imply validation or approval. Failed/incomplete artifacts never update the approved pointer. A retry receives a new immutable execution or publication identity. The previous approved publication remains active when a candidate fails.

`source_manifest` Contract v1 keeps `downloaded_at_utc` as a required field, but the
field is intentionally nullable. A non-null value remains subject to `TimestampUTC`
validation; JSON `null` means the actual acquisition time is unknown and is not a source
quality failure. Existing timestamp-bearing v1 manifests remain valid, and this
nullability relaxation does not change the Contract v1 identity or version. The profile
artifact's `profiled_at_utc` is separate profiling evidence and is not added as a
redundant source-manifest lifecycle field.

## HDFS Artifact Conventions

### Standard dataset scope evidence — `m5-scope-v1`

Contract v1 dataset fields, schema fingerprints and strict manifest fields are unchanged.
Every new scoped artifact carries an immutable `_scope.json` sidecar. It contains:
`dataset_scope`, `selection_policy`, `selection_seed`, `source_version`,
`source_snapshot_id`, sorted `selected_item_ids`, `selected_store_ids`,
`selected_department_ids`, `selected_sku_count`, `selected_series_count`,
`original_series_count`, `excluded_series_count`, `historical_day_count`,
`expected_observation_count`, and `selection_digest`. Version 1 also requires
`schema_version`, `available_department_ids`, and sorted `selected_series` identity records
(`id`, `item_id`, `dept_id`, `cat_id`, `store_id`, `state_id`) to validate exact membership.

The digest is `sha256:` plus SHA-256 of the entire sidecar **except `selection_digest`**,
serialized with sorted keys, ASCII escaping, compact comma/colon separators, no NaN,
and one trailing LF. Arrays/series use their canonical sorted order. Manifests bind
`dataset_scope` and `selection_digest` inside the existing JSON
`coverage_and_exclusion_counts` field. J04/model/forecast/evaluation bindings embed the
identical scope object; file inventories include its sidecar. Consumers and publishers
validate the digest, source/snapshot, exact population and upstream completion status.
Missing scope evidence cannot imply full mode or authorize a historical smoke input.

`demo_sample` requires ten SKUs × ten original stores = 100 series, all 1,941 days,
and exactly 194,100 observations. J05 uses at most 10,000 eligible training rows. J06 and
evaluation consume the frozen ten-SKU cohort without resampling; actual scored coverage
may be lower, with exclusions. Full-mode reference contracts remain available only with
explicit execution selection and authorization. Practical commands and recovery:
[SMALL_DEMO_MODE.md](SMALL_DEMO_MODE.md).

The catalog records logical locations only; it does not create HDFS directories.

```text
/data/warehouse/raw/m5/source_version=<source_version>/
/data/warehouse/staging/pipeline_run_id=<pipeline_run_id>/
/data/warehouse/curated/sales_daily/source_snapshot_id=<source_snapshot_id>/
/data/warehouse/curated/sales_enriched/source_snapshot_id=<source_snapshot_id>/
/data/warehouse/curated/store_daily_sales/source_snapshot_id=<source_snapshot_id>/
/data/warehouse/curated/category_monthly_sales/source_snapshot_id=<source_snapshot_id>/
/data/warehouse/features/demand_forecasting/source_snapshot_id=<source_snapshot_id>/feature_version=<feature_version>/
/data/warehouse/models/<model_run_id>/
/data/warehouse/outputs/forecasts/<forecast_run_id>/
/data/warehouse/outputs/decisions/<decision_run_id>/
/data/warehouse/outputs/evaluation/<evaluation_run_id>/
/data/warehouse/manifests/runs/<pipeline_run_id>.json
/data/warehouse/manifests/publications/<publication_id>.json
/data/warehouse/manifests/published/current.json
```

Data paths are immutable by identity. Each J03 curated dataset contains a validated
`_dataset_manifest.json` with its exact schema, contract version, source lineage, physical
schema, row count, and reconciliation measures. The final validated J03
`pipeline_run_manifest` is the completion marker for the complete three-output bundle;
consumers must reject a run when that marker or any referenced dataset manifest is absent.
Manifest paths contain metadata. `published/current.json` is an approval pointer, not a data directory.

### Frozen chronological forecast evaluation (M06-T03)

Each phase has its own `evaluation_run_id` and independent immutable bundle at
`outputs/evaluation/<evaluation_run_id>/`; evaluation never changes a sealed model.
Validation origins are d_1885, d_1892, d_1899, d_1906 (outcomes d_1886–d_1913).
Test origins are d_1913, d_1920, d_1927, d_1934 (outcomes d_1914–d_1941).
Holdout IDs are `m5-chronological-paired-v1-validation` and
`m5-chronological-paired-v1-test`. Each origin's RF forecast ID is deterministically
derived from the evaluation ID and day index, and mapped in binding/manifest evidence.

Before forecasting, validation persists the unchanged T01 model/checksum/training
selection binding, frozen M5 Small cohort evidence, seed, feature/source lineage,
code revision, origins, preprocessing and analytical policy in `input_binding.json`.
Test explicitly selects a sealed validation evaluation and reuses its cohort; the
model, source, code and analytical freeze must match before test scoring or label
alignment. Preparation reads observed history only through each origin. Seasonal
naïve uses the original observed `T+h-7` value, without fallback or imputation.

`forecast_metrics` remains Contract v1: exactly 48 rows per phase (two methods ×
three metrics × horizons 0–7). Horizon 0 pools all paired observations across all
four origins. Both methods use identical valid observed-label keys, including
partially labelled series. MAE is SAE/N, RMSE is sqrt(SSE/N), and WAPE is
100×SAE/sum(actual units). Zero actual sum gives null WAPE; empty slices have zero
counts/sums and null scores. Aggregate `forecast_run_id` is null; only RF metrics
carry `model_run_id`. Squared-error sums live in evidence, outside the v1 schema.

Bundles contain `metrics`, `metric_statistics`, `paired`, `alignment`, `coverage`,
`exceptions`, `cohort_selection`, `diagnostics`, `seven_day_errors`, plus per-origin
`origins/d_<index>/{forecast_daily,baseline_predictions,baseline_exceptions,rf_exceptions,eligibility}`.
RF rows retain FORECAST_DAILY v1 and predicted-unit-sales provenance; baseline
evidence records observed-history provenance and its exact source date. Coverage
reconciles candidates, preparation eligibility, method predictions, labels, pairs,
drops, full/partial series and prediction differences. All unpaired method
predictions and attributable exclusions remain available for audit.

The `evaluation_manifest.json` completion marker uses EVALUATION_MANIFEST v1;
execution identity belongs to the binding and publication request. Parquet writes
use `errorifexists` and are independently reread/validated before staging a request
under `staging/pipeline_run_id=<id>/candidates/evaluation`. The bundle checksum is
SHA-256 of canonical JSON `{files: sorted relative paths, sizes, HDFS checksums}`
plus newline, excluding `evaluation_manifest.json` and `evidence/publication.json`
to avoid self-reference; the request binds their contents separately. Inventories
are bounded to 4,096 entries and the declared layout. Owner-only private claim,
seal and exclusive rename publish the complete bundle. Recovery checks exact
arguments and fresh integrity without rescoring; verified published reuse performs
no writes. Real-data execution and distributed acceptance belong to opt-in T04.

## Application Read Contract

J08 is the publication approval authority. The HDFS pointer
`/data/warehouse/manifests/published/current.json` is authoritative. The application-side
`artifacts/approved_publication.json` is an exact read-only materialized copy with the same
`publication_id` and manifest checksum. It is not an independent selector.

An approved context contains `publication_id`, manifest checksum, `source_snapshot_id`, `model_run_id`, `forecast_run_id`, `scenario_id`, `decision_run_id`, optional `evaluation_run_id` and `holdout_id`, `issue_date`, approved policy identifiers, and measured coverage. Every repository operation receives that context and must not independently select a latest run ID.

| Read operation | Bounded inputs | Result contract |
|---|---|---|
| `get_store_trend` | Store and bounded date range | Ordered observed daily units; valid empty result is allowed |
| `get_forecast_series` | Item and store | Exactly seven valid dates or explicit incomplete/unavailable result |
| `get_forecast_metrics` | Context-compatible evaluation | Same model/method/cohort metrics; no fabricated score |
| `get_replenishment` | Item and store | One matching recommendation or explicit missing result |
| `get_central_supply` | Item | One product-level simulated supply row or explicit missing result |
| `get_allocation` | Item and context-approved policy | All eligible store rows for that policy |
| `get_policy_evaluation` | Item and approved policies | Historical outcomes only when a compatible evaluation exists |

Invalid publication metadata or unavailable shards is an error state. A valid filter with no rows is an empty-result state. Missing values are not replaced with zero. All application reads remain bounded and read-only.

## Discrepancy Decision Record

The implementation follows the canonical choices written in the reviewed plan. The cataloged Contract v1 decisions below are approved for FND-06 implementation. Calendar and weekly-price lookup contracts remain outside this catalog and are not approved by this record.

| Issue | Implementation record | Approval status |
|---|---|---|
| CONTRACT-001 | Keep `source_version` for acquired raw identity and `source_snapshot_id` for validated curated identity; manifests record the parent relationship. | Approved for FND-06 implementation |
| CONTRACT-002 | Keep canonical `date` and `unit_sales`; map serving `sale_date` to `date` and retain `units_sold` only as a query alias. | Implemented per plan |
| CONTRACT-003 | Use `forecast_run_id` in the forecast uniqueness key; retain `model_run_id` as model lineage. | Implemented per plan |
| CONTRACT-004 | Keep the complete logical forecast contract in Parquet; retain application/lineage fields in serving; record `raw_prediction` serving retention as pending. | Approved for FND-06 implementation |
| CONTRACT-005 | Keep logical names `replenishment_recommendations` and `allocation_recommendations`; map serving names to `replenishment` and `allocation`. | Implemented per plan |
| CONTRACT-006 | Use `forecast_metrics` for scored forecast errors; keep model-training metadata in `model_manifest`; keep policy outcomes separate. | Approved for FND-06 implementation |
| CONTRACT-007 | Use forecast run types `validation`, `test`, and `forward_demo`; use only `validation`/`test` for scored policy outcomes. | Approved for FND-06 implementation |
| CONTRACT-008 | Use `decision_run_id` for decision batches and retain forecast/model/scenario/policy lineage. | Implemented per plan |
| CONTRACT-009 | Include `issue_date` in scenario uniqueness keys. | Implemented per plan |
| CONTRACT-010 | Treat HDFS `published/current.json` as authoritative and the application file as an exact copy. | Approved for FND-06 implementation |
| CONTRACT-011 | Use `pipeline_run_id`, `source_snapshot_id`, `feature_version`, `model_run_id`, `forecast_run_id`, `decision_run_id`, `evaluation_run_id`, and `publication_id`; do not introduce the rejected path aliases. | Approved for FND-06 implementation |
| CONTRACT-012 | Bind application reads to one approved publication context rather than independent latest-ID parameters. | Implemented per plan |
| CONTRACT-013 | Represent `policy_id` and `policy_version` as separate identities. | Approved for FND-06 implementation |
| CONTRACT-014 | Carry `evaluation_run_id` on independently rerunnable evaluation artifacts. | Approved for FND-06 implementation |
| CONTRACT-015 | Keep `source_manifest.downloaded_at_utc` present but nullable; validate non-null values as `TimestampUTC` and preserve explicit timestamps. | Implemented as a compatible Contract v1 clarification |

## Owning Documents

- [`03_SYSTEM_ARCHITECTURE.md`](03_SYSTEM_ARCHITECTURE.md): system boundaries and stage ownership.
- [`04_DATASET.md`](04_DATASET.md): M5 source semantics, canonical fields, provenance, and simulation boundary.
- [`06_DATA_PIPELINE.md`](06_DATA_PIPELINE.md): J01-J08 boundaries, HDFS paths, quality gates, manifests, and publication.
- [`07_MACHINE_LEARNING.md`](07_MACHINE_LEARNING.md): feature timing, model identity, forecast horizon, run types, and forecast metrics.
- [`08_BUSINESS_RULES_AND_OPTIMIZATION.md`](08_BUSINESS_RULES_AND_OPTIMIZATION.md): scenario, decision, policy, and historical evaluation semantics.
- [`09_DATABASE_DESIGN.md`](09_DATABASE_DESIGN.md): ClickHouse serving names, mappings, lineage, and publication gate.
- [`10_APPLICATION_DESIGN.md`](10_APPLICATION_DESIGN.md): approved publication selection and read-only application behavior.
- [`11_TESTING.md`](11_TESTING.md): later behavioral, fixture, integration, and performance verification; FND-06 owns deterministic fixtures.
