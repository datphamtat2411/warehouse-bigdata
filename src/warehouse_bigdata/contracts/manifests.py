"""Run, manifest, artifact-location, and application publication contracts."""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from types import MappingProxyType
from typing import Final

from .datasets import FieldSpec

COMPLETED: Final = "COMPLETED"
VALIDATED: Final = "VALIDATED"
FAILED: Final = "FAILED"
APPROVED: Final = "APPROVED"

COMPUTATION_STATUSES: Final = ("PLANNED", "RUNNING", COMPLETED, VALIDATED, FAILED)
PUBLICATION_STATUSES: Final = (*COMPUTATION_STATUSES, APPROVED)

FORECAST_RUN_TYPES: Final = ("validation", "test", "forward_demo")
POLICY_EVALUATION_RUN_TYPES: Final = ("validation", "test")


@dataclass(frozen=True, slots=True)
class RunIdentity:
    """Meaning, owner, and propagation rule for one cross-module identifier."""

    name: str
    purpose: str
    owner: str
    propagation: str


RUN_IDENTITIES: Final = (
    RunIdentity(
        "source_version",
        "Immutable identity of one acquired raw source package.",
        "J01",
        "Raw HDFS path and source manifest; parent of curated lineage.",
    ),
    RunIdentity(
        "source_snapshot_id",
        "Immutable identity of one validated curated source derivation.",
        "J02/J03 quality gates",
        "Downstream rows, manifests, paths, and publication context.",
    ),
    RunIdentity(
        "pipeline_run_id",
        "Identity of one ingestion or transformation execution attempt.",
        "J01-J08 stage owner",
        "Run manifests, staging paths, and quality summaries.",
    ),
    RunIdentity(
        "feature_version",
        "Public identity of a feature schema and as-of transformation contract.",
        "J04",
        "Feature rows, model manifests, and forecast rows.",
    ),
    RunIdentity(
        "model_run_id",
        "Immutable identity of one trained model artifact and parameter set.",
        "J05",
        "Model, forecast, decision, and publication lineage.",
    ),
    RunIdentity(
        "forecast_run_id",
        "Identity of one scoring batch for a model, issue date, scope, and configuration.",
        "J06",
        "Forecast row key, decision lineage, and publication context.",
    ),
    RunIdentity(
        "scenario_id",
        "Immutable identity of one simulated input scenario.",
        "J07",
        "Scenario rows, decisions, and publication context.",
    ),
    RunIdentity(
        "decision_run_id",
        "Identity of one decision batch bound to one forecast, scenario, policy, and issue date.",
        "J07",
        "Replenishment/allocation rows and publication context.",
    ),
    RunIdentity(
        "evaluation_run_id",
        "Identity of one independently rerunnable evaluation artifact or bundle.",
        "J05/J07",
        "Forecast metrics, policy outcomes, and evaluation manifests.",
    ),
    RunIdentity(
        "publication_id",
        "Identity of one candidate or approved serving release.",
        "J08",
        "Publication manifest, approved pointer, and application copy.",
    ),
    RunIdentity(
        "policy_id",
        "Stable identity of a policy family.",
        "J07 policy owner",
        "Allocation and policy-evaluation rows.",
    ),
    RunIdentity(
        "policy_version",
        "Immutable implementation or configuration revision of a policy family.",
        "J07 policy owner",
        "Decision rows, evaluations, and manifests.",
    ),
)

RUN_IDENTITIES_BY_NAME: Mapping[str, RunIdentity] = MappingProxyType(
    {identity.name: identity for identity in RUN_IDENTITIES}
)


COMMON_MANIFEST_FIELDS: Final = (
    "manifest_schema_version",
    "contract_name",
    "contract_version",
    "manifest_kind",
    "status",
    "producer",
    "artifact_uri",
    "artifact_checksum",
    "schema_fingerprint",
    "grain",
    "logical_unique_key",
    "row_count",
    "required_field_null_counts",
    "code_revision",
    "configuration_digest",
    "created_at",
    "completed_at",
    "coverage_and_exclusion_counts",
)


@dataclass(frozen=True, slots=True)
class ManifestContract:
    """Required metadata and lifecycle for one manifest category."""

    name: str
    version: str
    manifest_kind: str
    required_fields: tuple[str, ...]
    field_specs: tuple[FieldSpec, ...]
    conditional_identifiers: tuple[str, ...]
    status_values: tuple[str, ...]
    description: str

    @property
    def field_names(self) -> tuple[str, ...]:
        """Return manifest fields in the declared order."""

        return tuple(field.name for field in self.field_specs)


_MANIFEST_FIELD_TYPES: Mapping[str, str] = MappingProxyType(
    {
        "manifest_schema_version": "String",
        "contract_name": "String",
        "contract_version": "String",
        "manifest_kind": "String",
        "status": "String",
        "producer": "String",
        "artifact_uri": "String",
        "artifact_checksum": "String",
        "schema_fingerprint": "String",
        "grain": "String",
        "logical_unique_key": "String",
        "row_count": "NonNegativeInt",
        "required_field_null_counts": "String",
        "code_revision": "String",
        "configuration_digest": "String",
        "created_at": "TimestampUTC",
        "completed_at": "TimestampUTC",
        "coverage_and_exclusion_counts": "String",
        "source_version": "String",
        "source_name": "String",
        "source_url": "String",
        "downloaded_at_utc": "TimestampUTC",
        "source_file": "String",
        "size_bytes": "NonNegativeInt",
        "source_schema_version": "String",
        "source_file_row_count": "NonNegativeInt",
        "pipeline_run_id": "String",
        "output_dataset_names": "String",
        "output_dataset_uris": "String",
        "quality_report_uri": "String",
        "source_snapshot_id": "String",
        "model_run_id": "String",
        "feature_version": "String",
        "algorithm": "String",
        "training_start": "Date",
        "training_end": "Date",
        "validation_start": "Date",
        "validation_end": "Date",
        "test_start": "Date",
        "test_end": "Date",
        "forecast_strategy": "String",
        "model_parameters": "String",
        "seed": "Integer",
        "training_row_count": "NonNegativeInt",
        "training_series_count": "NonNegativeInt",
        "model_artifact_uri": "String",
        "model_artifact_checksum": "String",
        "model_artifact_schema_fingerprint": "String",
        "evaluation_run_id": "String",
        "forecast_run_id": "String",
        "issue_date": "Date",
        "horizon_days": "NonNegativeInt",
        "run_type": "String",
        "forecast_artifact_uri": "String",
        "forecast_artifact_checksum": "String",
        "scenario_id": "String",
        "scenario_version": "String",
        "scenario_type": "String",
        "scenario_seed": "Integer",
        "scenario_parameters": "String",
        "parameter_digest": "String",
        "scenario_generator_version": "String",
        "origin_label": "String",
        "decision_run_id": "String",
        "policy_id": "String",
        "policy_version": "String",
        "decision_artifact_uris": "String",
        "decision_artifact_checksums": "String",
        "holdout_id": "String",
        "evaluation_start": "Date",
        "evaluation_end": "Date",
        "evaluation_artifact_uris": "String",
        "evaluation_artifact_checksums": "String",
        "publication_id": "String",
        "selected_dataset_manifests": "String",
        "expected_rows": "String",
        "loaded_rows_per_shard": "String",
        "representative_measure_reconciliation": "String",
        "serving_schema_versions": "String",
        "approved_status": "String",
        "publication_timestamp": "TimestampUTC",
        "application_manifest_checksum": "String",
        "materialization_reference": "String",
    }
)

_MANIFEST_NULLABLE_FIELDS: Final = frozenset(
    {
        "artifact_checksum",
        "schema_fingerprint",
        "completed_at",
        "scenario_seed",
        "downloaded_at_utc",
    }
)


def _manifest(
    name: str,
    manifest_kind: str,
    specific_fields: tuple[str, ...],
    *,
    conditional_identifiers: tuple[str, ...] = (),
    status_values: tuple[str, ...] = COMPUTATION_STATUSES,
    description: str,
) -> ManifestContract:
    required_fields = COMMON_MANIFEST_FIELDS + specific_fields
    return ManifestContract(
        name=name,
        version="1.0",
        manifest_kind=manifest_kind,
        required_fields=required_fields,
        field_specs=tuple(
            FieldSpec(
                field_name,
                _MANIFEST_FIELD_TYPES[field_name],
                nullable=field_name in _MANIFEST_NULLABLE_FIELDS,
            )
            for field_name in required_fields
        ),
        conditional_identifiers=conditional_identifiers,
        status_values=status_values,
        description=description,
    )


SOURCE_MANIFEST = _manifest(
    "source_manifest",
    "source_acquisition",
    (
        "source_version",
        "source_name",
        "source_url",
        "downloaded_at_utc",
        "source_file",
        "size_bytes",
        "source_schema_version",
        "source_file_row_count",
    ),
    description="Acquisition metadata for immutable raw M5 files.",
)

PIPELINE_RUN_MANIFEST = _manifest(
    "pipeline_run_manifest",
    "pipeline_output",
    (
        "pipeline_run_id",
        "source_version",
        "output_dataset_names",
        "output_dataset_uris",
        "quality_report_uri",
    ),
    conditional_identifiers=("source_snapshot_id",),
    description="Execution attempt and validated pipeline output metadata.",
)

MODEL_MANIFEST = _manifest(
    "model_manifest",
    "model_run",
    (
        "model_run_id",
        "source_snapshot_id",
        "feature_version",
        "algorithm",
        "training_start",
        "training_end",
        "validation_start",
        "validation_end",
        "test_start",
        "test_end",
        "forecast_strategy",
        "model_parameters",
        "seed",
        "training_row_count",
        "training_series_count",
        "model_artifact_uri",
        "model_artifact_checksum",
        "model_artifact_schema_fingerprint",
    ),
    conditional_identifiers=("pipeline_run_id", "evaluation_run_id"),
    description="Integrity and reproducibility metadata for one trained model artifact.",
)

FORECAST_MANIFEST = _manifest(
    "forecast_manifest",
    "forecast_run",
    (
        "forecast_run_id",
        "model_run_id",
        "source_snapshot_id",
        "feature_version",
        "issue_date",
        "horizon_days",
        "run_type",
        "forecast_artifact_uri",
        "forecast_artifact_checksum",
    ),
    conditional_identifiers=("evaluation_run_id",),
    description="Scoring batch metadata and seven-day forecast coverage.",
)

SCENARIO_MANIFEST = _manifest(
    "scenario_manifest",
    "scenario",
    (
        "scenario_id",
        "scenario_version",
        "source_snapshot_id",
        "issue_date",
        "scenario_type",
        "scenario_seed",
        "scenario_parameters",
        "parameter_digest",
        "scenario_generator_version",
        "origin_label",
    ),
    description="Reproducibility metadata for simulated stock and central supply inputs.",
)

DECISION_MANIFEST = _manifest(
    "decision_manifest",
    "decision_run",
    (
        "decision_run_id",
        "source_snapshot_id",
        "forecast_run_id",
        "model_run_id",
        "scenario_id",
        "issue_date",
        "policy_id",
        "policy_version",
        "decision_artifact_uris",
        "decision_artifact_checksums",
    ),
    description="Lineage and readiness metadata for replenishment and allocation outputs.",
)

EVALUATION_MANIFEST = _manifest(
    "evaluation_manifest",
    "evaluation_run",
    (
        "evaluation_run_id",
        "source_snapshot_id",
        "holdout_id",
        "run_type",
        "evaluation_start",
        "evaluation_end",
        "evaluation_artifact_uris",
        "evaluation_artifact_checksums",
    ),
    conditional_identifiers=(
        "model_run_id",
        "forecast_run_id",
        "scenario_id",
        "decision_run_id",
        "policy_id",
        "policy_version",
    ),
    description="Metadata for one independently rerunnable forecast or policy evaluation artifact.",
)

PUBLICATION_MANIFEST = _manifest(
    "publication_manifest",
    "publication",
    (
        "publication_id",
        "selected_dataset_manifests",
        "source_snapshot_id",
        "expected_rows",
        "loaded_rows_per_shard",
        "representative_measure_reconciliation",
        "serving_schema_versions",
        "approved_status",
        "publication_timestamp",
        "application_manifest_checksum",
        "materialization_reference",
    ),
    conditional_identifiers=(
        "model_run_id",
        "forecast_run_id",
        "scenario_id",
        "decision_run_id",
        "evaluation_run_id",
        "policy_id",
        "policy_version",
    ),
    status_values=PUBLICATION_STATUSES,
    description="Validated serving release and authoritative application-selection metadata.",
)

MANIFEST_CONTRACTS: Final = (
    SOURCE_MANIFEST,
    PIPELINE_RUN_MANIFEST,
    MODEL_MANIFEST,
    FORECAST_MANIFEST,
    SCENARIO_MANIFEST,
    DECISION_MANIFEST,
    EVALUATION_MANIFEST,
    PUBLICATION_MANIFEST,
)

MANIFEST_CONTRACTS_BY_NAME: Mapping[str, ManifestContract] = MappingProxyType(
    {manifest.name: manifest for manifest in MANIFEST_CONTRACTS}
)


@dataclass(frozen=True, slots=True)
class ArtifactPathContract:
    """Logical HDFS location convention; no directory is created by this module."""

    name: str
    template: str
    identity_fields: tuple[str, ...]
    content_kind: str
    owner: str
    description: str


HDFS_ARTIFACT_PATHS: Final = (
    ArtifactPathContract(
        "raw_m5",
        "/data/warehouse/raw/m5/source_version={source_version}/",
        ("source_version",),
        "data",
        "J01",
        "Immutable acquired source files and source manifest.",
    ),
    ArtifactPathContract(
        "staging",
        "/data/warehouse/staging/pipeline_run_id={pipeline_run_id}/",
        ("pipeline_run_id",),
        "data",
        "J01-J03",
        "Exclusive, retryable candidate data for one execution attempt.",
    ),
    ArtifactPathContract(
        "curated_sales_daily",
        "/data/warehouse/curated/sales_daily/source_snapshot_id={source_snapshot_id}/",
        ("source_snapshot_id",),
        "data",
        "J02",
        "Validated normalized observed sales Parquet.",
    ),
    ArtifactPathContract(
        "curated_sales_enriched",
        "/data/warehouse/curated/sales_enriched/source_snapshot_id={source_snapshot_id}/",
        ("source_snapshot_id",),
        "data",
        "J03",
        "Validated enriched observed sales Parquet.",
    ),
    ArtifactPathContract(
        "forecast_features",
        "/data/warehouse/features/demand_forecasting/source_snapshot_id={source_snapshot_id}/feature_version={feature_version}/",
        ("source_snapshot_id", "feature_version"),
        "data",
        "J04",
        "Leakage-controlled feature snapshot.",
    ),
    ArtifactPathContract(
        "model_artifact",
        "/data/warehouse/models/{model_run_id}/",
        ("model_run_id",),
        "data",
        "J05",
        "Immutable model artifact and model manifest.",
    ),
    ArtifactPathContract(
        "forecast_output",
        "/data/warehouse/outputs/forecasts/{forecast_run_id}/",
        ("forecast_run_id",),
        "data",
        "J06",
        "Forecast Parquet and coverage exceptions.",
    ),
    ArtifactPathContract(
        "decision_output",
        "/data/warehouse/outputs/decisions/{decision_run_id}/",
        ("decision_run_id",),
        "data",
        "J07",
        "Replenishment and allocation Parquet.",
    ),
    ArtifactPathContract(
        "evaluation_output",
        "/data/warehouse/outputs/evaluation/{evaluation_run_id}/",
        ("evaluation_run_id",),
        "data",
        "J05/J07",
        "Forecast and policy evaluation artifacts.",
    ),
    ArtifactPathContract(
        "run_manifest",
        "/data/warehouse/manifests/runs/{pipeline_run_id}.json",
        ("pipeline_run_id",),
        "metadata",
        "J01-J08",
        "Execution and quality metadata, not a data directory.",
    ),
    ArtifactPathContract(
        "publication_manifest",
        "/data/warehouse/manifests/publications/{publication_id}.json",
        ("publication_id",),
        "metadata",
        "J08",
        "Immutable candidate or approved publication metadata.",
    ),
    ArtifactPathContract(
        "approved_publication_pointer",
        "/data/warehouse/manifests/published/current.json",
        ("publication_id",),
        "approval_pointer",
        "J08",
        "Authoritative pointer to the approved publication manifest.",
    ),
)

AUTHORITATIVE_PUBLICATION_POINTER: Final = "/data/warehouse/manifests/published/current.json"
APPROVED_PUBLICATION_MANIFEST_PATH: Final = "artifacts/approved_publication.json"


@dataclass(frozen=True, slots=True)
class PublicationCoverage:
    """Measured serving coverage carried by an approved publication context."""

    series_total: int
    series_eligible: int
    series_scored: int
    loaded_rows: int
    available_stores: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PublicationContext:
    """One coherent, approved bundle consumed by read-only application code."""

    contract_version: str
    publication_id: str
    manifest_checksum: str
    source_snapshot_id: str
    model_run_id: str
    forecast_run_id: str
    scenario_id: str
    decision_run_id: str
    evaluation_run_id: str | None
    holdout_id: str | None
    issue_date: date
    policy_ids: tuple[str, ...]
    coverage: PublicationCoverage


PUBLICATION_CONTEXT_FIELDS: Final = (
    "publication_id",
    "manifest_checksum",
    "source_snapshot_id",
    "model_run_id",
    "forecast_run_id",
    "scenario_id",
    "decision_run_id",
    "evaluation_run_id",
    "holdout_id",
    "issue_date",
    "policy_ids",
    "coverage",
)


@dataclass(frozen=True, slots=True)
class ApplicationReadContract:
    """Logical read operation bound to one publication context."""

    name: str
    required_context_fields: tuple[str, ...]
    additional_inputs: tuple[str, ...]
    result_contract: str
    missing_data_behavior: str
    requires_historical_evaluation: bool = False


APPLICATION_READ_CONTRACTS: Final = (
    ApplicationReadContract(
        "get_store_trend",
        PUBLICATION_CONTEXT_FIELDS,
        ("store_id", "start_date", "end_date"),
        "Ordered observed daily units for a bounded date range.",
        "A valid empty filter result is distinct from an invalid publication or unavailable shard.",
    ),
    ApplicationReadContract(
        "get_forecast_series",
        PUBLICATION_CONTEXT_FIELDS,
        ("item_id", "store_id"),
        "Exactly seven valid forecast dates or an explicit incomplete/unavailable result.",
        "Missing or incomplete forecast coverage is reported, never replaced with zero.",
    ),
    ApplicationReadContract(
        "get_forecast_metrics",
        PUBLICATION_CONTEXT_FIELDS,
        (),
        "Forecast metrics for the context's compatible holdout and method cohort.",
        "Missing evaluation context is an unavailable score, never a fabricated metric.",
        requires_historical_evaluation=True,
    ),
    ApplicationReadContract(
        "get_replenishment",
        PUBLICATION_CONTEXT_FIELDS,
        ("item_id", "store_id"),
        "One matching recommendation or an explicit missing result.",
        "Missing decision coverage is reported without selecting another decision run.",
    ),
    ApplicationReadContract(
        "get_central_supply",
        PUBLICATION_CONTEXT_FIELDS,
        ("item_id",),
        "One product-level simulated supply row or an explicit missing result.",
        "Missing supply is reported; repeated store rows are not summed as supply.",
    ),
    ApplicationReadContract(
        "get_allocation",
        PUBLICATION_CONTEXT_FIELDS,
        ("item_id", "policy_id"),
        "All eligible store allocation rows for a context-approved policy.",
        "An unapproved policy or incomplete store population is an explicit error state.",
    ),
    ApplicationReadContract(
        "get_policy_evaluation",
        PUBLICATION_CONTEXT_FIELDS,
        ("item_id",),
        "Historical policy outcomes only when a compatible evaluation exists.",
        "Forward-demo or missing labels produce an unavailable evaluation state.",
        requires_historical_evaluation=True,
    ),
)


PUBLICATION_SELECTION_RULES: Final = (
    "The HDFS published/current.json pointer selects the authoritative approved publication.",
    "artifacts/approved_publication.json is an exact read-only materialized copy with checksum and publication_id.",
    "The application never selects independent latest model, forecast, scenario, decision, or evaluation IDs.",
    "Invalid publication metadata and unavailable shards are explicit errors; valid empty filters are empty results.",
)


# Kept as a small type declaration for manifest consumers that need common metadata types.
COMMON_MANIFEST_FIELD_TYPES: Final = (
    FieldSpec("manifest_schema_version", "String"),
    FieldSpec("contract_name", "String"),
    FieldSpec("contract_version", "String"),
    FieldSpec("manifest_kind", "String"),
    FieldSpec("status", "String"),
    FieldSpec("producer", "String"),
    FieldSpec("artifact_uri", "String"),
    FieldSpec("artifact_checksum", "String"),
    FieldSpec("schema_fingerprint", "String"),
    FieldSpec("grain", "String"),
    FieldSpec("logical_unique_key", "String"),
    FieldSpec("row_count", "NonNegativeInt"),
    FieldSpec("required_field_null_counts", "String"),
    FieldSpec("code_revision", "String"),
    FieldSpec("configuration_digest", "String"),
    FieldSpec("created_at", "TimestampUTC"),
    FieldSpec("completed_at", "TimestampUTC", nullable=True),
    FieldSpec("coverage_and_exclusion_counts", "String"),
)
