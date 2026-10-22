"""Focused consistency checks for the cross-module contract catalog."""

from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from warehouse_bigdata import contracts as root_contracts
from warehouse_bigdata.contracts import (
    APPLICATION_READ_CONTRACTS,
    CATEGORY_MONTHLY_SALES,
    DATASET_CONTRACTS,
    DATASET_CONTRACTS_BY_NAME,
    FIELD_MAPPINGS,
    HDFS_ARTIFACT_PATHS,
    MANIFEST_CONTRACTS,
    PUBLICATION_CONTEXT_FIELDS,
    RUN_IDENTITIES,
)
from warehouse_bigdata.contracts.datasets import LOGICAL_TYPES
from warehouse_bigdata.contracts.manifests import (
    APPROVED_PUBLICATION_MANIFEST_PATH,
    AUTHORITATIVE_PUBLICATION_POINTER,
    FORECAST_RUN_TYPES,
    POLICY_EVALUATION_RUN_TYPES,
)
from warehouse_bigdata.contracts.validation import (
    ContractValidationError,
    ValidationIssue,
    validate_dataset,
    validate_manifest,
    validate_publication,
    validate_record,
)


def test_dataset_names_versions_and_fields_are_unique() -> None:
    assert DATASET_CONTRACTS
    assert len({contract.name for contract in DATASET_CONTRACTS}) == len(DATASET_CONTRACTS)

    for contract in DATASET_CONTRACTS:
        assert contract.version.count(".") == 1
        assert contract.fields
        field_names = [field.name for field in contract.fields]
        assert len(field_names) == len(set(field_names))
        assert all(field.logical_type in LOGICAL_TYPES for field in contract.fields)
        assert set(contract.unique_key).issubset(field_names)


def test_category_monthly_sales_v1_is_an_explicit_snapshot_contract() -> None:
    assert CATEGORY_MONTHLY_SALES.name == "category_monthly_sales"
    assert CATEGORY_MONTHLY_SALES.version == "1.0"
    assert CATEGORY_MONTHLY_SALES.field_names == (
        "source_snapshot_id",
        "cat_id",
        "month_start",
        "total_units",
    )
    assert CATEGORY_MONTHLY_SALES.unique_key == (
        "source_snapshot_id",
        "cat_id",
        "month_start",
    )
    assert tuple(field.logical_type for field in CATEGORY_MONTHLY_SALES.fields) == (
        "String",
        "String",
        "Date",
        "NonNegativeInt",
    )
    assert CATEGORY_MONTHLY_SALES is DATASET_CONTRACTS_BY_NAME[CATEGORY_MONTHLY_SALES.name]


def test_validation_public_api_is_reexported_without_aliases() -> None:
    assert root_contracts.ContractValidationError is ContractValidationError
    assert root_contracts.ValidationIssue is ValidationIssue
    assert root_contracts.validate_dataset is validate_dataset
    assert root_contracts.validate_manifest is validate_manifest
    assert root_contracts.validate_publication is validate_publication
    assert root_contracts.validate_record is validate_record


def test_required_lineage_has_an_explicit_authority() -> None:
    for contract in DATASET_CONTRACTS:
        declared_names = {field.name for field in contract.fields}
        authority_names = {item.name for item in contract.lineage_authority}
        assert set(contract.required_lineage).issubset(declared_names | authority_names)
        assert all(item.location for item in contract.lineage_authority)


def test_contract_objects_are_immutable() -> None:
    with pytest.raises(FrozenInstanceError):
        DATASET_CONTRACTS[0].name = "changed"  # type: ignore[misc]


def test_manifest_requirements_and_lifecycle_are_consistent() -> None:
    assert MANIFEST_CONTRACTS
    common_fields = {
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
    }

    for manifest in MANIFEST_CONTRACTS:
        assert manifest.version.count(".") == 1
        assert len(manifest.required_fields) == len(set(manifest.required_fields))
        assert manifest.field_names == manifest.required_fields
        assert all(field.logical_type in LOGICAL_TYPES for field in manifest.field_specs)
        assert common_fields.issubset(manifest.required_fields)
        assert set(manifest.conditional_identifiers).issubset(
            {identity.name for identity in RUN_IDENTITIES}
        )
        assert {"COMPLETED", "VALIDATED", "FAILED"}.issubset(manifest.status_values)

    publication = next(
        manifest for manifest in MANIFEST_CONTRACTS if manifest.name == "publication_manifest"
    )
    assert "APPROVED" in publication.status_values
    assert {
        "publication_id",
        "selected_dataset_manifests",
        "approved_status",
        "application_manifest_checksum",
        "materialization_reference",
    }.issubset(publication.required_fields)


def test_run_identity_catalog_excludes_unapproved_aliases() -> None:
    names = {identity.name for identity in RUN_IDENTITIES}
    assert {
        "source_version",
        "source_snapshot_id",
        "pipeline_run_id",
        "feature_version",
        "model_run_id",
        "forecast_run_id",
        "scenario_id",
        "decision_run_id",
        "evaluation_run_id",
        "publication_id",
        "policy_id",
        "policy_version",
    }.issubset(names)
    assert "feature_run_id" not in names
    assert "scenario_run_id" not in names
    assert "export_run_id" not in names


def test_forecast_and_policy_run_types_remain_distinct() -> None:
    assert FORECAST_RUN_TYPES == ("validation", "test", "forward_demo")
    assert POLICY_EVALUATION_RUN_TYPES == ("validation", "test")
    assert "historical_backtest" not in FORECAST_RUN_TYPES
    assert "historical_backtest" not in POLICY_EVALUATION_RUN_TYPES


def test_field_mappings_are_declared_and_unambiguous() -> None:
    mapping_keys = [(mapping.dataset_name, mapping.logical_field) for mapping in FIELD_MAPPINGS]
    assert len(mapping_keys) == len(set(mapping_keys))

    for mapping in FIELD_MAPPINGS:
        contract = DATASET_CONTRACTS_BY_NAME[mapping.dataset_name]
        assert mapping.logical_field in {field.name for field in contract.fields}
        assert mapping.parquet_field
        assert mapping.conversion
        assert mapping.retention_location

    sales_date = next(
        mapping
        for mapping in FIELD_MAPPINGS
        if mapping.dataset_name == "sales_daily" and mapping.logical_field == "date"
    )
    assert sales_date.serving_field == "sale_date"

    unit_sales = next(
        mapping
        for mapping in FIELD_MAPPINGS
        if mapping.dataset_name == "sales_daily" and mapping.logical_field == "unit_sales"
    )
    assert unit_sales.serving_field == "unit_sales"
    assert "units_sold" in unit_sales.query_aliases

    raw_prediction = next(
        mapping
        for mapping in FIELD_MAPPINGS
        if mapping.dataset_name == "forecast_daily" and mapping.logical_field == "raw_prediction"
    )
    assert raw_prediction.approval_required
    assert raw_prediction.serving_field is None


def test_hdfs_paths_use_canonical_identity_dimensions() -> None:
    assert HDFS_ARTIFACT_PATHS
    for artifact in HDFS_ARTIFACT_PATHS:
        assert artifact.template.startswith("/data/warehouse/")
        assert artifact.identity_fields
        assert artifact.content_kind in {"data", "metadata", "approval_pointer"}
        assert "feature_run_id" not in artifact.template
        assert "scenario_run_id" not in artifact.template
        assert "export_run_id" not in artifact.template

    assert any("source_version={source_version}" in item.template for item in HDFS_ARTIFACT_PATHS)
    assert any("feature_version={feature_version}" in item.template for item in HDFS_ARTIFACT_PATHS)
    assert any("{publication_id}.json" in item.template for item in HDFS_ARTIFACT_PATHS)


def test_application_reads_require_one_publication_context() -> None:
    assert {
        "publication_id",
        "manifest_checksum",
        "source_snapshot_id",
        "model_run_id",
        "forecast_run_id",
        "scenario_id",
        "decision_run_id",
        "issue_date",
    }.issubset(PUBLICATION_CONTEXT_FIELDS)

    for operation in APPLICATION_READ_CONTRACTS:
        assert set(operation.required_context_fields) == set(PUBLICATION_CONTEXT_FIELDS)
        assert operation.result_contract
        assert operation.missing_data_behavior


def test_publication_authority_and_document_traceability() -> None:
    assert AUTHORITATIVE_PUBLICATION_POINTER.endswith("manifests/published/current.json")
    assert APPROVED_PUBLICATION_MANIFEST_PATH == "artifacts/approved_publication.json"

    documentation = Path("docs/CONTRACTS.md").read_text(encoding="utf-8")
    for contract in DATASET_CONTRACTS:
        assert f"`{contract.name}`" in documentation
    for mapping in FIELD_MAPPINGS:
        assert mapping.dataset_name in documentation
