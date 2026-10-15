"""Contract tests for run manifests and declared lineage."""

from copy import deepcopy

import pytest

from tests.fixtures.loader import load_context, load_manifest
from warehouse_bigdata.contracts.manifests import MANIFEST_CONTRACTS_BY_NAME
from warehouse_bigdata.contracts.validation import (
    ContractValidationError,
    validate_manifest,
)


def _assert_code(error: ContractValidationError, code: str) -> None:
    assert any(issue.code == code for issue in error.issues), str(error)


def test_all_cataloged_manifests_are_accepted() -> None:
    manifest_names = (
        "source_manifest",
        "pipeline_run_manifest",
        "model_manifest",
        "forecast_manifest",
        "scenario_manifest",
        "decision_manifest",
        "evaluation_manifest",
        "publication_manifest",
    )

    for manifest_name in manifest_names:
        record = load_manifest(manifest_name)
        validate_manifest(MANIFEST_CONTRACTS_BY_NAME[manifest_name], record)


def test_manifest_identity_and_status_match_catalog() -> None:
    manifest_names = (
        "source_manifest",
        "pipeline_run_manifest",
        "model_manifest",
        "forecast_manifest",
        "scenario_manifest",
        "decision_manifest",
        "evaluation_manifest",
        "publication_manifest",
    )

    for manifest_name in manifest_names:
        record = load_manifest(manifest_name)
        contract = MANIFEST_CONTRACTS_BY_NAME[manifest_name]
        assert record["contract_name"] == contract.name
        assert record["contract_version"] == contract.version
        assert record["manifest_kind"] == contract.manifest_kind
        assert record["status"] in contract.status_values


def test_missing_required_manifest_field_is_rejected() -> None:
    record = deepcopy(load_manifest("source_manifest"))
    del record["artifact_uri"]

    with pytest.raises(ContractValidationError) as error:
        validate_manifest(MANIFEST_CONTRACTS_BY_NAME["source_manifest"], record)
    _assert_code(error.value, "MISSING_FIELD")


def test_manifest_nullability_and_logical_types_are_enforced() -> None:
    source_manifest = load_manifest("source_manifest")
    contract = MANIFEST_CONTRACTS_BY_NAME["source_manifest"]

    nullable = deepcopy(source_manifest)
    nullable["artifact_checksum"] = None
    nullable["completed_at"] = None
    validate_manifest(contract, nullable)

    null_record = deepcopy(source_manifest)
    null_record["row_count"] = None
    with pytest.raises(ContractValidationError) as null_error:
        validate_manifest(contract, null_record)
    _assert_code(null_error.value, "NULL_NOT_ALLOWED")

    type_record = deepcopy(source_manifest)
    type_record["row_count"] = "8"
    with pytest.raises(ContractValidationError) as type_error:
        validate_manifest(contract, type_record)
    _assert_code(type_error.value, "TYPE_MISMATCH")


def test_invalid_manifest_status_and_unapproved_identifier_are_rejected() -> None:
    pipeline_manifest = load_manifest("pipeline_run_manifest")
    contract = MANIFEST_CONTRACTS_BY_NAME["pipeline_run_manifest"]

    status_record = deepcopy(pipeline_manifest)
    status_record["status"] = "UNKNOWN"
    with pytest.raises(ContractValidationError) as status_error:
        validate_manifest(contract, status_record)
    _assert_code(status_error.value, "INVALID_ENUM")

    alias_record = deepcopy(pipeline_manifest)
    alias_record["feature_run_id"] = "synthetic-feature-run-001"
    with pytest.raises(ContractValidationError) as alias_error:
        validate_manifest(contract, alias_record)
    _assert_code(alias_error.value, "UNEXPECTED_FIELD")


def test_completed_manifest_is_not_treated_as_validated() -> None:
    pipeline_manifest = load_manifest("pipeline_run_manifest")
    contract = MANIFEST_CONTRACTS_BY_NAME["pipeline_run_manifest"]
    record = deepcopy(pipeline_manifest)
    record["status"] = "COMPLETED"

    with pytest.raises(ContractValidationError) as error:
        validate_manifest(contract, record, lineage_context={"required_status": "VALIDATED"})
    _assert_code(error.value, "STATUS_MISMATCH")


def test_conditional_source_lineage_is_required_when_context_requests_it() -> None:
    context = load_context()
    pipeline_manifest = load_manifest("pipeline_run_manifest")
    contract = MANIFEST_CONTRACTS_BY_NAME["pipeline_run_manifest"]
    record = deepcopy(pipeline_manifest)
    del record["source_snapshot_id"]

    with pytest.raises(ContractValidationError) as missing_error:
        validate_manifest(
            contract,
            record,
            lineage_context={"source_snapshot_id": context["lineage"]["source_snapshot_id"]},
        )
    _assert_code(missing_error.value, "MISSING_LINEAGE")

    mismatch = deepcopy(pipeline_manifest)
    mismatch["source_snapshot_id"] = "synthetic-snapshot-other"
    with pytest.raises(ContractValidationError) as mismatch_error:
        validate_manifest(
            contract,
            mismatch,
            lineage_context={"source_snapshot_id": context["lineage"]["source_snapshot_id"]},
        )
    _assert_code(mismatch_error.value, "LINEAGE_MISMATCH")
