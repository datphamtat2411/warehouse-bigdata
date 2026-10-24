"""Contract tests for approved publication metadata and coherent context."""

import hashlib
import json
from copy import deepcopy

import pytest

from tests.fixtures.loader import load_manifest, load_publication_fixture
from warehouse_bigdata.contracts.validation import (
    ContractValidationError,
    validate_publication,
)


def _assert_code(error: ContractValidationError, code: str) -> None:
    assert any(issue.code == code for issue in error.issues), str(error)


def _valid_publication_inputs() -> tuple[dict, dict, dict]:
    fixture = load_publication_fixture()
    return (
        load_manifest("publication_manifest"),
        fixture["publication_context"],
        fixture["publication_copy"],
    )


def test_approved_publication_preserves_one_coherent_context() -> None:
    record, context, application_copy = _valid_publication_inputs()

    validate_publication(record, context=context, application_copy=application_copy)


def test_publication_requires_approved_status() -> None:
    record, context, application_copy = _valid_publication_inputs()
    record = deepcopy(record)
    record["status"] = "VALIDATED"

    with pytest.raises(ContractValidationError) as error:
        validate_publication(record, context=context, application_copy=application_copy)
    _assert_code(error.value, "INVALID_STATUS")

    record["status"] = "APPROVED"
    record["approved_status"] = "VALIDATED"
    with pytest.raises(ContractValidationError) as metadata_error:
        validate_publication(record, context=context, application_copy=application_copy)
    _assert_code(metadata_error.value, "INVALID_STATUS")


def test_publication_rejects_incompatible_identity_or_checksum() -> None:
    record, context, application_copy = _valid_publication_inputs()

    mismatch_record = deepcopy(record)
    mismatch_record["forecast_run_id"] = "synthetic-forecast-other"
    with pytest.raises(ContractValidationError) as lineage_error:
        validate_publication(
            mismatch_record,
            context=context,
            application_copy=application_copy,
        )
    _assert_code(lineage_error.value, "LINEAGE_MISMATCH")

    mismatch_copy = deepcopy(application_copy)
    mismatch_copy["manifest_checksum"] = "sha256:other-publication"
    with pytest.raises(ContractValidationError) as checksum_error:
        validate_publication(record, context=context, application_copy=mismatch_copy)
    _assert_code(checksum_error.value, "PUBLICATION_MISMATCH")


def test_publication_requires_selected_metadata_and_matching_versions() -> None:
    record, context, application_copy = _valid_publication_inputs()
    missing = deepcopy(record)
    del missing["selected_dataset_manifests"]

    with pytest.raises(ContractValidationError) as missing_error:
        validate_publication(missing, context=context, application_copy=application_copy)
    _assert_code(missing_error.value, "MISSING_PUBLICATION_METADATA")

    incompatible = deepcopy(record)
    selected = json.loads(incompatible["selected_dataset_manifests"])
    selected["forecast_daily"] = "2.0"
    incompatible["selected_dataset_manifests"] = json.dumps(selected, sort_keys=True)
    with pytest.raises(ContractValidationError) as version_error:
        validate_publication(incompatible, context=context, application_copy=application_copy)
    _assert_code(version_error.value, "VERSION_MISMATCH")


def test_publication_context_is_required_and_has_no_latest_id_fallback() -> None:
    record, _, application_copy = _valid_publication_inputs()

    with pytest.raises(ContractValidationError) as error:
        validate_publication(record, application_copy=application_copy)
    _assert_code(error.value, "MISSING_PUBLICATION_METADATA")


def test_explicit_dataset_bindings_must_match_application_copy_and_context():
    record, context, application_copy = _valid_publication_inputs()
    bindings = {
        "forecast_metrics": {"evaluation_run_id": "historical-forecast-evaluation"},
        "inventory_policy_evaluation": {"evaluation_run_id": "historical-policy-evaluation"},
    }
    record["dataset_bindings"] = bindings
    context["dataset_bindings"] = deepcopy(bindings)
    application_copy["dataset_bindings"] = deepcopy(bindings)
    validate_publication(record, context=context, application_copy=application_copy)
    application_copy["dataset_bindings"]["forecast_metrics"]["evaluation_run_id"] = "forward-demo"
    with pytest.raises(ContractValidationError) as error:
        validate_publication(record, context=context, application_copy=application_copy)
    _assert_code(error.value, "PUBLICATION_MISMATCH")


def test_application_checksum_binds_the_exact_materialized_copy():
    record, context, application_copy = _valid_publication_inputs()
    record["application_checksum_definition"] = "sha256-canonical-application-copy-v1"
    payload = (json.dumps(application_copy, sort_keys=True, separators=(",", ":")) + "\n").encode()
    record["application_manifest_checksum"] = "sha256:" + hashlib.sha256(payload).hexdigest()
    validate_publication(record, context=context, application_copy=application_copy)
    application_copy["unapproved_extra_field"] = "changed"
    with pytest.raises(ContractValidationError) as error:
        validate_publication(record, context=context, application_copy=application_copy)
    _assert_code(error.value, "PUBLICATION_MISMATCH")


def test_evaluation_query_binding_never_falls_back_to_primary_context():
    from warehouse_bigdata.contracts.manifests import PublicationContext

    _, context, _ = _valid_publication_inputs()
    publication = PublicationContext(**context)
    with pytest.raises(ValueError, match="unavailable"):
        publication.evaluation_binding("forecast_metrics")
    historical = {
        "evaluation_run_id": "historical",
        "run_type": "test",
        "forecast_run_id": "historical-forecast",
    }
    publication = PublicationContext(**context, dataset_bindings={"forecast_metrics": historical})
    assert publication.evaluation_binding("forecast_metrics") == historical


def test_read_context_rejects_missing_approval(tmp_path):
    from warehouse_bigdata.application.read_repository import load_approved_publication_context

    with pytest.raises((OSError, ValueError)):
        load_approved_publication_context(tmp_path / "missing.json")
