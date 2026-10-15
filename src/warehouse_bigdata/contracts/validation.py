"""Stable public facade for deterministic catalog-contract validation."""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from ._validation.common import ContractValidationError, ValidationIssue, _raise_if_invalid
from ._validation.datasets import validate_dataset as _validate_dataset
from ._validation.datasets import validate_record as _validate_record
from ._validation.manifests import manifest_issues
from ._validation.publication import publication_issues
from .datasets import DatasetContract
from .manifests import MANIFEST_CONTRACTS_BY_NAME, ManifestContract

__all__ = [
    "ContractValidationError",
    "ValidationIssue",
    "validate_dataset",
    "validate_manifest",
    "validate_publication",
    "validate_record",
]


def validate_record(
    contract: DatasetContract,
    record: Mapping[str, object],
    *,
    lineage_context: Mapping[str, object] | None = None,
) -> None:
    """Validate one dataset record against the canonical catalog contract."""

    _validate_record(contract, record, lineage_context=lineage_context)


def validate_dataset(
    contract: DatasetContract,
    records: Iterable[Mapping[str, object]],
    *,
    lineage_context: Mapping[str, object] | None = None,
) -> None:
    """Validate records, logical uniqueness, temporal coverage, and bounds."""

    _validate_dataset(contract, records, lineage_context=lineage_context)


def validate_manifest(
    contract: ManifestContract,
    record: Mapping[str, object],
    *,
    lineage_context: Mapping[str, object] | None = None,
) -> None:
    """Validate a manifest using its catalog field specifications and status."""

    issues = manifest_issues(contract, record, lineage_context=lineage_context)
    _raise_if_invalid(issues)


def validate_publication(
    record: Mapping[str, object],
    *,
    context: Mapping[str, object] | None = None,
    application_copy: Mapping[str, object] | None = None,
) -> None:
    """Validate one approved publication and its coherent application context."""

    contract = MANIFEST_CONTRACTS_BY_NAME["publication_manifest"]
    issues = manifest_issues(contract, record, lineage_context=context)
    issues.extend(publication_issues(record, context=context, application_copy=application_copy))
    _raise_if_invalid(issues)
