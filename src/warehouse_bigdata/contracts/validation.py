"""Deterministic validation for the cataloged logical contracts."""

from __future__ import annotations

import json
import math
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from numbers import Real

from .datasets import (
    BOOL,
    DATE,
    FINITE_FLOAT,
    INTEGER,
    NON_NEGATIVE_FLOAT,
    NON_NEGATIVE_INT,
    STRING,
    TIMESTAMP_UTC,
    DatasetContract,
    FieldSpec,
)
from .manifests import (
    APPROVED,
    FORECAST_RUN_TYPES,
    MANIFEST_CONTRACTS_BY_NAME,
    POLICY_EVALUATION_RUN_TYPES,
    PUBLICATION_CONTEXT_FIELDS,
    ManifestContract,
)


@dataclass(frozen=True, slots=True)
class ValidationIssue:
    """One stable, actionable contract-validation diagnostic."""

    code: str
    contract_name: str
    record_index: int | None = None
    key: tuple[object, ...] | None = None
    field: str | None = None
    message: str = ""

    def __str__(self) -> str:
        location = self.contract_name
        if self.record_index is not None:
            location += f" record[{self.record_index}]"
        if self.key is not None:
            location += f" key={self.key!r}"
        if self.field is not None:
            location += f" field={self.field}"
        return f"{self.code} ({location}): {self.message}"


class ContractValidationError(ValueError):
    """Raised when one or more contract invariants are violated."""

    def __init__(self, issues: Iterable[ValidationIssue]) -> None:
        self.issues = tuple(issues)
        if not self.issues:
            raise ValueError("ContractValidationError requires at least one issue")
        super().__init__("\n".join(str(issue) for issue in self.issues))


def validate_record(
    contract: DatasetContract,
    record: Mapping[str, object],
    *,
    lineage_context: Mapping[str, object] | None = None,
) -> None:
    """Validate one dataset record against the canonical catalog contract."""

    issues = _dataset_record_issues(contract, record, lineage_context=lineage_context)
    _raise_if_invalid(issues)


def validate_dataset(
    contract: DatasetContract,
    records: Iterable[Mapping[str, object]],
    *,
    lineage_context: Mapping[str, object] | None = None,
) -> None:
    """Validate records, logical uniqueness, temporal coverage, and bounds."""

    materialized = list(records)
    issues: list[ValidationIssue] = []
    seen_keys: dict[tuple[object, ...], int] = {}

    for index, record in enumerate(materialized):
        issues.extend(
            _dataset_record_issues(
                contract,
                record,
                record_index=index,
                lineage_context=lineage_context,
            )
        )
        key = _record_key(record, contract.unique_key)
        if key is None:
            continue
        if key in seen_keys:
            issues.append(
                _issue(
                    "DUPLICATE_KEY",
                    contract.name,
                    record_index=index,
                    key=key,
                    field="unique_key",
                    message=(
                        f"logical key duplicates record {seen_keys[key]} at the declared "
                        "dataset grain"
                    ),
                )
            )
        else:
            seen_keys[key] = index

    if contract.name == "forecast_daily":
        issues.extend(_forecast_coverage_issues(materialized, contract.name))
    if contract.name == "allocation_recommendations":
        issues.extend(_allocation_bound_issues(materialized, contract.name))

    _raise_if_invalid(issues)


def validate_manifest(
    contract: ManifestContract,
    record: Mapping[str, object],
    *,
    lineage_context: Mapping[str, object] | None = None,
) -> None:
    """Validate a manifest using its catalog field specifications and status."""

    issues = _manifest_issues(contract, record, lineage_context=lineage_context)
    _raise_if_invalid(issues)


def validate_publication(
    record: Mapping[str, object],
    *,
    context: Mapping[str, object] | None = None,
    application_copy: Mapping[str, object] | None = None,
) -> None:
    """Validate one approved publication and its coherent application context."""

    contract = MANIFEST_CONTRACTS_BY_NAME["publication_manifest"]
    issues = _manifest_issues(contract, record, lineage_context=context)
    issues.extend(_publication_issues(record, context=context, application_copy=application_copy))
    _raise_if_invalid(issues)


def _dataset_record_issues(
    contract: DatasetContract,
    record: Mapping[str, object],
    *,
    record_index: int | None = None,
    lineage_context: Mapping[str, object] | None = None,
) -> list[ValidationIssue]:
    if not isinstance(record, Mapping):
        return [
            _issue(
                "TYPE_MISMATCH",
                contract.name,
                record_index=record_index,
                message="record must be a mapping of declared field names to values",
            )
        ]

    fields = {field.name: field for field in contract.fields}
    issues: list[ValidationIssue] = []
    for field_name in sorted(set(record) - set(fields)):
        issues.append(
            _issue(
                "UNEXPECTED_FIELD",
                contract.name,
                record_index=record_index,
                field=str(field_name),
                message="field is not declared by the catalog contract",
            )
        )

    for field in contract.fields:
        if field.name not in record:
            code = "MISSING_LINEAGE" if field.name in contract.required_lineage else "MISSING_FIELD"
            issues.append(
                _issue(
                    code,
                    contract.name,
                    record_index=record_index,
                    field=field.name,
                    message="required field is absent",
                )
            )
            continue
        value = record[field.name]
        if value is None:
            if not field.nullable:
                issues.append(
                    _issue(
                        "NULL_NOT_ALLOWED",
                        contract.name,
                        record_index=record_index,
                        field=field.name,
                        message="field is non-nullable in the catalog contract",
                    )
                )
            continue
        type_issue = _logical_type_issue(field, value)
        if type_issue is not None:
            code, message = type_issue
            issues.append(
                _issue(
                    code,
                    contract.name,
                    record_index=record_index,
                    field=field.name,
                    message=message,
                )
            )
        if _is_identifier_field(field.name) and isinstance(value, str) and not value:
            issues.append(
                _issue(
                    "INVALID_IDENTIFIER",
                    contract.name,
                    record_index=record_index,
                    field=field.name,
                    message="identifier values must not be empty",
                )
            )

    issues.extend(
        _dataset_lineage_issues(
            contract,
            record,
            record_index=record_index,
            lineage_context=lineage_context,
        )
    )
    issues.extend(_dataset_specific_record_issues(contract, record, record_index=record_index))
    return issues


def _dataset_lineage_issues(
    contract: DatasetContract,
    record: Mapping[str, object],
    *,
    record_index: int | None,
    lineage_context: Mapping[str, object] | None,
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    for lineage_name in contract.required_lineage:
        expected = None if lineage_context is None else lineage_context.get(lineage_name)
        if lineage_name not in record:
            if expected in (None, ""):
                issues.append(
                    _issue(
                        "MISSING_LINEAGE",
                        contract.name,
                        record_index=record_index,
                        field=lineage_name,
                        message="required lineage is absent from the row and context",
                    )
                )
            continue
        actual = record[lineage_name]
        if expected is not None and actual != expected:
            issues.append(
                _issue(
                    "LINEAGE_MISMATCH",
                    contract.name,
                    record_index=record_index,
                    field=lineage_name,
                    message=f"value {actual!r} does not match lineage context {expected!r}",
                )
            )
    return issues


def _dataset_specific_record_issues(
    contract: DatasetContract,
    record: Mapping[str, object],
    *,
    record_index: int | None,
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    if contract.name == "forecast_daily":
        horizon = record.get("horizon")
        if isinstance(horizon, int) and not isinstance(horizon, bool) and not 1 <= horizon <= 7:
            issues.append(
                _issue(
                    "DOMAIN_VIOLATION",
                    contract.name,
                    record_index=record_index,
                    field="horizon",
                    message="forecast horizon must be an integer from 1 through 7",
                )
            )
        run_type = record.get("run_type")
        if run_type not in FORECAST_RUN_TYPES:
            issues.append(
                _issue(
                    "INVALID_ENUM",
                    contract.name,
                    record_index=record_index,
                    field="run_type",
                    message=f"run_type must be one of {FORECAST_RUN_TYPES!r}",
                )
            )
        issue_date = _parse_date(record.get("issue_date"))
        forecast_date = _parse_date(record.get("forecast_date"))
        if issue_date is not None and forecast_date is not None and isinstance(horizon, int):
            if forecast_date != issue_date + timedelta(days=horizon):
                issues.append(
                    _issue(
                        "TEMPORAL_MISMATCH",
                        contract.name,
                        record_index=record_index,
                        field="forecast_date",
                        message="forecast_date must equal issue_date plus horizon days",
                    )
                )
    elif contract.name in {"forecast_metrics", "inventory_policy_evaluation"}:
        run_type = record.get("run_type")
        if run_type == "forward_demo":
            issues.append(
                _issue(
                    "RUN_TYPE_MISMATCH",
                    contract.name,
                    record_index=record_index,
                    field="run_type",
                    message="forward_demo runs cannot contain observed evaluation outcomes",
                )
            )
        elif run_type not in POLICY_EVALUATION_RUN_TYPES:
            issues.append(
                _issue(
                    "INVALID_ENUM",
                    contract.name,
                    record_index=record_index,
                    field="run_type",
                    message=f"run_type must be one of {POLICY_EVALUATION_RUN_TYPES!r}",
                )
            )
    elif contract.name == "allocation_recommendations":
        requested = record.get("requested_units")
        allocated = record.get("allocated_units")
        if _is_nonnegative_integer(requested) and _is_nonnegative_integer(allocated):
            if allocated > requested:
                issues.append(
                    _issue(
                        "CONSTRAINT_VIOLATION",
                        contract.name,
                        record_index=record_index,
                        field="allocated_units",
                        message="allocated_units must not exceed requested_units",
                    )
                )
    return issues


def _forecast_coverage_issues(
    records: list[Mapping[str, object]], contract_name: str
) -> list[ValidationIssue]:
    series: dict[tuple[object, ...], dict[int, int]] = defaultdict(dict)
    issues: list[ValidationIssue] = []
    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            continue
        series_key_fields = ("forecast_run_id", "item_id", "store_id")
        if not all(field in record for field in series_key_fields):
            continue
        horizon = record.get("horizon")
        if not _is_integer(horizon):
            continue
        key = tuple(record[field] for field in series_key_fields)
        if horizon in series[key]:
            issues.append(
                _issue(
                    "DUPLICATE_HORIZON",
                    contract_name,
                    record_index=index,
                    key=key + (horizon,),
                    field="horizon",
                    message="a forecast series contains the same horizon more than once",
                )
            )
        else:
            series[key][horizon] = index

    expected = set(range(1, 8))
    for key, horizons in series.items():
        actual = set(horizons)
        if actual != expected:
            missing = sorted(expected - actual)
            issues.append(
                _issue(
                    "INCOMPLETE_HORIZON_COVERAGE",
                    contract_name,
                    key=key,
                    field="horizon",
                    message=f"series must contain horizons 1 through 7; missing {missing}",
                )
            )
    return issues


def _allocation_bound_issues(
    records: list[Mapping[str, object]], contract_name: str
) -> list[ValidationIssue]:
    groups: dict[tuple[object, ...], list[Mapping[str, object]]] = defaultdict(list)
    for record in records:
        if not isinstance(record, Mapping):
            continue
        fields = ("scenario_id", "issue_date", "item_id", "source_warehouse_id")
        if all(field in record for field in fields):
            groups[tuple(record[field] for field in fields)].append(record)

    issues: list[ValidationIssue] = []
    for key, group in groups.items():
        allocations = [record.get("allocated_units") for record in group]
        capacities = [record.get("central_available_units") for record in group]
        if not all(_is_nonnegative_integer(value) for value in allocations + capacities):
            continue
        if sum(allocations) > max(capacities):
            issues.append(
                _issue(
                    "CONSTRAINT_VIOLATION",
                    contract_name,
                    key=key,
                    field="allocated_units",
                    message="total allocation must not exceed one central supply row",
                )
            )
    return issues


def _manifest_issues(
    contract: ManifestContract,
    record: Mapping[str, object],
    *,
    lineage_context: Mapping[str, object] | None,
) -> list[ValidationIssue]:
    if not isinstance(record, Mapping):
        return [
            _issue(
                "TYPE_MISMATCH",
                contract.name,
                message="manifest must be a mapping of declared field names to values",
            )
        ]

    fields = {field.name: field for field in contract.field_specs}
    allowed_fields = set(fields) | set(contract.conditional_identifiers)
    issues: list[ValidationIssue] = []
    for field_name in sorted(set(record) - allowed_fields):
        issues.append(
            _issue(
                "UNEXPECTED_FIELD",
                contract.name,
                field=str(field_name),
                message="field is not declared by the manifest contract",
            )
        )

    for field in contract.field_specs:
        if field.name not in record:
            issues.append(
                _issue(
                    "MISSING_FIELD",
                    contract.name,
                    field=field.name,
                    message="required manifest field is absent",
                )
            )
            continue
        value = record[field.name]
        if value is None:
            if not field.nullable:
                issues.append(
                    _issue(
                        "NULL_NOT_ALLOWED",
                        contract.name,
                        field=field.name,
                        message="field is non-nullable in the manifest contract",
                    )
                )
            continue
        type_issue = _logical_type_issue(field, value)
        if type_issue is not None:
            code, message = type_issue
            issues.append(_issue(code, contract.name, field=field.name, message=message))
        if _is_identifier_field(field.name) and isinstance(value, str) and not value:
            issues.append(
                _issue(
                    "INVALID_IDENTIFIER",
                    contract.name,
                    field=field.name,
                    message="identifier values must not be empty",
                )
            )

    for field_name in contract.conditional_identifiers:
        if field_name not in record:
            continue
        value = record[field_name]
        if not isinstance(value, str):
            issues.append(
                _issue(
                    "TYPE_MISMATCH",
                    contract.name,
                    field=field_name,
                    message="conditional lineage identifiers must be strings",
                )
            )
        elif not value:
            issues.append(
                _issue(
                    "INVALID_IDENTIFIER",
                    contract.name,
                    field=field_name,
                    message="conditional lineage identifiers must not be empty",
                )
            )

    if record.get("contract_name") != contract.name:
        issues.append(
            _issue(
                "CONTRACT_MISMATCH",
                contract.name,
                field="contract_name",
                message=f"contract_name must be {contract.name!r}",
            )
        )
    if record.get("contract_version") != contract.version:
        issues.append(
            _issue(
                "VERSION_MISMATCH",
                contract.name,
                field="contract_version",
                message=f"contract_version must be {contract.version!r}",
            )
        )
    if record.get("manifest_kind") != contract.manifest_kind:
        issues.append(
            _issue(
                "CONTRACT_MISMATCH",
                contract.name,
                field="manifest_kind",
                message=f"manifest_kind must be {contract.manifest_kind!r}",
            )
        )

    status = record.get("status")
    if status not in contract.status_values:
        issues.append(
            _issue(
                "INVALID_ENUM",
                contract.name,
                field="status",
                message=f"status must be one of {contract.status_values!r}",
            )
        )
    required_status = None if lineage_context is None else lineage_context.get("required_status")
    if required_status is not None and status != required_status:
        issues.append(
            _issue(
                "STATUS_MISMATCH",
                contract.name,
                field="status",
                message=f"status must be {required_status!r} for this validation context",
            )
        )

    if contract.name == "forecast_manifest":
        run_type = record.get("run_type")
        if run_type not in FORECAST_RUN_TYPES:
            issues.append(
                _issue(
                    "INVALID_ENUM",
                    contract.name,
                    field="run_type",
                    message=f"run_type must be one of {FORECAST_RUN_TYPES!r}",
                )
            )
        if record.get("horizon_days") != 7:
            issues.append(
                _issue(
                    "DOMAIN_VIOLATION",
                    contract.name,
                    field="horizon_days",
                    message="forecast manifests must declare a seven-day horizon",
                )
            )
    elif contract.name == "evaluation_manifest":
        run_type = record.get("run_type")
        if run_type == "forward_demo":
            issues.append(
                _issue(
                    "RUN_TYPE_MISMATCH",
                    contract.name,
                    field="run_type",
                    message="forward_demo runs cannot represent observed evaluation outcomes",
                )
            )
        elif run_type not in POLICY_EVALUATION_RUN_TYPES:
            issues.append(
                _issue(
                    "INVALID_ENUM",
                    contract.name,
                    field="run_type",
                    message=f"run_type must be one of {POLICY_EVALUATION_RUN_TYPES!r}",
                )
            )

    if lineage_context is not None:
        context_keys = set(contract.field_names) | set(contract.conditional_identifiers)
        for field_name, expected in lineage_context.items():
            if field_name in {"required_status"} or field_name not in context_keys:
                continue
            if expected in (None, ""):
                continue
            if field_name not in record:
                issues.append(
                    _issue(
                        "MISSING_LINEAGE",
                        contract.name,
                        field=field_name,
                        message="required lineage is absent from the manifest",
                    )
                )
            elif record[field_name] != expected:
                issues.append(
                    _issue(
                        "LINEAGE_MISMATCH",
                        contract.name,
                        field=field_name,
                        message=f"value {record[field_name]!r} does not match {expected!r}",
                    )
                )
    return issues


def _publication_issues(
    record: Mapping[str, object],
    *,
    context: Mapping[str, object] | None,
    application_copy: Mapping[str, object] | None,
) -> list[ValidationIssue]:
    contract_name = "publication_manifest"
    issues: list[ValidationIssue] = []
    required_metadata = (
        "publication_id",
        "selected_dataset_manifests",
        "approved_status",
        "application_manifest_checksum",
        "materialization_reference",
    )
    for field_name in required_metadata:
        if field_name not in record or record[field_name] in (None, ""):
            issues.append(
                _issue(
                    "MISSING_PUBLICATION_METADATA",
                    contract_name,
                    field=field_name,
                    message="approved publication metadata is required",
                )
            )

    if record.get("status") != APPROVED or record.get("approved_status") != APPROVED:
        issues.append(
            _issue(
                "INVALID_STATUS",
                contract_name,
                field="approved_status",
                message="publication status and approved_status must both be APPROVED",
            )
        )

    if context is None:
        issues.append(
            _issue(
                "MISSING_PUBLICATION_METADATA",
                contract_name,
                field="context",
                message="publication validation requires one coherent publication context",
            )
        )
    else:
        issues.extend(_publication_context_issues(context))
        if context.get("contract_version") != record.get("contract_version"):
            issues.append(
                _issue(
                    "VERSION_MISMATCH",
                    contract_name,
                    field="contract_version",
                    message="publication context and manifest versions must agree",
                )
            )
        identity_fields = (
            "publication_id",
            "source_snapshot_id",
            "model_run_id",
            "forecast_run_id",
            "scenario_id",
            "decision_run_id",
            "evaluation_run_id",
        )
        for field_name in identity_fields:
            expected = context.get(field_name)
            if expected is None:
                continue
            if record.get(field_name) != expected:
                issues.append(
                    _issue(
                        "LINEAGE_MISMATCH",
                        contract_name,
                        field=field_name,
                        message=f"publication value must match context value {expected!r}",
                    )
                )

        manifest_checksum = context.get("manifest_checksum")
        if manifest_checksum not in (
            record.get("artifact_checksum"),
            record.get("application_manifest_checksum"),
        ):
            issues.append(
                _issue(
                    "PUBLICATION_MISMATCH",
                    contract_name,
                    field="manifest_checksum",
                    message="context checksum must match publication manifest checksums",
                )
            )

    if application_copy is None:
        issues.append(
            _issue(
                "MISSING_PUBLICATION_METADATA",
                contract_name,
                field="application_copy",
                message="application publication metadata copy is required",
            )
        )
    else:
        for field_name in ("publication_id", "manifest_checksum"):
            if field_name not in application_copy:
                issues.append(
                    _issue(
                        "MISSING_PUBLICATION_METADATA",
                        contract_name,
                        field=f"application_copy.{field_name}",
                        message="application copy must preserve publication identity",
                    )
                )
        if application_copy.get("publication_id") != record.get("publication_id"):
            issues.append(
                _issue(
                    "PUBLICATION_MISMATCH",
                    contract_name,
                    field="application_copy.publication_id",
                    message="application copy publication_id must match the manifest",
                )
            )
        if application_copy.get("manifest_checksum") != record.get("artifact_checksum"):
            issues.append(
                _issue(
                    "PUBLICATION_MISMATCH",
                    contract_name,
                    field="application_copy.manifest_checksum",
                    message="application copy checksum must match the manifest artifact checksum",
                )
            )

    for field_name in ("selected_dataset_manifests", "serving_schema_versions"):
        value = record.get(field_name)
        if not isinstance(value, str) or not value:
            continue
        try:
            versions = json.loads(value)
        except json.JSONDecodeError:
            issues.append(
                _issue(
                    "TYPE_MISMATCH",
                    contract_name,
                    field=field_name,
                    message="publication version metadata must be a JSON object string",
                )
            )
            continue
        if not isinstance(versions, dict) or not versions:
            issues.append(
                _issue(
                    "MISSING_PUBLICATION_METADATA",
                    contract_name,
                    field=field_name,
                    message="publication version metadata must contain at least one dataset",
                )
            )
            continue
        for dataset_name, version in versions.items():
            if not isinstance(dataset_name, str) or version != record.get("contract_version"):
                issues.append(
                    _issue(
                        "VERSION_MISMATCH",
                        contract_name,
                        field=field_name,
                        message="selected dataset versions must match the publication contract version",
                    )
                )
    return issues


def _publication_context_issues(context: Mapping[str, object]) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    required_fields = set(PUBLICATION_CONTEXT_FIELDS) - {"evaluation_run_id", "holdout_id"}
    for field_name in sorted(required_fields):
        if field_name not in context or context[field_name] in (None, ""):
            issues.append(
                _issue(
                    "MISSING_PUBLICATION_METADATA",
                    "publication_manifest",
                    field=f"context.{field_name}",
                    message="publication context field is required",
                )
            )
    if "issue_date" in context and _parse_date(context["issue_date"]) is None:
        issues.append(
            _issue(
                "TYPE_MISMATCH",
                "publication_manifest",
                field="context.issue_date",
                message="publication context issue_date must be an ISO date",
            )
        )
    policy_ids = context.get("policy_ids")
    if (
        not isinstance(policy_ids, list)
        or not policy_ids
        or not all(isinstance(policy_id, str) and policy_id for policy_id in policy_ids)
    ):
        issues.append(
            _issue(
                "TYPE_MISMATCH",
                "publication_manifest",
                field="context.policy_ids",
                message="policy_ids must be a non-empty list of identifiers",
            )
        )
    coverage = context.get("coverage")
    if not isinstance(coverage, Mapping):
        issues.append(
            _issue(
                "TYPE_MISMATCH",
                "publication_manifest",
                field="context.coverage",
                message="coverage must be a mapping of measured publication counts",
            )
        )
    return issues


def _logical_type_issue(field: FieldSpec, value: object) -> tuple[str, str] | None:
    logical_type = field.logical_type
    if logical_type == STRING:
        if not isinstance(value, str):
            return "TYPE_MISMATCH", "value must be a string"
        return None
    if logical_type == DATE:
        if _parse_date(value) is None:
            return "TYPE_MISMATCH", "value must be an ISO calendar date string"
        return None
    if logical_type == TIMESTAMP_UTC:
        if _parse_timestamp(value) is None:
            return "TYPE_MISMATCH", "value must be an ISO UTC timestamp string"
        return None
    if logical_type == BOOL:
        if not isinstance(value, bool):
            return "TYPE_MISMATCH", "value must be a boolean"
        return None
    if logical_type in {NON_NEGATIVE_INT, INTEGER}:
        if not _is_integer(value):
            return "TYPE_MISMATCH", "value must be an integer and booleans are not integers"
        if logical_type == NON_NEGATIVE_INT and value < 0:  # type: ignore[operator]
            return "DOMAIN_VIOLATION", "value must be nonnegative"
        return None
    if logical_type in {FINITE_FLOAT, NON_NEGATIVE_FLOAT}:
        if not _is_real_number(value):
            return "TYPE_MISMATCH", "value must be a finite numeric value"
        if not math.isfinite(value):  # type: ignore[arg-type]
            return "DOMAIN_VIOLATION", "value must be finite"
        if logical_type == NON_NEGATIVE_FLOAT and value < 0:  # type: ignore[operator]
            return "DOMAIN_VIOLATION", "value must be nonnegative"
        return None
    return "TYPE_MISMATCH", f"unsupported logical type {logical_type!r}"


def _record_key(
    record: Mapping[str, object], field_names: tuple[str, ...]
) -> tuple[object, ...] | None:
    if not isinstance(record, Mapping) or not all(name in record for name in field_names):
        return None
    values = tuple(record[name] for name in field_names)
    if any(value is None for value in values):
        return None
    try:
        hash(values)
    except TypeError:
        return None
    return values


def _is_identifier_field(name: str) -> bool:
    return (
        name.endswith("_id")
        or name.endswith("_version")
        or name
        in {
            "source_version",
            "feature_version",
            "contract_version",
        }
    )


def _is_integer(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_nonnegative_integer(value: object) -> bool:
    return _is_integer(value) and value >= 0  # type: ignore[operator]


def _is_real_number(value: object) -> bool:
    return isinstance(value, Real) and not isinstance(value, bool)


def _parse_date(value: object) -> date | None:
    if not isinstance(value, str):
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _parse_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        return None
    return parsed


def _issue(
    code: str,
    contract_name: str,
    *,
    record_index: int | None = None,
    key: tuple[object, ...] | None = None,
    field: str | None = None,
    message: str,
) -> ValidationIssue:
    return ValidationIssue(
        code=code,
        contract_name=contract_name,
        record_index=record_index,
        key=key,
        field=field,
        message=message,
    )


def _raise_if_invalid(issues: Iterable[ValidationIssue]) -> None:
    materialized = tuple(issues)
    if materialized:
        raise ContractValidationError(materialized)
