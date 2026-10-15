"""Manifest field, lifecycle, run-type, and conditional-lineage validation."""

from __future__ import annotations

from collections.abc import Mapping

from ..manifests import FORECAST_RUN_TYPES, POLICY_EVALUATION_RUN_TYPES, ManifestContract
from .common import (
    ValidationIssue,
    _is_identifier_field,
    _issue,
    _logical_type_issue,
)


def manifest_issues(
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
