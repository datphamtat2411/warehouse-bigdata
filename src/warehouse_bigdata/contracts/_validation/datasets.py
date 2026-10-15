"""Dataset record, lineage, temporal, allocation, and size validation."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping, Sized
from datetime import timedelta
from itertools import islice

from ..datasets import DatasetContract
from ..manifests import FORECAST_RUN_TYPES, POLICY_EVALUATION_RUN_TYPES
from .common import (
    ValidationIssue,
    _is_identifier_field,
    _is_integer,
    _is_nonnegative_integer,
    _issue,
    _logical_type_issue,
    _parse_date,
    _raise_if_invalid,
    _record_key,
)

_MAX_DATASET_RECORDS = 10_000


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
    """Validate a bounded collection of records and dataset-level invariants."""

    if isinstance(records, Sized) and len(records) > _MAX_DATASET_RECORDS:
        _raise_if_invalid([_dataset_too_large_issue(contract.name)])

    materialized = list(islice(records, _MAX_DATASET_RECORDS + 1))
    if len(materialized) > _MAX_DATASET_RECORDS:
        _raise_if_invalid([_dataset_too_large_issue(contract.name)])

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


def _dataset_too_large_issue(contract_name: str) -> ValidationIssue:
    return _issue(
        "DATASET_TOO_LARGE",
        contract_name,
        message=(
            f"dataset validation accepts at most {_MAX_DATASET_RECORDS:,} records; "
            "use the distributed validation path for larger workloads"
        ),
    )


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
