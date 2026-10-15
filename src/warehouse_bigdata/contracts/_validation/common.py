"""Shared diagnostics, scalar parsing, and logical-type validation helpers."""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from numbers import Real

from ..datasets import (
    BOOL,
    DATE,
    FINITE_FLOAT,
    INTEGER,
    NON_NEGATIVE_FLOAT,
    NON_NEGATIVE_INT,
    STRING,
    TIMESTAMP_UTC,
    FieldSpec,
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
