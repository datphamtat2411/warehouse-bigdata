"""Distributed, independent quality gates for persisted historical feature Parquet."""

from __future__ import annotations

from functools import reduce
from operator import or_
from typing import TYPE_CHECKING

from warehouse_bigdata.contracts.datasets import FORECAST_FEATURES
from warehouse_bigdata.forecasting.features import TEST_DAYS, TRAIN_DAYS, VALIDATION_DAYS

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from warehouse_bigdata.forecasting.features import HistoricalFeatureResult


FEATURE_TYPES = (
    *("string",) * 6,
    "date",
    "date",
    "bigint",
    "bigint",
    "bigint",
    "double",
    "double",
    "int",
    "int",
    "boolean",
    "boolean",
    "boolean",
    "double",
    "bigint",
)
SCOPED_KEY = (
    "source_snapshot_id",
    "feature_version",
    "feature_role",
    *FORECAST_FEATURES.unique_key,
)
_DIAGNOSTIC_KEY = ("source_snapshot_id", "feature_version", "item_id", "store_id", "target_date")
_GROUPS = ("source_snapshot_id", "feature_version", "feature_role", "status")
_COUNTS = ("observed_count", "candidate_count", "eligible_count", "excluded_count")
_STATUSES = ("LOOKBACK_ONLY", "INSUFFICIENT_HISTORY", "DISCONTINUOUS_HISTORY", "ELIGIBLE")


class FeatureValidationError(ValueError):
    """Persisted features or diagnostics disagree with the approved source/transformation."""


def typed_feature_schema() -> list[dict]:
    """Fingerprint input: ordered names, exact Spark types, and logical v1 nullability."""
    return [
        {"name": field.name, "type": kind, "nullable": field.nullable}
        for field, kind in zip(FORECAST_FEATURES.fields, FEATURE_TYPES, strict=True)
    ]


def _reject_rows(frame, message):
    samples = frame.limit(8).collect()
    if samples:
        raise FeatureValidationError(f"{message}; samples={[r.asDict() for r in samples]}")


def _same_rows(actual, expected, message):
    if len(actual.columns) != len(expected.columns) or set(actual.columns) != set(expected.columns):
        raise FeatureValidationError(f"{message}: fields differ")
    ordered = actual.select(*expected.columns)
    _reject_rows(ordered.exceptAll(expected).unionByName(expected.exceptAll(ordered)), message)


def validate_persisted_feature_snapshot(
    persisted: DataFrame,
    *,
    expected: HistoricalFeatureResult,
    source: DataFrame,
    exclusions: DataFrame,
    source_snapshot_id: str,
    feature_version: str,
) -> dict:
    """Validate actual inferred Parquet, retaining only bounded summaries on the driver.

    Expected T01 features are reconciled as multisets in both directions, independently
    of schema/domain/source checks. Diagnostics cover every observed row, including
    lookback-only rows. Roles absent from the declared source scope are legitimate.
    """
    from pyspark.sql import functions as F

    fields = FORECAST_FEATURES.field_names
    if len(persisted.columns) != len(fields) or set(persisted.columns) != set(fields):
        raise FeatureValidationError("persisted feature fields differ from FORECAST_FEATURES v1.0")
    types = {f.name: f.dataType.simpleString() for f in persisted.schema}
    if types != dict(zip(fields, FEATURE_TYPES, strict=True)):
        raise FeatureValidationError("persisted feature physical types differ from T01")
    persisted = persisted.select(*fields)
    required = (*FORECAST_FEATURES.required_field_names, "label")
    summary = (
        persisted.agg(
            F.count("*").alias("row_count"),
            *[F.count(F.when(F.col(name).isNull(), 1)).alias(name) for name in required],
        )
        .first()
        .asDict()
    )
    count = summary.pop("row_count")
    if count == 0:
        raise FeatureValidationError("an empty eligible feature artifact is not publishable")
    if any(summary.values()):
        raise FeatureValidationError(f"required historical fields contain nulls: {summary}")

    weekday = F.pmod(F.dayofweek("target_date") + 5, F.lit(7)) + 1
    invalid = [
        *[F.col(name).rlike(r"^\s*$") for name in fields[:6]],
        F.col("source_snapshot_id") != source_snapshot_id,
        F.col("feature_version") != feature_version,
        ~F.col("feature_role").isin("train", "validation", "test"),
        F.col("as_of_date") != F.date_sub("target_date", 1),
        F.col("day_of_week") != weekday,
        F.col("month") != F.month("target_date"),
        F.col("is_weekend") != weekday.isin(6, 7),
        F.col("event_flag"),
        F.col("price_missing") != F.col("last_known_price").isNull(),
        *[F.col(name) < 0 for name in ("lag_1", "lag_7", "lag_28", "label")],
        *[
            (F.col(name) < 0) | F.isnan(name) | (F.abs(F.col(name)) > 1.7976931348623157e308)
            for name in ("rolling_mean_7", "rolling_mean_28")
        ],
        F.col("last_known_price").isNotNull()
        & (
            (F.col("last_known_price") <= 0)
            | F.isnan("last_known_price")
            | (F.abs(F.col("last_known_price")) > 1.7976931348623157e308)
        ),
    ]
    _reject_rows(persisted.where(reduce(or_, invalid)), "persisted historical feature invariant")
    _reject_rows(
        persisted.groupBy(*SCOPED_KEY).count().where("count != 1"), "duplicate scoped feature keys"
    )
    # This join checks original observed labels, department, and canonical split dates.
    observed = source.select(
        "item_id",
        "store_id",
        F.col("date").alias("target_date"),
        F.col("dept_id").alias("source_dept"),
        "day_index",
        "unit_sales",
    )
    joined = persisted.join(observed, list(FORECAST_FEATURES.unique_key), "left")
    day = F.col("day_index")
    role = (
        F.when(day.between(*TRAIN_DAYS), "train")
        .when(day.between(*VALIDATION_DAYS), "validation")
        .when(day.between(*TEST_DAYS), "test")
    )
    _reject_rows(
        joined.where(
            day.isNull()
            | ~F.col("feature_role").eqNullSafe(role)
            | (F.col("dept_id") != F.col("source_dept"))
            | (F.col("label") != F.col("unit_sales"))
        ),
        "source label/department/split mismatch",
    )
    _same_rows(persisted, expected.features, "persisted feature content reconciliation")

    eligibility = expected.eligibility
    _reject_rows(
        eligibility.where(
            ~F.col("status").isin(*_STATUSES)
            | F.col("status").isNull()
            | (F.col("source_snapshot_id") != source_snapshot_id)
            | (F.col("feature_version") != feature_version)
        ),
        "invalid eligibility diagnostics",
    )
    _same_rows(
        eligibility.select("item_id", "store_id", "target_date", "day_index"),
        source.select("item_id", "store_id", F.col("date").alias("target_date"), "day_index"),
        "observed scope/diagnostic reconciliation",
    )
    _same_rows(
        persisted.select(*SCOPED_KEY),
        eligibility.where("status = 'ELIGIBLE'").select(*SCOPED_KEY),
        "eligible diagnostic key reconciliation",
    )
    _same_rows(
        exclusions,
        eligibility.where("status != 'ELIGIBLE'"),
        "persisted exclusion diagnostic reconciliation",
    )
    _reject_rows(
        eligibility.groupBy(*_DIAGNOSTIC_KEY).count().where("count != 1"),
        "duplicate diagnostic key",
    )
    candidate = F.col("feature_role").isNotNull()
    eligible = F.col("status") == "ELIGIBLE"

    def count_if(condition):
        return F.count(F.when(condition, 1))

    reconciled = eligibility.groupBy(*_GROUPS).agg(
        F.count("*").alias("observed_count"),
        count_if(candidate).alias("candidate_count"),
        count_if(eligible).alias("eligible_count"),
        count_if(candidate & ~eligible).alias("excluded_count"),
    )
    _same_rows(
        reconciled,
        expected.coverage.select(*_GROUPS, *_COUNTS),
        "T01 coverage/reason reconciliation",
    )
    _reject_rows(
        expected.coverage.where(
            (F.col("price_missing_count") < 0)
            | (F.col("price_missing_count") > F.col("observed_count"))
        ),
        "invalid price coverage",
    )
    groups = reconciled.limit(17).collect()
    if len(groups) > 16:
        raise FeatureValidationError("unbounded role/reason coverage")
    groups.sort(key=lambda row: (row.feature_role or "", row.status))
    totals = {field: sum(row[field] for row in groups) for field in _COUNTS}
    lookback = sum(row.observed_count for row in groups if row.status == "LOOKBACK_ONLY")
    if (
        totals["observed_count"] != lookback + totals["candidate_count"]
        or totals["candidate_count"] != totals["eligible_count"] + totals["excluded_count"]
        or totals["eligible_count"] != count
    ):
        raise FeatureValidationError("coverage conservation failed")
    roles = {
        role: sum(r.eligible_count for r in groups if r.feature_role == role)
        for role in ("train", "validation", "test")
    }
    reasons = {
        status: sum(r.observed_count for r in groups if r.status == status)
        for status in _STATUSES
        if status != "ELIGIBLE"
    }
    return {
        **totals,
        "row_count": count,
        "lookback_only_count": lookback,
        "role_counts": roles,
        "reason_counts": reasons,
        "required_field_null_counts": summary,
        "coverage": [r.asDict() for r in groups],
        "persisted_parquet_validated": True,
    }
