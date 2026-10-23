"""Internal M05-T02 as-of state and single-horizon scoring features.

The immutable issue context uses hold_last_known_at_issue prices (including T)
and T01's recorded_events_disabled policy. Only the 28 demand entries are
recursive: M06 supplies each subsequent temporary state, with unrounded doubles
and explicit prediction provenance. This module never performs inference or
updates state, and never publishes or persists DataFrames.

These schemas are internal representation v1, NOT FORECAST_FEATURES v1.0.
feature_version continues to identify transformation semantics independently.
M06's fitted preprocessing must consume numeric double lags without truncation;
category handling and actual fitted-pipeline compatibility belong to M06.

Persistence is caller-owned. M06 must materialize prepared state once before
recursive use and release superseded caches to avoid recomputing source plans.
Ordinary lazy DataFrames returned here do not establish a materialization boundary.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from functools import reduce
from operator import or_
from typing import TYPE_CHECKING, Final

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, BooleanType, DateType, DoubleType, IntegerType, StringType,
    StructField, StructType,
)

from warehouse_bigdata.contracts.datasets import FORECAST_FEATURES, SALES_ENRICHED
from warehouse_bigdata.forecasting.features import (
    LAG_DAYS, LOOKBACK_DAYS, MEAN_DAYS, calendar_features,
)

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

INTERNAL_REPRESENTATION_VERSION: Final = "1"
OBSERVED_ORIGIN: Final = "observed_m5_unit_sales"
PREDICTED_ORIGIN: Final = "predicted_unit_sales"
_KEYS: Final = ("item_id", "store_id")
_METADATA: Final = ("dept_id", "cat_id", "state_id", "series_id")
_MAX_DOUBLE: Final = 1.7976931348623157e308
_SALES_TYPES: Final = (
    *("string",) * 7, "int", "date", "int", "bigint", "double", "boolean",
    "int", "int", "int", "boolean", "boolean", *("string",) * 4,
)
_REASONS: Final = tuple(sorted((
    "MISSING_HISTORY", "INSUFFICIENT_HISTORY", "DISCONTINUOUS_HISTORY", "DUPLICATE_DATE",
    "INVALID_SERIES", "INVALID_VALUE", "INVALID_PROVENANCE", "NON_SEQUENTIAL_HISTORY",
    "LINEAGE_MISMATCH",
)))

HISTORY_ENTRY_SCHEMA_V1 = StructType([
    StructField("date", DateType()),
    StructField("value", DoubleType()),
    StructField("origin", StringType()),
    StructField("prediction_issue_date", DateType()),
])
AS_OF_HISTORY_SCHEMA_V1 = StructType([
    StructField("source_snapshot_id", StringType()),
    StructField("feature_version", StringType()),
    StructField("as_of_date", DateType()),
    *[StructField(name, StringType()) for name in (*_KEYS, *_METADATA)],
    StructField("last_known_price", DoubleType()),
    StructField("price_missing", BooleanType()),
    StructField("history", ArrayType(HISTORY_ENTRY_SCHEMA_V1)),
])
SCORING_FEATURE_SCHEMA_V1 = StructType([
    StructField(field.name, (
        DoubleType() if field.name.startswith(("lag_", "rolling_mean_"))
        or field.name == "last_known_price" else
        DateType() if field.name in ("target_date", "as_of_date") else
        IntegerType() if field.name in ("day_of_week", "month") else
        BooleanType() if field.name in ("is_weekend", "event_flag", "price_missing") else
        StringType()
    ))
    for field in FORECAST_FEATURES.fields if field.name != "label"
])


class ScoringFeatureError(ValueError):
    """Invalid invocation or incompatible internal/input schema.

    Attributable series defects instead appear in distributed eligibility rows.
    """


@dataclass(frozen=True, slots=True)
class AsOfHistoryResult:
    """Eligible bounded state, all-candidate eligibility, and grouped coverage."""

    state: DataFrame
    eligibility: DataFrame
    coverage: DataFrame


@dataclass(frozen=True, slots=True)
class ScoringFeatureResult:
    """One feature row per eligible series, eligibility, and grouped coverage."""

    features: DataFrame
    eligibility: DataFrame
    coverage: DataFrame


def _require_date(name: str, value: date) -> None:
    if type(value) is not date:
        raise ScoringFeatureError(f"{name} must be an explicit Python date")


def _require_schema(frame: DataFrame, names: tuple[str, ...], types: tuple[str, ...]) -> None:
    if tuple(frame.columns) != names:
        raise ScoringFeatureError(f"Expected exact ordered fields {names}")
    for field, expected in zip(frame.schema, types, strict=True):
        if field.dataType.simpleString() != expected:
            raise ScoringFeatureError(f"{field.name} must have Spark type {expected}")


def _blank(value: Column) -> Column:
    return value.isNull() | value.rlike(r"^\s*$")


def _any(conditions) -> Column:
    return reduce(or_, conditions)


def _invalid_value(value: Column) -> Column:
    return value.isNull() | F.isnan(value) | (value < 0.0) | (value > _MAX_DOUBLE)


def _invalid_price(value: Column) -> Column:
    return value.isNotNull() & (_invalid_value(value) | (value == 0.0))


def _flag(condition: Column) -> Column:
    return F.when(F.coalesce(condition, F.lit(False)), 1).otherwise(0)


def _classify(frame: DataFrame, conditions: dict[str, Column]) -> DataFrame:
    """All applicable reasons in a fixed order, without exploding candidates."""
    reasons = F.filter(F.array(*[
        F.when(F.coalesce(conditions.get(reason, F.lit(False)), F.lit(False)), F.lit(reason))
        for reason in _REASONS
    ]), lambda reason: reason.isNotNull())
    return frame.withColumn("reason_codes", reasons).withColumn(
        "status", F.when(F.size("reason_codes") == 0, "ELIGIBLE").otherwise("EXCLUDED"),
    )


def _diagnostics(classified: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Counts are per series, never per reason; groups retain issue lineage."""
    eligibility = classified.select(
        "source_snapshot_id", "feature_version", *_KEYS, "issue_date", "target_date",
        "horizon", "status", "reason_codes", "history_count", "missing_day_count",
        "prediction_count",
    )
    eligible = F.col("status") == "ELIGIBLE"
    coverage = eligibility.groupBy(
        "source_snapshot_id", "feature_version", "issue_date", "target_date", "horizon", "status",
    ).agg(
        F.count(F.lit(1)).alias("candidate_count"),
        F.sum(_flag(eligible)).cast("long").alias("eligible_count"),
        F.sum(_flag(~eligible)).cast("long").alias("excluded_count"),
    )
    return eligibility, coverage


def prepare_as_of_history(
    sales_enriched: DataFrame,
    *,
    issue_date: date,
    source_snapshot_id: str,
    feature_version: str,
    series_scope: DataFrame,
) -> AsOfHistoryResult:
    """Prepare T-27..T from a single authoritative SALES_ENRICHED snapshot.

    The unique item/store scope is required, including candidates with no rows.
    Scope duplicates are an invocation error. Series defects are exclusions;
    null dates cannot establish an as-of observation and invalidate their series.
    No post-issue sales, identity, price, or event values enter any calculation.
    Older demand is not retained; older prices reduce to one scalar per series.

    Eligibility history_count counts recent rows (including duplicates), while
    missing_day_count counts missing calendar dates. Preparation has null target
    and horizon and zero predictions. Missing candidates can have several reasons.
    """
    _require_date("issue_date", issue_date)
    for name, value in (("source_snapshot_id", source_snapshot_id), ("feature_version", feature_version)):
        if not isinstance(value, str) or not value.strip():
            raise ScoringFeatureError(f"{name} must be an explicit nonblank string")
    _require_schema(sales_enriched, SALES_ENRICHED.field_names, _SALES_TYPES)
    _require_schema(series_scope, _KEYS, ("string", "string"))
    if series_scope.groupBy(*_KEYS).count().where(F.col("count") > 1).limit(1).count():
        raise ScoringFeatureError("series_scope must contain unique candidate item/store rows")

    start = F.date_sub(F.lit(issue_date), LOOKBACK_DAYS - 1)
    # Null dates are inspected only to flag the absence of a usable cutoff, never
    # as values or identity context. Post-T rows are removed before all reduction.
    scoped = sales_enriched.where(
        (F.col("date") <= F.lit(issue_date)) | F.col("date").isNull(),
    ).join(series_scope, list(_KEYS), "left_semi")
    past = scoped.where(F.col("date").isNotNull())
    recent = F.col("date") >= start
    metadata_invalid = _any([_blank(F.col(name)) for name in (*_KEYS, *_METADATA)]) | (
        ~F.col("state_id").isin("CA", "TX", "WI")
    )
    price_invalid = _invalid_price(F.col("sell_price")) | ~F.col("price_missing").eqNullSafe(
        F.col("sell_price").isNull(),
    )
    demand_invalid = _invalid_value(F.col("unit_sales").cast("double")) | ~F.col(
        "is_zero_sale"
    ).eqNullSafe(F.col("unit_sales") == 0)
    day_invalid = (
        F.col("day_index").isNull() | (F.col("day_index") < 1)
        | ~F.col("day_key").eqNullSafe(F.concat(F.lit("d_"), F.col("day_index")))
    )
    offset = F.datediff("date", F.lit("1970-01-01").cast("date")) - F.col("day_index")
    summary = past.groupBy(*_KEYS).agg(
        F.count(F.lit(1)).alias("_past_count"),
        F.min("date").alias("_first_date"),
        F.min(F.struct(*_METADATA)).alias("_identity"),
        F.countDistinct(F.struct(*_METADATA)).alias("_identity_count"),
        F.max(_flag(metadata_invalid | day_invalid)).alias("_invalid_series"),
        F.min(offset).alias("_min_offset"), F.max(offset).alias("_max_offset"),
        F.max(_flag(price_invalid | (recent & demand_invalid))).alias("_invalid_value"),
        F.sum(_flag(recent)).cast("long").alias("history_count"),
        F.countDistinct(F.when(recent, F.col("date"))).alias("_date_count"),
        F.countDistinct(F.when(recent, F.col("day_index"))).alias("_day_count"),
        F.max(F.when(F.col("sell_price").isNotNull(), F.struct("date", "sell_price")))
        .alias("_price"),
    )
    # A reused audit ID is invalid for every implicated scoped series. Aggregate
    # scalars instead of collecting all identities or historical observations.
    ambiguous = past.groupBy("series_id").agg(
        F.countDistinct(F.struct(*_KEYS)).alias("_series_count"),
    ).where(F.col("_series_count") > 1).select("series_id")
    ambiguous_keys = past.join(ambiguous, "series_id", "left_semi").select(*_KEYS).distinct().withColumn(
        "_ambiguous", F.lit(True),
    )
    null_dates = scoped.where(F.col("date").isNull()).select(*_KEYS).distinct().withColumn(
        "_null_date", F.lit(True),
    )
    context = (
        series_scope.join(summary, list(_KEYS), "left")
        .join(ambiguous_keys, list(_KEYS), "left").join(null_dates, list(_KEYS), "left")
        .fillna(0, subset=["history_count", "_date_count", "_day_count", "_past_count"])
        .withColumn("source_snapshot_id", F.lit(source_snapshot_id))
        .withColumn("feature_version", F.lit(feature_version))
        .withColumn("issue_date", F.lit(issue_date))
        .withColumn("as_of_date", F.lit(issue_date))
        .withColumn("target_date", F.lit(None).cast("date"))
        .withColumn("horizon", F.lit(None).cast("int"))
        .withColumn("prediction_count", F.lit(0).cast("long"))
        .withColumn("missing_day_count", (F.lit(LOOKBACK_DAYS) - F.col("_date_count")).cast("long"))
        .withColumn("last_known_price", F.col("_price.sell_price"))
        .withColumn("price_missing", F.col("last_known_price").isNull())
    )
    classified = _classify(context, {
        "MISSING_HISTORY": F.col("_past_count") == 0,
        "INSUFFICIENT_HISTORY": F.col("_date_count") < LOOKBACK_DAYS,
        "DISCONTINUOUS_HISTORY": (F.col("missing_day_count") > 0) & (F.col("_first_date") <= start),
        "DUPLICATE_DATE": (F.col("history_count") > F.col("_date_count"))
        | (F.col("history_count") > F.col("_day_count")),
        "INVALID_SERIES": _any([
            *[_blank(F.col(name)) for name in _KEYS],
            F.col("_invalid_series") > 0, F.col("_identity_count") != 1,
            F.col("_min_offset") != F.col("_max_offset"),
            F.coalesce(F.col("_ambiguous"), F.lit(False)),
            F.coalesce(F.col("_null_date"), F.lit(False)),
        ]),
        "INVALID_VALUE": F.col("_invalid_value") > 0,
    })
    eligible = classified.where(F.col("status") == "ELIGIBLE")
    # Eligibility precedes collect_list: even duplicate-heavy corrupt series
    # cannot allocate oversized per-series history arrays.
    histories = past.where(recent).join(eligible.select(*_KEYS), list(_KEYS), "left_semi").groupBy(
        *_KEYS,
    ).agg(F.sort_array(F.collect_list(F.struct(
        F.col("date"), F.col("unit_sales").cast("double").alias("value"),
        F.lit(OBSERVED_ORIGIN).alias("origin"),
        F.lit(None).cast("date").alias("prediction_issue_date"),
    ))).alias("history"))
    state = eligible.join(histories, list(_KEYS)).select(
        "source_snapshot_id", "feature_version", "as_of_date", *_KEYS,
        *[F.col(f"_identity.{name}").alias(name) for name in _METADATA],
        "last_known_price", "price_missing", "history",
    )
    eligibility, coverage = _diagnostics(classified)
    return AsOfHistoryResult(state=state, eligibility=eligibility, coverage=coverage)


def build_scoring_features(
    state: DataFrame,
    *,
    issue_date: date,
    target_date: date,
    known_calendar: DataFrame,
) -> ScoringFeatureResult:
    """Score exactly one h=1..7 feature horizon, without modifying supplied state.

    Calendar context accepts dates only, with unique non-null dates and target
    coverage. All observed entries must be through T; exactly h-1 subsequent
    entries must be predictions issued at T. The array order itself is validated,
    never repaired. Duplicate state rows exclude that series and are counted once.
    Lineage must be nonblank and as_of_date must match T; callers preserve the
    supplied frozen identity/price/lineage when constructing subsequent states.
    No canonical history is consulted or reconstructed during scoring.
    """
    _require_date("issue_date", issue_date)
    _require_date("target_date", target_date)
    horizon = (target_date - issue_date).days
    if not 1 <= horizon <= 7:
        raise ScoringFeatureError("target_date must be issue_date plus a horizon from 1 through 7")
    _require_schema(
        state, tuple(AS_OF_HISTORY_SCHEMA_V1.fieldNames()),
        tuple(field.dataType.simpleString() for field in AS_OF_HISTORY_SCHEMA_V1),
    )
    _require_schema(known_calendar, ("date",), ("date",))
    calendar = known_calendar.agg(
        F.count(F.lit(1)).alias("rows"), F.countDistinct("date").alias("dates"),
        F.sum(_flag(F.col("date") == F.lit(target_date))).alias("target_count"),
    ).first()
    if calendar.rows != calendar.dates or calendar.target_count != 1:
        raise ScoringFeatureError("known_calendar requires unique non-null dates and target-date coverage")

    empty_history = F.array().cast(ArrayType(HISTORY_ENTRY_SCHEMA_V1))
    # Bound all higher-order computation even for an oversized corrupt caller
    # array; the original size is checked separately and such rows never score.
    checked = state.withColumn("history_count", F.when(
        F.col("history").isNull(), 0,
    ).otherwise(F.size("history")).cast("long")).withColumn(
        "_history", F.coalesce(F.slice("history", 1, LOOKBACK_DAYS), empty_history),
    )
    dates = F.transform("_history", lambda entry: entry["date"])
    expected_start = target_date.fromordinal(target_date.toordinal() - LOOKBACK_DAYS)
    in_window = F.filter(dates, lambda day: day.between(F.lit(expected_start), F.lit(target_date))
                         & (day < F.lit(target_date)))
    checked = checked.withColumn("missing_day_count", (
        F.lit(LOOKBACK_DAYS) - F.size(F.array_distinct(in_window))
    ).cast("long")).withColumn("prediction_count", F.size(F.filter(
        "_history", lambda entry: entry["origin"] == PREDICTED_ORIGIN,
    )).cast("long"))
    # Native array indexing preserves fractional predictions. Sum then divide
    # matches T01 averages; a non-finite derived mean is an exclusion, not clipped.
    checked = checked.select(
        "*", *[F.element_at("_history", -days)["value"].alias(f"lag_{days}") for days in LAG_DAYS],
        *[(F.aggregate(
            F.slice("_history", LOOKBACK_DAYS - days + 1, days), F.lit(0.0),
            lambda total, entry: total + entry["value"],
        ) / days).alias(f"rolling_mean_{days}") for days in MEAN_DAYS],
    )
    invalid_provenance = F.exists("_history", lambda entry: ~F.coalesce(
        F.when(entry["date"] <= F.lit(issue_date),
               (entry["origin"] == OBSERVED_ORIGIN) & entry["prediction_issue_date"].isNull())
        .otherwise((entry["origin"] == PREDICTED_ORIGIN)
                   & (entry["prediction_issue_date"] == F.lit(issue_date))
                   & (entry["date"] < F.lit(target_date))), F.lit(False),
    ))
    invalid_order = F.exists(F.transform(
        "_history", lambda entry, index: ~entry["date"].eqNullSafe(F.date_add(F.lit(expected_start), index)),
    ), lambda invalid: invalid)
    conditions = {
        "MISSING_HISTORY": F.col("history_count") == 0,
        "INSUFFICIENT_HISTORY": F.col("history_count") < LOOKBACK_DAYS,
        "DISCONTINUOUS_HISTORY": F.col("missing_day_count") > 0,
        "DUPLICATE_DATE": F.size(dates) > F.size(F.array_distinct(dates)),
        "INVALID_SERIES": _any([_blank(F.col(name)) for name in (*_KEYS, *_METADATA)])
        | ~F.col("state_id").isin("CA", "TX", "WI"),
        "INVALID_VALUE": _any([
            F.exists("_history", lambda entry: entry.isNull() | entry["date"].isNull()
                     | _invalid_value(entry["value"])),
            *[_invalid_value(F.col(f"lag_{days}")) for days in LAG_DAYS],
            *[_invalid_value(F.col(f"rolling_mean_{days}")) for days in MEAN_DAYS],
            _invalid_price(F.col("last_known_price")),
            ~F.col("price_missing").eqNullSafe(F.col("last_known_price").isNull()),
        ]),
        "INVALID_PROVENANCE": invalid_provenance | (F.col("prediction_count") != horizon - 1),
        "NON_SEQUENTIAL_HISTORY": invalid_order | (F.col("history_count") != LOOKBACK_DAYS),
        "LINEAGE_MISMATCH": _blank(F.col("source_snapshot_id")) | _blank(F.col("feature_version"))
        | ~F.col("as_of_date").eqNullSafe(F.lit(issue_date)),
    }
    # Aggregate reason bits, not arrays of state/history rows. This also makes
    # diagnostics deterministic for duplicate supplied states with differing data.
    marked = checked.select("*", *[
        _flag(condition).alias(f"_reason_{reason}") for reason, condition in conditions.items()
    ])
    grouped = marked.groupBy(*_KEYS).agg(
        F.count(F.lit(1)).alias("_state_count"),
        F.min("source_snapshot_id").alias("source_snapshot_id"),
        F.min("feature_version").alias("feature_version"),
        *[F.max(name).alias(name) for name in ("history_count", "missing_day_count", "prediction_count")],
        *[F.max(f"_reason_{reason}").alias(f"_reason_{reason}") for reason in _REASONS],
    ).withColumn("issue_date", F.lit(issue_date)).withColumn(
        "target_date", F.lit(target_date),
    ).withColumn("horizon", F.lit(horizon))
    grouped_conditions = {reason: F.col(f"_reason_{reason}") > 0 for reason in _REASONS}
    grouped_conditions["INVALID_SERIES"] = grouped_conditions["INVALID_SERIES"] | (F.col("_state_count") != 1)
    classified = _classify(grouped, grouped_conditions)
    features = checked.join(
        classified.where(F.col("status") == "ELIGIBLE").select(*_KEYS), list(_KEYS), "left_semi",
    ).withColumn("target_date", F.lit(target_date)).select(
        "source_snapshot_id", "feature_version", F.lit("scoring").alias("feature_role"),
        *_KEYS, "dept_id", "target_date", "as_of_date",
        *[F.col(f"lag_{days}") for days in LAG_DAYS],
        *[F.col(f"rolling_mean_{days}") for days in MEAN_DAYS],
        *calendar_features(), "price_missing", "last_known_price",
    )
    eligibility, coverage = _diagnostics(classified)
    return ScoringFeatureResult(features=features, eligibility=eligibility, coverage=coverage)


__all__ = [
    "AS_OF_HISTORY_SCHEMA_V1", "HISTORY_ENTRY_SCHEMA_V1", "SCORING_FEATURE_SCHEMA_V1",
    "INTERNAL_REPRESENTATION_VERSION", "OBSERVED_ORIGIN", "PREDICTED_ORIGIN",
    "AsOfHistoryResult", "ScoringFeatureResult", "ScoringFeatureError",
    "prepare_as_of_history", "build_scoring_features",
]
