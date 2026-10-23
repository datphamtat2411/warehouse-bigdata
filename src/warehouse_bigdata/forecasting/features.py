"""Point-in-time historical FORECAST_FEATURES v1.0 construction.

The caller's feature_version identifies these approved transformation semantics:
recorded events are disabled (False means no *verified known scheduled event*),
and a price observed on date d is assumed available at the close of d, usable
only for later targets. This is a historical simulation assumption, not verified
real-world publication timing. Neither weekly membership nor recorded event
names establish advance availability. Changing these policies requires an
explicit feature-version decision.

Historical validation/test rows are one-step examples, not simultaneous
seven-day backtest inputs. Labels remain observed M5 unit sales; the feature
dataset retains the catalog's derived_leakage_controlled_features provenance.
No missing sales observation is imputed, including missing target dates.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import reduce
from operator import or_
from typing import TYPE_CHECKING, Final

from warehouse_bigdata.contracts.datasets import FORECAST_FEATURES, SALES_ENRICHED

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame
    from pyspark.sql.window import WindowSpec


LAG_DAYS: Final = (1, 7, 28)
MEAN_DAYS: Final = (7, 28)
LOOKBACK_DAYS: Final = 28
TRAIN_DAYS: Final = (29, 1_885)
VALIDATION_DAYS: Final = (1_886, 1_913)
TEST_DAYS: Final = (1_914, 1_941)
ISO_WEEKEND_DAYS: Final = (6, 7)
_MAX_SAMPLES: Final = 8
_MAX_DOUBLE: Final = 1.7976931348623157e308
_SERIES_KEYS: Final = ("item_id", "store_id")
_METADATA: Final = ("dept_id", "cat_id", "state_id", "series_id")
_INPUT_TYPES: Final = (
    *("string",) * 7, "int", "date", "int", "bigint", "double", "boolean",
    "int", "int", "int", "boolean", "boolean", *("string",) * 4,
)
_FEATURE_TYPES: Final = (
    *("string",) * 6, "date", "date", "bigint", "bigint", "bigint",
    "double", "double", "int", "int", "boolean", "boolean", "boolean",
    "double", "bigint",
)


class HistoricalFeatureError(ValueError):
    """Invalid configuration, corrupt observations, or invalid feature output."""


@dataclass(frozen=True, slots=True)
class HistoricalFeatureResult:
    """Features and separate distributed eligibility/coverage diagnostics.

    Eligibility has one row per supplied observation, with lineage, item/store,
    target_date, day_index, nullable feature_role, status, prior_day_count and
    missing_day_count. Counts refer to the preceding 28 calendar days, even for
    lookback-only rows. Status precedence is LOOKBACK_ONLY, INSUFFICIENT_HISTORY,
    DISCONTINUOUS_HISTORY, then ELIGIBLE.

    Coverage groups by lineage, feature_role and status. observed_count includes
    all supplied rows; candidate_count excludes LOOKBACK_ONLY. eligible_count
    plus excluded_count equals candidate_count. price_missing_count counts
    missing *as-of* prices among all observations in that group, including
    context/excluded rows. Empty input has empty diagnostics and features.
    """

    features: DataFrame
    eligibility: DataFrame
    coverage: DataFrame


def build_historical_features(
    sales_enriched: DataFrame,
    *,
    source_snapshot_id: str,
    feature_version: str,
) -> HistoricalFeatureResult:
    """Build historical features from one caller-supplied SALES_ENRICHED snapshot.

    Validation is eager and distributed; only scalar aggregates and bounded
    failure samples reach the driver. Returned DataFrames use normal Spark
    execution semantics, without persistence, filesystem or manifest access.
    The input's authoritative M04 calendar mapping is trusted after consistency
    checks; raw calendar data and publication timestamps are not available here.
    """
    from pyspark.sql import functions as F

    for name, value in (
        ("source_snapshot_id", source_snapshot_id), ("feature_version", feature_version)
    ):
        if not isinstance(value, str) or not value.strip():
            raise HistoricalFeatureError(f"{name} must be an explicit nonblank string")

    calendar_offset = _validate_input(sales_enriched)
    history = _with_history(sales_enriched)
    classified = (
        history.withColumn("source_snapshot_id", F.lit(source_snapshot_id))
        .withColumn("feature_version", F.lit(feature_version))
        .withColumn("feature_role", _historical_role(F.col("day_index")))
        .withColumn("target_date", F.col("date"))
        .withColumn("missing_day_count", F.lit(LOOKBACK_DAYS) - F.col("prior_day_count"))
        .withColumn(
            "status",
            F.when(F.col("day_index") < TRAIN_DAYS[0], F.lit("LOOKBACK_ONLY"))
            .when(
                F.col("_day_ordinal") - F.col("_first_day_ordinal") < LOOKBACK_DAYS,
                F.lit("INSUFFICIENT_HISTORY"),
            )
            .when(F.col("prior_day_count") < LOOKBACK_DAYS, F.lit("DISCONTINUOUS_HISTORY"))
            .otherwise(F.lit("ELIGIBLE")),
        )
        .withColumn("_as_of_price_missing", F.col("last_known_price").isNull())
    )
    features = classified.where(F.col("status") == "ELIGIBLE").select(
        "source_snapshot_id", "feature_version", "feature_role",
        "item_id", "store_id", "dept_id", "target_date",
        F.date_sub("target_date", 1).alias("as_of_date"),
        *[F.col(f"lag_{days}") for days in LAG_DAYS],
        *[F.col(f"rolling_mean_{days}") for days in MEAN_DAYS],
        *calendar_features(),
        F.col("_as_of_price_missing").alias("price_missing"),
        "last_known_price", F.col("unit_sales").alias("label"),
    ).select(*FORECAST_FEATURES.field_names)
    _validate_output(features, source_snapshot_id, feature_version, calendar_offset)

    eligibility = classified.select(
        "source_snapshot_id", "feature_version", "item_id", "store_id", "target_date",
        "day_index", "feature_role", "status", "prior_day_count", "missing_day_count",
    )

    def count_if(condition: Column) -> Column:
        return F.sum(F.when(condition, 1).otherwise(0)).cast("long")

    candidate = F.col("feature_role").isNotNull()
    eligible = F.col("status") == "ELIGIBLE"
    coverage = classified.groupBy(
        "source_snapshot_id", "feature_version", "feature_role", "status"
    ).agg(
        F.count(F.lit(1)).alias("observed_count"),
        count_if(candidate).alias("candidate_count"),
        count_if(eligible).alias("eligible_count"),
        count_if(candidate & ~eligible).alias("excluded_count"),
        count_if(F.col("_as_of_price_missing")).alias("price_missing_count"),
    )
    return HistoricalFeatureResult(features=features, eligibility=eligibility, coverage=coverage)


def _day_ordinal(date_column: Column) -> Column:
    """Numeric calendar-day ordering, independent of observed row positions."""
    from pyspark.sql import functions as F

    return F.datediff(date_column, F.lit("1970-01-01").cast("date"))


def _history_window(start: int, end: int = -1) -> WindowSpec:
    from pyspark.sql import Window

    return Window.partitionBy(*_SERIES_KEYS).orderBy("_day_ordinal").rangeBetween(start, end)


def _with_history(sales_enriched: DataFrame) -> DataFrame:
    """Reusable native date-range demand and strictly prior known-price expressions."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    dated = sales_enriched.withColumn("_day_ordinal", _day_ordinal(F.col("date")))
    return dated.select(
        "*",
        F.min("_day_ordinal").over(_history_window(Window.unboundedPreceding, 0))
        .alias("_first_day_ordinal"),
        F.count(F.lit(1)).over(_history_window(-LOOKBACK_DAYS)).alias("prior_day_count"),
        *[
            F.max("unit_sales").over(_history_window(-days, -days)).alias(f"lag_{days}")
            for days in LAG_DAYS
        ],
        *[
            F.avg("unit_sales").over(_history_window(-days)).alias(f"rolling_mean_{days}")
            for days in MEAN_DAYS
        ],
        F.last("sell_price", ignorenulls=True)
        .over(_history_window(Window.unboundedPreceding)).alias("last_known_price"),
    )


def calendar_features() -> tuple[Column, ...]:
    """Shared target_date predictors: ISO weekdays and disabled recorded events.

    A recorded M5 event is not evidence of advance availability. Historical and
    as-of scoring callers must use the same feature-version policy here.
    """
    from pyspark.sql import functions as F

    iso_weekday = F.pmod(F.dayofweek("target_date") + 5, F.lit(7)) + 1
    return (
        iso_weekday.cast("int").alias("day_of_week"),
        F.month("target_date").alias("month"),
        iso_weekday.isin(*ISO_WEEKEND_DAYS).alias("is_weekend"),
        F.lit(False).alias("event_flag"),
    )


def _historical_role(day_index: Column) -> Column:
    from pyspark.sql import functions as F

    return (
        F.when(day_index.between(*TRAIN_DAYS), F.lit("train"))
        .when(day_index.between(*VALIDATION_DAYS), F.lit("validation"))
        .when(day_index.between(*TEST_DAYS), F.lit("test"))
        .otherwise(F.lit(None).cast("string"))
    )


def _require_schema(dataframe: DataFrame, fields: tuple[str, ...], types: tuple[str, ...]) -> None:
    if tuple(dataframe.columns) != fields:
        raise HistoricalFeatureError(f"Expected exact ordered fields {fields}")
    for field, expected in zip(dataframe.schema, types, strict=True):
        if field.dataType.simpleString() != expected:
            raise HistoricalFeatureError(f"{field.name} must have Spark type {expected}")


def _any(conditions) -> Column:
    return reduce(or_, conditions)


def _invalid_price(price: Column) -> Column:
    from pyspark.sql import functions as F

    return price.isNotNull() & (F.isnan(price) | (price <= 0.0) | (price > _MAX_DOUBLE))


def _raise_for_rows(dataframe: DataFrame, condition: Column, message: str) -> None:
    """Bound diagnostic retrieval even when the invalid dataset is large."""
    from pyspark.sql import functions as F

    samples = dataframe.where(F.coalesce(condition, F.lit(False))).limit(_MAX_SAMPLES).collect()
    if samples:
        raise HistoricalFeatureError(f"{message}; samples={[row.asDict() for row in samples]}")


def _validate_input(sales: DataFrame) -> int | None:
    from pyspark.sql import functions as F

    _require_schema(sales, SALES_ENRICHED.field_names, _INPUT_TYPES)
    invalid = _any([
        *[F.col(name).isNull() for name in SALES_ENRICHED.required_field_names],
        *[F.col(name).rlike(r"^\s*$") for name in (*_SERIES_KEYS, *_METADATA)],
        ~F.col("state_id").isin("CA", "TX", "WI"),
        ~F.col("day_index").between(1, TEST_DAYS[1]),
        F.col("day_key") != F.concat(F.lit("d_"), F.col("day_index").cast("string")),
        F.col("unit_sales") < 0,
        F.col("wm_yr_wk") < 0,
        _invalid_price(F.col("sell_price")),
        F.col("price_missing") != F.col("sell_price").isNull(),
        F.col("is_zero_sale") != (F.col("unit_sales") == 0),
        F.col("wday") != F.pmod(F.dayofweek("date"), F.lit(7)) + 1,
        F.col("month") != F.month("date"),
        F.col("year") != F.year("date"),
        F.col("year") < 1,
        *[F.col(name).rlike(r"^\s*$") for name in (
            "event_name_1", "event_type_1", "event_name_2", "event_type_2"
        )],
    ])
    # A single global offset proves one-to-one mapping AND calendar-day spacing,
    # including gaps and mappings observed in different series. No dense calendar
    # or single-partition global ordering is needed.
    offset = _day_ordinal(F.col("date")).cast("long") - F.col("day_index").cast("long")
    metrics = sales.agg(
        F.max(F.when(invalid, 1).otherwise(0)).alias("invalid"),
        F.min(offset).alias("minimum_offset"),
        F.max(offset).alias("maximum_offset"),
    ).first()
    if metrics.invalid:
        _raise_for_rows(sales, invalid, "Invalid SALES_ENRICHED observation")
    if metrics.minimum_offset != metrics.maximum_offset:
        _raise_for_rows(
            sales, offset != F.lit(metrics.minimum_offset),
            "Inconsistent day-index/date mapping or calendar-day spacing",
        )

    series = sales.groupBy(*_SERIES_KEYS).agg(
        F.count(F.lit(1)).alias("observed"),
        F.countDistinct("date").alias("dates"),
        F.countDistinct("day_index").alias("days"),
        F.countDistinct(F.struct(*_METADATA)).alias("metadata_versions"),
    )
    invalid_series = series.where(
        (F.col("observed") != F.col("dates"))
        | (F.col("observed") != F.col("days"))
        | (F.col("metadata_versions") != 1)
    ).select(
        F.lit("duplicate grain or unstable metadata").alias("issue"),
        F.to_json(F.struct("*")).alias("details"),
    )
    ambiguous_ids = sales.groupBy("series_id").agg(
        F.countDistinct(F.struct(*_SERIES_KEYS)).alias("series_count")
    ).where(F.col("series_count") != 1).select(
        F.lit("ambiguous series_id").alias("issue"),
        F.to_json(F.struct("*")).alias("details"),
    )
    _raise_for_rows(
        invalid_series.unionByName(ambiguous_ids), F.lit(True),
        "Invalid SALES_ENRICHED grain or series identity",
    )
    return metrics.minimum_offset


def _validate_output(
    features: DataFrame,
    source_snapshot_id: str,
    feature_version: str,
    calendar_offset: int | None,
) -> None:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    _require_schema(features, FORECAST_FEATURES.field_names, _FEATURE_TYPES)
    scope = ("source_snapshot_id", "feature_version", "feature_role", *FORECAST_FEATURES.unique_key)
    checked = features.withColumn("_scope_count", F.count(F.lit(1)).over(Window.partitionBy(*scope)))
    expected_day = _day_ordinal(F.col("target_date")).cast("long") - F.lit(calendar_offset)
    expected_role = _historical_role(expected_day)
    invalid = _any([
        *[F.col(name).isNull() for name in (*FORECAST_FEATURES.required_field_names, "label")],
        *[F.col(name).rlike(r"^\s*$") for name in (
            "source_snapshot_id", "feature_version", "feature_role", "item_id", "store_id", "dept_id"
        )],
        F.col("source_snapshot_id") != source_snapshot_id,
        F.col("feature_version") != feature_version,
        ~F.col("feature_role").eqNullSafe(expected_role),
        ~expected_day.between(TRAIN_DAYS[0], TEST_DAYS[1]),
        F.col("as_of_date") != F.date_sub("target_date", 1),
        *[F.col(f"lag_{days}") < 0 for days in LAG_DAYS],
        *[
            (F.col(f"rolling_mean_{days}") < 0)
            | F.isnan(f"rolling_mean_{days}")
            | (F.col(f"rolling_mean_{days}") > _MAX_DOUBLE)
            for days in MEAN_DAYS
        ],
        F.col("label") < 0,
        ~F.col("day_of_week").between(1, 7),
        F.col("day_of_week") != F.pmod(F.dayofweek("target_date") + 5, F.lit(7)) + 1,
        F.col("month") != F.month("target_date"),
        F.col("is_weekend") != F.col("day_of_week").isin(*ISO_WEEKEND_DAYS),
        F.col("event_flag"),
        _invalid_price(F.col("last_known_price")),
        F.col("price_missing") != F.col("last_known_price").isNull(),
        F.col("_scope_count") != 1,
    ])
    _raise_for_rows(checked, invalid, "Historical FORECAST_FEATURES invariant violation")


__all__ = [
    "HistoricalFeatureError", "HistoricalFeatureResult", "build_historical_features",
    "calendar_features",
]
