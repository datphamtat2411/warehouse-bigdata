"""Pure Spark normalization of the raw M5 evaluation sales table."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from functools import reduce
from operator import or_
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


_EVALUATION_DAY_START: Final = 1
_EVALUATION_DAY_END: Final = 1_941
_CALENDAR_DAY_END: Final = 1_969
_LONG_MAX_TEXT: Final = "9223372036854775807"
_INTEGER_MAX_TEXT: Final = "2147483647"
_MAX_DIAGNOSTIC_SAMPLES: Final = 8
_SALES_METADATA: Final = ("id", "item_id", "dept_id", "cat_id", "store_id", "state_id")
_SALES_DAYS: Final = tuple(f"d_{day}" for day in range(1, _EVALUATION_DAY_END + 1))
_CALENDAR_FIELDS: Final = ("date", "wm_yr_wk", "d")
_OUTPUT_FIELDS: Final = (
    "item_id",
    "dept_id",
    "cat_id",
    "store_id",
    "state_id",
    "series_id",
    "day_key",
    "day_index",
    "date",
    "wm_yr_wk",
    "unit_sales",
)


class NormalizationError(ValueError):
    """Raised when raw input or normalized output violates the M04 contract."""


@dataclass(frozen=True, slots=True)
class SalesNormalizationConfig:
    """Bounded day-range configuration for sales normalization."""

    day_start: int = 1
    day_end: int = _EVALUATION_DAY_END
    days_per_chunk: int = 128

    def __post_init__(self) -> None:
        values = {
            "day_start": self.day_start,
            "day_end": self.day_end,
            "days_per_chunk": self.days_per_chunk,
        }
        if any(type(value) is not int for value in values.values()):
            raise ValueError("day range and chunk width must be integers")
        if not _EVALUATION_DAY_START <= self.day_start <= self.day_end <= _EVALUATION_DAY_END:
            raise ValueError(
                f"day range must satisfy 1 <= day_start <= day_end <= {_EVALUATION_DAY_END}"
            )
        if not 1 <= self.days_per_chunk <= _EVALUATION_DAY_END:
            raise ValueError(
                f"days_per_chunk must satisfy 1 <= days_per_chunk <= {_EVALUATION_DAY_END}"
            )


@dataclass(frozen=True, slots=True)
class NormalizationMetrics:
    """Scalar distributed quality metrics for one normalized result."""

    row_count: int
    series_count: int
    day_count: int
    unit_sales_total: int
    zero_sales_count: int

    @property
    def selected_day_count(self) -> int:
        """Return the selected calendar-day count using an explicit name."""
        return self.day_count


def normalize_sales_daily(
    sales_wide: DataFrame,
    calendar: DataFrame,
    *,
    config: SalesNormalizationConfig | None = None,
) -> DataFrame:
    """Normalize raw evaluation sales into Contract v1 ``SALES_DAILY`` rows.

    The function accepts already-loaded DataFrames only.  It validates source and
    calendar inputs, processes selected days in bounded groups, and validates the
    complete output before returning it to a caller that may persist it.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType, LongType

    selected_config = config or SalesNormalizationConfig()
    _validate_sales_source(sales_wide)
    selected_day_keys = tuple(
        f"d_{day}" for day in range(selected_config.day_start, selected_config.day_end + 1)
    )
    calendar_lookup = _validate_calendar(calendar, selected_day_keys)
    expected_series_count = sales_wide.count()

    normalized_chunks: list[DataFrame] = []
    for start in range(0, len(selected_day_keys), selected_config.days_per_chunk):
        day_columns = selected_day_keys[start : start + selected_config.days_per_chunk]
        projected = sales_wide.select(*_SALES_METADATA, *day_columns)
        unpivoted = projected.unpivot(
            list(_SALES_METADATA),
            list(day_columns),
            "day_key",
            "raw_unit_sales",
        )
        raw_sales = F.col("raw_unit_sales")
        valid_shape = raw_sales.rlike(r"^[0-9]+$")
        fits_long = _nonnegative_digits_fit(raw_sales, _LONG_MAX_TEXT)
        valid_sales = F.coalesce(raw_sales.isNotNull() & valid_shape & fits_long, F.lit(False))
        _raise_for_rows(
            unpivoted,
            ~valid_sales,
            (*_SALES_METADATA, "day_key", "raw_unit_sales"),
            "invalid sales value",
        )
        normalized_chunks.append(
            unpivoted.select(
                *[F.col(field) for field in _SALES_METADATA],
                F.col("day_key"),
                F.when(valid_sales, raw_sales.cast(LongType()))
                .otherwise(F.lit(None).cast(LongType()))
                .alias("unit_sales"),
            )
        )

    normalized_long = reduce(
        lambda left, right: left.unionByName(right), normalized_chunks
    )
    source_metrics = _sales_metrics(normalized_long)
    calendar = F.broadcast(calendar_lookup).alias("calendar")
    sales = normalized_long.alias("sales")
    joined = sales.join(
        calendar,
        F.col("sales.day_key") == F.col("calendar.calendar_day_key"),
        "left",
    )
    sales_daily = joined.select(
        *[F.col(f"sales.{field}") for field in _SALES_METADATA[1:]],
        F.col("sales.id").alias("series_id"),
        F.col("sales.day_key"),
        F.regexp_extract(F.col("sales.day_key"), r"^d_([1-9][0-9]*)$", 1)
        .cast(IntegerType())
        .alias("day_index"),
        F.col("calendar.date"),
        F.col("calendar.wm_yr_wk"),
        F.col("sales.unit_sales"),
    )
    validate_sales_daily(
        sales_daily,
        expected_series_count=expected_series_count,
        selected_day_keys=selected_day_keys,
        expected_unit_sales_total=source_metrics.unit_sales_total,
        expected_zero_sales_count=source_metrics.zero_sales_count,
    )
    return sales_daily


def validate_sales_daily(
    sales_daily: DataFrame,
    *,
    expected_series_count: int,
    selected_day_keys: Sequence[str],
    expected_unit_sales_total: int,
    expected_zero_sales_count: int,
) -> NormalizationMetrics:
    """Run distributed Contract v1 checks over an already-normalized DataFrame."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import DateType, IntegerType, LongType, StringType

    selected_days = tuple(selected_day_keys)
    _validate_quality_arguments(
        expected_series_count,
        selected_days,
        expected_unit_sales_total,
        expected_zero_sales_count,
    )
    expected_types = (
        StringType(),
        StringType(),
        StringType(),
        StringType(),
        StringType(),
        StringType(),
        StringType(),
        IntegerType(),
        DateType(),
        IntegerType(),
        LongType(),
    )
    if tuple(sales_daily.columns) != _OUTPUT_FIELDS:
        raise NormalizationError(
            f"SALES_DAILY schema fields must be exactly {_OUTPUT_FIELDS}; "
            f"received {tuple(sales_daily.columns)}"
        )
    for field, expected_type in zip(sales_daily.schema.fields, expected_types, strict=True):
        if field.dataType != expected_type:
            raise NormalizationError(
                f"SALES_DAILY field {field.name!r} must have type {expected_type.simpleString()}; "
                f"received {field.dataType.simpleString()}"
            )

    null_condition = reduce(
        or_, (F.col(field).isNull() for field in _OUTPUT_FIELDS)
    )
    _raise_for_rows(sales_daily, null_condition, _OUTPUT_FIELDS, "null SALES_DAILY field")

    for field in ("item_id", "dept_id", "cat_id", "store_id", "state_id", "series_id"):
        _raise_for_rows(
            sales_daily,
            F.col(field).rlike(r"^\s*$"),
            (field,),
            f"blank SALES_DAILY identifier {field}",
        )
    _raise_for_rows(
        sales_daily,
        ~F.col("day_key").isin(list(selected_days)),
        ("day_key",),
        "unexpected SALES_DAILY day key",
    )
    expected_index = F.regexp_extract(F.col("day_key"), r"^d_([1-9][0-9]*)$", 1).cast(
        IntegerType()
    )
    _raise_for_rows(
        sales_daily,
        (F.col("day_index") < 1)
        | (F.col("day_index") != expected_index)
        | (F.col("wm_yr_wk") < 0)
        | (F.col("unit_sales") < 0),
        ("day_key", "day_index", "wm_yr_wk", "unit_sales"),
        "SALES_DAILY domain violation",
    )

    _reject_duplicate_grain(sales_daily, ("item_id", "store_id", "day_key"), "day key")
    _reject_duplicate_grain(sales_daily, ("item_id", "store_id", "date"), "date")

    row_count = sales_daily.count()
    expected_row_count = expected_series_count * len(selected_days)
    if row_count != expected_row_count:
        raise NormalizationError(
            f"SALES_DAILY cardinality mismatch: expected {expected_row_count}, received {row_count}"
        )
    series_count = sales_daily.select("series_id").distinct().count()
    if series_count != expected_series_count:
        raise NormalizationError(
            f"SALES_DAILY series cardinality mismatch: expected {expected_series_count}, "
            f"received {series_count}"
        )
    day_count = sales_daily.select("day_key").distinct().count()
    if day_count != len(selected_days):
        raise NormalizationError(
            f"SALES_DAILY day coverage mismatch: expected {len(selected_days)}, received {day_count}"
        )
    uneven_days = (
        sales_daily.groupBy("day_key")
        .count()
        .where(F.col("count") != expected_series_count)
    )
    _raise_for_rows(uneven_days, F.lit(True), ("day_key", "count"), "incomplete day coverage")

    metrics = _sales_metrics(sales_daily)
    if metrics.unit_sales_total != expected_unit_sales_total:
        raise NormalizationError(
            f"SALES_DAILY total mismatch: expected {expected_unit_sales_total}, "
            f"received {metrics.unit_sales_total}"
        )
    if metrics.zero_sales_count != expected_zero_sales_count:
        raise NormalizationError(
            f"SALES_DAILY zero-sales mismatch: expected {expected_zero_sales_count}, "
            f"received {metrics.zero_sales_count}"
        )
    return metrics


def _validate_sales_source(sales_wide: DataFrame) -> None:
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType

    expected_columns = (*_SALES_METADATA, *_SALES_DAYS)
    _validate_exact_columns(sales_wide, expected_columns, "sales source")
    for field in expected_columns:
        if sales_wide.schema[field].dataType != StringType():
            raise NormalizationError(f"sales source field {field!r} must be a raw string")
    if sales_wide.limit(1).count() == 0:
        raise NormalizationError("sales source must contain at least one series")
    for field in _SALES_METADATA:
        _raise_for_rows(
            sales_wide,
            sales_wide[field].isNull() | sales_wide[field].rlike(r"^\s*$"),
            (field,),
            f"invalid source identifier {field}",
        )
    _reject_duplicate_grain(sales_wide, ("id",), "source id")
    _reject_duplicate_grain(sales_wide, ("item_id", "store_id"), "source series")


def _validate_calendar(calendar: DataFrame, selected_day_keys: Sequence[str]) -> DataFrame:
    from pyspark.sql import functions as F
    from pyspark.sql.types import DateType, IntegerType, StringType

    _validate_required_columns(calendar, _CALENDAR_FIELDS, "calendar")
    for field in _CALENDAR_FIELDS:
        if calendar.schema[field].dataType != StringType():
            raise NormalizationError(f"calendar field {field!r} must be a raw string")
    if calendar.limit(1).count() == 0:
        raise NormalizationError("calendar must contain at least one day mapping")

    day_key = F.col("d")
    valid_day_shape = day_key.rlike(r"^d_[1-9][0-9]*$")
    day_index_text = F.regexp_extract(day_key, r"^d_([1-9][0-9]*)$", 1)
    valid_day_index = _nonnegative_digits_fit(day_index_text, _INTEGER_MAX_TEXT)
    canonical_day_index = _canonical_digits(day_index_text)
    calendar_day_limit = str(_CALENDAR_DAY_END)
    valid_calendar_day_range = (F.length(canonical_day_index) < len(calendar_day_limit)) | (
        (F.length(canonical_day_index) == len(calendar_day_limit))
        & (canonical_day_index <= F.lit(calendar_day_limit))
    )
    valid_day = F.coalesce(
        day_key.isNotNull()
        & valid_day_shape
        & valid_day_index
        & valid_calendar_day_range,
        F.lit(False),
    )
    _raise_for_rows(calendar, ~valid_day, _CALENDAR_FIELDS, "invalid calendar day key")

    date_text = F.col("date")
    valid_date_shape = date_text.rlike(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
    parsed_date = F.expr("try_to_timestamp(`date`, 'yyyy-MM-dd')").cast(DateType())
    valid_date = F.coalesce(date_text.isNotNull() & valid_date_shape & parsed_date.isNotNull(), F.lit(False))
    _raise_for_rows(calendar, ~valid_date, _CALENDAR_FIELDS, "invalid calendar date")

    retail_week = F.col("wm_yr_wk")
    valid_week_shape = retail_week.rlike(r"^[0-9]+$")
    valid_week = F.coalesce(
        retail_week.isNotNull()
        & valid_week_shape
        & _nonnegative_digits_fit(retail_week, _INTEGER_MAX_TEXT),
        F.lit(False),
    )
    _raise_for_rows(calendar, ~valid_week, _CALENDAR_FIELDS, "invalid calendar retail week")
    _reject_duplicate_grain(calendar, ("d",), "calendar day")
    _reject_duplicate_grain(calendar, ("date",), "calendar date")

    lookup = calendar.select(
        F.col("d").alias("calendar_day_key"),
        parsed_date.alias("date"),
        F.when(valid_week, retail_week.cast(IntegerType()))
        .otherwise(F.lit(None).cast(IntegerType()))
        .alias("wm_yr_wk"),
    )
    selected_count = lookup.filter(F.col("calendar_day_key").isin(list(selected_day_keys))).select(
        "calendar_day_key"
    ).distinct().count()
    if selected_count != len(tuple(selected_day_keys)):
        raise NormalizationError(
            f"calendar does not cover all selected days: expected {len(tuple(selected_day_keys))}, "
            f"received {selected_count}"
        )
    return lookup


def _validate_exact_columns(dataframe: DataFrame, expected: Sequence[str], label: str) -> None:
    actual = tuple(dataframe.columns)
    expected_tuple = tuple(expected)
    if len(actual) != len(set(actual)) or set(actual) != set(expected_tuple):
        missing = [field for field in expected_tuple if field not in actual]
        extra = [field for field in actual if field not in expected_tuple]
        raise NormalizationError(
            f"{label} columns must match the canonical source; missing={missing}, extra={extra}"
        )


def _validate_required_columns(dataframe: DataFrame, required: Sequence[str], label: str) -> None:
    actual = tuple(dataframe.columns)
    missing = [field for field in required if field not in actual]
    if len(actual) != len(set(actual)) or missing:
        raise NormalizationError(f"{label} is missing required columns: {missing}")


def _nonnegative_digits_fit(column, maximum_text: str):
    from pyspark.sql import functions as F

    canonical = _canonical_digits(column)
    length = F.length(canonical)
    maximum_length = len(maximum_text)
    return (length < maximum_length) | (
        (length == maximum_length) & (canonical <= F.lit(maximum_text))
    )


def _canonical_digits(column):
    from pyspark.sql import functions as F

    without_leading_zeroes = F.regexp_replace(column, r"^0+", "")
    return F.when(without_leading_zeroes == "", F.lit("0")).otherwise(without_leading_zeroes)


def _reject_duplicate_grain(dataframe: DataFrame, keys: Sequence[str], label: str) -> None:
    from pyspark.sql import functions as F

    duplicates = dataframe.groupBy(*keys).count().where(F.col("count") > 1)
    _raise_for_rows(duplicates, F.lit(True), (*keys, "count"), f"duplicate {label}")


def _raise_for_rows(
    dataframe: DataFrame,
    condition,
    fields: Sequence[str],
    category: str,
) -> None:
    from pyspark.sql import functions as F

    invalid = dataframe.where(F.coalesce(condition, F.lit(False)))
    samples = invalid.select(*fields).limit(_MAX_DIAGNOSTIC_SAMPLES).collect()
    if samples:
        values = [row.asDict(recursive=True) for row in samples]
        raise NormalizationError(f"{category}; samples={values}")


def _sales_metrics(dataframe: DataFrame) -> NormalizationMetrics:
    from pyspark.sql import functions as F
    from pyspark.sql.types import DecimalType

    values = dataframe.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.countDistinct("id" if "id" in dataframe.columns else "series_id").alias("series_count"),
        F.sum(F.col("unit_sales").cast(DecimalType(38, 0))).alias("unit_sales_total"),
        F.sum(F.when(F.col("unit_sales") == 0, F.lit(1)).otherwise(F.lit(0))).alias(
            "zero_sales_count"
        ),
        F.countDistinct("day_key").alias("day_count"),
    ).first()
    return NormalizationMetrics(
        row_count=int(values["row_count"] or 0),
        series_count=int(values["series_count"] or 0),
        day_count=int(values["day_count"] or 0),
        unit_sales_total=int(values["unit_sales_total"] or 0),
        zero_sales_count=int(values["zero_sales_count"] or 0),
    )


def _validate_quality_arguments(
    expected_series_count: int,
    selected_day_keys: Sequence[str],
    expected_unit_sales_total: int,
    expected_zero_sales_count: int,
) -> None:
    if type(expected_series_count) is not int or expected_series_count < 1:
        raise ValueError("expected_series_count must be a positive integer")
    if not selected_day_keys or len(set(selected_day_keys)) != len(selected_day_keys):
        raise ValueError("selected_day_keys must be a non-empty unique sequence")
    if any(
        type(day_key) is not str or not _is_evaluation_day_key(day_key)
        for day_key in selected_day_keys
    ):
        raise ValueError("selected_day_keys must contain canonical evaluation day keys")
    if type(expected_unit_sales_total) is not int or expected_unit_sales_total < 0:
        raise ValueError("expected_unit_sales_total must be a nonnegative integer")
    if type(expected_zero_sales_count) is not int or expected_zero_sales_count < 0:
        raise ValueError("expected_zero_sales_count must be a nonnegative integer")


def _is_evaluation_day_key(day_key: str) -> bool:
    if not day_key.startswith("d_"):
        return False
    suffix = day_key[2:]
    if not suffix or not suffix.isascii() or not suffix.isdigit() or suffix.startswith("0"):
        return False
    return len(suffix) < 4 or (len(suffix) == 4 and suffix <= "1941")
