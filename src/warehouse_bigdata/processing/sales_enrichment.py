"""Validated Spark enrichment of canonical M5 daily sales."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from functools import reduce
from operator import or_
from typing import TYPE_CHECKING, Final

from warehouse_bigdata.contracts.datasets import SALES_DAILY, SALES_ENRICHED

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


_MAX_DIAGNOSTIC_SAMPLES: Final = 8
_INTEGER_MAX_TEXT: Final = "2147483647"
_CALENDAR_DAY_END: Final = 1_969
_MAX_FINITE_DOUBLE: Final = 1.7976931348623157e308
_KNOWN_STATES: Final = ("CA", "TX", "WI")
_CALENDAR_FIELDS: Final = (
    "date",
    "wm_yr_wk",
    "weekday",
    "wday",
    "month",
    "year",
    "d",
    "event_name_1",
    "event_type_1",
    "event_name_2",
    "event_type_2",
    "snap_CA",
    "snap_TX",
    "snap_WI",
)
_PRICE_FIELDS: Final = ("store_id", "item_id", "wm_yr_wk", "sell_price")
_CALENDAR_REQUIRED_FIELDS: Final = (
    "date",
    "wm_yr_wk",
    "weekday",
    "wday",
    "month",
    "year",
    "d",
    "snap_CA",
    "snap_TX",
    "snap_WI",
)
_CALENDAR_EVENT_FIELDS: Final = (
    "event_name_1",
    "event_type_1",
    "event_name_2",
    "event_type_2",
)
_SALES_FIELDS: Final = SALES_DAILY.field_names
_ENRICHED_FIELDS: Final = SALES_ENRICHED.field_names


class SalesEnrichmentError(ValueError):
    """Raised when sales enrichment inputs or outputs violate Contract v1."""


@dataclass(frozen=True, slots=True)
class SalesEnrichmentMetrics:
    """Distributed scalar quality metrics for one enriched sales result."""

    row_count: int
    unit_sales_total: int
    zero_sales_count: int
    matched_price_count: int
    missing_price_count: int
    price_match_rate: float


def enrich_sales_daily(
    sales_daily: DataFrame,
    calendar: DataFrame,
    sell_prices: DataFrame,
) -> DataFrame:
    """Enrich ``SALES_DAILY`` rows while preserving their grain and observed values."""
    from pyspark.sql import functions as F

    _validate_sales_daily(sales_daily)
    calendar_lookup = _validate_calendar(calendar)
    price_lookup = _validate_prices(sell_prices)

    expected_row_count = sales_daily.count()
    expected_unit_sales_total = _unit_sales_total(sales_daily)

    sales_days = sales_daily.select("day_key").distinct()
    unmatched_days = sales_days.join(
        calendar_lookup.select("calendar_day_key"),
        sales_days.day_key == calendar_lookup.calendar_day_key,
        "left_anti",
    )
    _raise_for_rows(
        unmatched_days,
        F.lit(True),
        ("day_key",),
        "calendar does not cover every SALES_DAILY day",
    )

    sales = sales_daily.alias("sales")
    calendar_alias = calendar_lookup.alias("calendar")
    calendar_enriched = sales.join(
        calendar_alias,
        F.col("sales.day_key") == F.col("calendar.calendar_day_key"),
        "left",
    ).select(
        *[F.col(f"sales.{field}").alias(field) for field in _SALES_FIELDS],
        F.col("calendar.calendar_date").alias("_calendar_date"),
        F.col("calendar.calendar_wm_yr_wk").alias("_calendar_wm_yr_wk"),
        F.col("calendar.wday").alias("_wday"),
        F.col("calendar.month").alias("_month"),
        F.col("calendar.year").alias("_year"),
        F.col("calendar.snap_CA").alias("_snap_CA"),
        F.col("calendar.snap_TX").alias("_snap_TX"),
        F.col("calendar.snap_WI").alias("_snap_WI"),
        *[F.col(f"calendar.{field}").alias(f"_{field}") for field in _CALENDAR_EVENT_FIELDS],
    )
    _raise_for_rows(
        calendar_enriched,
        F.col("_calendar_date").isNull(),
        ("day_key", "date", "wm_yr_wk"),
        "unmatched SALES_DAILY calendar row",
    )
    _raise_for_rows(
        calendar_enriched,
        (F.col("date") != F.col("_calendar_date"))
        | (F.col("wm_yr_wk") != F.col("_calendar_wm_yr_wk")),
        ("day_key", "date", "_calendar_date", "wm_yr_wk", "_calendar_wm_yr_wk"),
        "SALES_DAILY calendar values disagree with the calendar lookup",
    )

    sales_with_price = calendar_enriched.alias("sales").join(
        price_lookup.alias("prices"),
        (F.col("sales.store_id") == F.col("prices.store_id"))
        & (F.col("sales.item_id") == F.col("prices.item_id"))
        & (F.col("sales.wm_yr_wk") == F.col("prices.wm_yr_wk")),
        "left",
    )
    snap_applicable = (
        F.when(F.col("sales.state_id") == F.lit("CA"), F.col("sales._snap_CA"))
        .when(F.col("sales.state_id") == F.lit("TX"), F.col("sales._snap_TX"))
        .when(F.col("sales.state_id") == F.lit("WI"), F.col("sales._snap_WI"))
        .otherwise(F.lit(False))
    )
    enriched = sales_with_price.select(
        *[F.col(f"sales.{field}").alias(field) for field in _SALES_FIELDS],
        F.col("prices.sell_price").alias("sell_price"),
        F.col("prices.sell_price").isNull().alias("price_missing"),
        F.col("sales._wday").alias("wday"),
        F.col("sales._month").alias("month"),
        F.col("sales._year").alias("year"),
        snap_applicable.alias("snap_applicable"),
        (F.col("sales.unit_sales") == F.lit(0)).alias("is_zero_sale"),
        F.col("sales._event_name_1").alias("event_name_1"),
        F.col("sales._event_type_1").alias("event_type_1"),
        F.col("sales._event_name_2").alias("event_name_2"),
        F.col("sales._event_type_2").alias("event_type_2"),
    )
    validate_sales_enriched(enriched, expected_row_count, expected_unit_sales_total)
    return enriched


def validate_sales_enriched(
    sales_enriched: DataFrame,
    expected_row_count: int,
    expected_unit_sales_total: int,
) -> SalesEnrichmentMetrics:
    """Run distributed Contract v1 checks over an enriched sales DataFrame."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        BooleanType,
        DateType,
        DoubleType,
        IntegerType,
        LongType,
        StringType,
    )

    _validate_expected_metrics(expected_row_count, expected_unit_sales_total)
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
        DoubleType(),
        BooleanType(),
        IntegerType(),
        IntegerType(),
        IntegerType(),
        BooleanType(),
        BooleanType(),
        StringType(),
        StringType(),
        StringType(),
        StringType(),
    )
    _validate_exact_columns(sales_enriched, _ENRICHED_FIELDS, "SALES_ENRICHED")
    _validate_types(sales_enriched, _ENRICHED_FIELDS, expected_types, "SALES_ENRICHED")

    required_fields = (
        *_SALES_FIELDS,
        "price_missing",
        "wday",
        "month",
        "year",
        "snap_applicable",
        "is_zero_sale",
    )
    null_condition = reduce(or_, (F.col(field).isNull() for field in required_fields))
    _raise_for_rows(sales_enriched, null_condition, required_fields, "null SALES_ENRICHED field")

    for field in ("item_id", "dept_id", "cat_id", "store_id", "state_id", "series_id"):
        _raise_for_rows(
            sales_enriched,
            F.trim(F.col(field)) == F.lit(""),
            (field,),
            f"blank SALES_ENRICHED identifier {field}",
        )
    _raise_for_rows(
        sales_enriched,
        ~F.col("state_id").isin(list(_KNOWN_STATES)),
        ("state_id",),
        "unknown SALES_ENRICHED state",
    )
    _validate_sales_domains(sales_enriched, "SALES_ENRICHED")
    _raise_for_rows(
        sales_enriched,
        (F.col("wday") < F.lit(1))
        | (F.col("wday") > F.lit(7))
        | (F.col("month") < F.lit(1))
        | (F.col("month") > F.lit(12))
        | (F.col("year") < F.lit(1)),
        ("wday", "month", "year"),
        "invalid SALES_ENRICHED calendar numeric value",
    )
    _reject_duplicate_grain(sales_enriched, ("item_id", "store_id", "day_key"), "day key")
    _reject_duplicate_grain(sales_enriched, ("item_id", "store_id", "date"), "date")

    invalid_price = _invalid_price_expression(F.col("sell_price"), allow_null=True)
    _raise_for_rows(
        sales_enriched,
        invalid_price,
        ("store_id", "item_id", "wm_yr_wk", "sell_price"),
        "invalid SALES_ENRICHED price",
    )
    _raise_for_rows(
        sales_enriched,
        F.col("price_missing") != F.col("sell_price").isNull(),
        ("store_id", "item_id", "wm_yr_wk", "sell_price", "price_missing"),
        "price_missing does not match sell_price nullability",
    )
    _raise_for_rows(
        sales_enriched,
        F.col("is_zero_sale") != (F.col("unit_sales") == F.lit(0)),
        ("store_id", "item_id", "unit_sales", "is_zero_sale"),
        "is_zero_sale does not match unit_sales",
    )
    for field in _CALENDAR_EVENT_FIELDS:
        _raise_for_rows(
            sales_enriched,
            F.trim(F.col(field)) == F.lit(""),
            (field,),
            f"blank nullable calendar event {field}",
        )

    row_count = sales_enriched.count()
    if row_count != expected_row_count:
        raise SalesEnrichmentError(
            f"SALES_ENRICHED cardinality mismatch: expected {expected_row_count}, "
            f"received {row_count}"
        )
    metrics = _sales_enrichment_metrics(sales_enriched)
    if metrics.unit_sales_total != expected_unit_sales_total:
        raise SalesEnrichmentError(
            f"SALES_ENRICHED total mismatch: expected {expected_unit_sales_total}, "
            f"received {metrics.unit_sales_total}"
        )
    return metrics


def price_coverage(sales_enriched: DataFrame) -> DataFrame:
    """Return distributed price coverage grouped by category and retail week."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        BooleanType,
        DateType,
        DoubleType,
        IntegerType,
        LongType,
        StringType,
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
        DoubleType(),
        BooleanType(),
        IntegerType(),
        IntegerType(),
        IntegerType(),
        BooleanType(),
        BooleanType(),
        StringType(),
        StringType(),
        StringType(),
        StringType(),
    )
    _validate_exact_columns(sales_enriched, _ENRICHED_FIELDS, "SALES_ENRICHED")
    _validate_types(sales_enriched, _ENRICHED_FIELDS, expected_types, "SALES_ENRICHED")
    coverage = sales_enriched.groupBy("store_id", "cat_id", "wm_yr_wk").agg(
        F.count(F.lit(1)).alias("row_count"),
        F.sum(F.when(F.col("price_missing"), F.lit(0)).otherwise(F.lit(1)))
        .cast(LongType())
        .alias("matched_price_count"),
        F.sum(F.when(F.col("price_missing"), F.lit(1)).otherwise(F.lit(0)))
        .cast(LongType())
        .alias("missing_price_count"),
    )
    return coverage.withColumn(
        "price_match_rate",
        (F.col("matched_price_count") / F.col("row_count")).cast(DoubleType()),
    )


def _validate_sales_daily(sales_daily: DataFrame) -> None:
    from pyspark.sql import functions as F
    from pyspark.sql.types import DateType, IntegerType, LongType, StringType

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
    _validate_exact_columns(sales_daily, _SALES_FIELDS, "SALES_DAILY")
    _validate_types(sales_daily, _SALES_FIELDS, expected_types, "SALES_DAILY")
    if sales_daily.limit(1).count() == 0:
        raise SalesEnrichmentError("SALES_DAILY must contain at least one row")

    null_condition = reduce(or_, (F.col(field).isNull() for field in _SALES_FIELDS))
    _raise_for_rows(sales_daily, null_condition, _SALES_FIELDS, "null SALES_DAILY field")
    for field in ("item_id", "dept_id", "cat_id", "store_id", "state_id", "series_id"):
        _raise_for_rows(
            sales_daily,
            F.trim(F.col(field)) == F.lit(""),
            (field,),
            f"blank SALES_DAILY identifier {field}",
        )
    _raise_for_rows(
        sales_daily,
        ~F.col("state_id").isin(list(_KNOWN_STATES)),
        ("state_id",),
        "unknown SALES_DAILY state",
    )
    _validate_sales_domains(sales_daily, "SALES_DAILY")
    _reject_duplicate_grain(sales_daily, ("item_id", "store_id", "day_key"), "day key")
    _reject_duplicate_grain(sales_daily, ("item_id", "store_id", "date"), "date")


def _validate_sales_domains(dataframe: DataFrame, label: str) -> None:
    from pyspark.sql import functions as F

    day_key = F.col("day_key")
    day_index_text = F.regexp_extract(day_key, r"^d_([1-9][0-9]*)$", 1)
    valid_day_index = _nonnegative_digits_fit(day_index_text, _INTEGER_MAX_TEXT)
    parsed_day_index = F.expr("try_cast(`day_index` AS INT)")
    valid_day = F.coalesce(
        day_key.rlike(r"^d_[1-9][0-9]*$")
        & valid_day_index
        & parsed_day_index.between(1, 2_147_483_647)
        & (parsed_day_index == day_index_text.cast("int")),
        F.lit(False),
    )
    _raise_for_rows(
        dataframe,
        ~valid_day,
        ("day_key", "day_index"),
        f"invalid {label} day key or index",
    )
    _raise_for_rows(
        dataframe,
        (F.col("wm_yr_wk") < F.lit(0)) | (F.col("unit_sales") < F.lit(0)),
        ("wm_yr_wk", "unit_sales"),
        f"negative {label} numeric value",
    )


def _validate_calendar(calendar: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F
    from pyspark.sql.types import DateType, IntegerType, StringType

    _validate_required_columns(calendar, _CALENDAR_FIELDS, "calendar")
    for field in _CALENDAR_FIELDS:
        if calendar.schema[field].dataType != StringType():
            raise SalesEnrichmentError(f"calendar field {field!r} must be a raw string")
    if calendar.limit(1).count() == 0:
        raise SalesEnrichmentError("calendar must contain at least one day mapping")

    required_nonblank = _CALENDAR_REQUIRED_FIELDS
    for field in required_nonblank:
        _raise_for_rows(
            calendar,
            F.col(field).isNull() | (F.trim(F.col(field)) == F.lit("")),
            (field,),
            f"null or blank calendar field {field}",
        )

    day_key = F.col("d")
    day_index_text = F.regexp_extract(day_key, r"^d_([1-9][0-9]*)$", 1)
    day_index = F.expr("try_cast(regexp_extract(`d`, '^d_([1-9][0-9]*)$', 1) AS INT)")
    valid_day = F.coalesce(
        day_key.rlike(r"^d_[1-9][0-9]*$")
        & _nonnegative_digits_fit(day_index_text, _INTEGER_MAX_TEXT)
        & day_index.between(1, _CALENDAR_DAY_END),
        F.lit(False),
    )
    _raise_for_rows(calendar, ~valid_day, _CALENDAR_FIELDS, "invalid calendar day key")

    date_text = F.col("date")
    parsed_date = F.expr("try_to_timestamp(`date`, 'yyyy-MM-dd')").cast(DateType())
    valid_date = F.coalesce(
        date_text.rlike(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$") & parsed_date.isNotNull(),
        F.lit(False),
    )
    _raise_for_rows(calendar, ~valid_date, _CALENDAR_FIELDS, "invalid calendar date")

    retail_week = F.col("wm_yr_wk")
    parsed_week = F.expr("try_cast(`wm_yr_wk` AS INT)")
    valid_week = F.coalesce(
        retail_week.rlike(r"^[0-9]+$")
        & _nonnegative_digits_fit(retail_week, _INTEGER_MAX_TEXT)
        & parsed_week.between(0, 2_147_483_647),
        F.lit(False),
    )
    _raise_for_rows(calendar, ~valid_week, _CALENDAR_FIELDS, "invalid calendar retail week")

    numeric_fields = {
        "wday": (1, 7),
        "month": (1, 12),
        "year": (1, 2_147_483_647),
    }
    parsed_numeric: dict[str, object] = {}
    for field, (minimum, maximum) in numeric_fields.items():
        parsed = F.expr(f"try_cast(`{field}` AS INT)")
        parsed_numeric[field] = parsed
        valid = F.coalesce(
            F.col(field).rlike(r"^[0-9]+$")
            & _nonnegative_digits_fit(F.col(field), _INTEGER_MAX_TEXT)
            & parsed.between(minimum, maximum),
            F.lit(False),
        )
        _raise_for_rows(calendar, ~valid, (field,), f"invalid calendar {field}")

    parsed_snap: dict[str, object] = {}
    for field in ("snap_CA", "snap_TX", "snap_WI"):
        parsed = F.expr(f"try_cast(`{field}` AS INT)")
        parsed_snap[field] = parsed
        valid = F.coalesce(
            F.col(field).rlike(r"^[0-9]+$") & parsed.isin([0, 1]),
            F.lit(False),
        )
        _raise_for_rows(calendar, ~valid, (field,), f"invalid calendar {field} SNAP value")

    _reject_duplicate_grain(calendar, ("d",), "calendar day")
    _reject_duplicate_grain(calendar, ("date",), "calendar date")

    def nullable_event(field: str):
        trimmed = F.trim(F.col(field))
        return F.when(trimmed == F.lit(""), F.lit(None).cast(StringType())).otherwise(trimmed)

    return calendar.select(
        F.col("d").alias("calendar_day_key"),
        parsed_date.alias("calendar_date"),
        parsed_week.cast(IntegerType()).alias("calendar_wm_yr_wk"),
        parsed_numeric["wday"].cast(IntegerType()).alias("wday"),
        parsed_numeric["month"].cast(IntegerType()).alias("month"),
        parsed_numeric["year"].cast(IntegerType()).alias("year"),
        *[nullable_event(field).alias(field) for field in _CALENDAR_EVENT_FIELDS],
        *[
            (parsed_snap[field] == F.lit(1)).alias(field)
            for field in ("snap_CA", "snap_TX", "snap_WI")
        ],
    )


def _validate_prices(sell_prices: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType, IntegerType, StringType

    _validate_required_columns(sell_prices, _PRICE_FIELDS, "sell_prices")
    for field in _PRICE_FIELDS:
        if sell_prices.schema[field].dataType != StringType():
            raise SalesEnrichmentError(f"sell_prices field {field!r} must be a raw string")

    for field in ("store_id", "item_id", "wm_yr_wk"):
        _raise_for_rows(
            sell_prices,
            F.col(field).isNull() | (F.trim(F.col(field)) == F.lit("")),
            (field,),
            f"null or blank sell_prices key {field}",
        )
    parsed_week = F.expr("try_cast(`wm_yr_wk` AS INT)")
    valid_week = F.coalesce(
        F.col("wm_yr_wk").rlike(r"^[0-9]+$")
        & _nonnegative_digits_fit(F.col("wm_yr_wk"), _INTEGER_MAX_TEXT)
        & parsed_week.between(0, 2_147_483_647),
        F.lit(False),
    )
    _raise_for_rows(
        sell_prices,
        ~valid_week,
        ("store_id", "item_id", "wm_yr_wk"),
        "invalid sell_prices retail week",
    )

    parsed_price = F.expr("try_cast(`sell_price` AS DOUBLE)")
    _raise_for_rows(
        sell_prices,
        _invalid_price_expression(parsed_price),
        ("store_id", "item_id", "wm_yr_wk", "sell_price"),
        "invalid sell_price",
    )

    validated = sell_prices.select(
        F.col("store_id").alias("store_id"),
        F.col("item_id").alias("item_id"),
        parsed_week.cast(IntegerType()).alias("wm_yr_wk"),
        parsed_price.cast(DoubleType()).alias("sell_price"),
    )
    _reject_duplicate_grain(validated, _PRICE_FIELDS[:3], "price key")
    return validated


def _sales_enrichment_metrics(dataframe: DataFrame) -> SalesEnrichmentMetrics:
    from pyspark.sql import functions as F
    from pyspark.sql.types import DecimalType

    values = dataframe.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.sum(F.col("unit_sales").cast(DecimalType(38, 0))).alias("unit_sales_total"),
        F.sum(F.when(F.col("unit_sales") == F.lit(0), F.lit(1)).otherwise(F.lit(0))).alias(
            "zero_sales_count"
        ),
        F.count(F.col("sell_price")).alias("matched_price_count"),
        F.sum(F.when(F.col("price_missing"), F.lit(1)).otherwise(F.lit(0))).alias(
            "missing_price_count"
        ),
    ).first()
    row_count = int(values["row_count"] or 0)
    matched_price_count = int(values["matched_price_count"] or 0)
    return SalesEnrichmentMetrics(
        row_count=row_count,
        unit_sales_total=int(values["unit_sales_total"] or 0),
        zero_sales_count=int(values["zero_sales_count"] or 0),
        matched_price_count=matched_price_count,
        missing_price_count=int(values["missing_price_count"] or 0),
        price_match_rate=(matched_price_count / row_count) if row_count else 0.0,
    )


def _unit_sales_total(dataframe: DataFrame) -> int:
    from pyspark.sql import functions as F
    from pyspark.sql.types import DecimalType

    value = dataframe.agg(
        F.sum(F.col("unit_sales").cast(DecimalType(38, 0))).alias("unit_sales_total")
    ).first()["unit_sales_total"]
    return int(value or 0)


def _invalid_price_expression(column, *, allow_null: bool = False):
    from pyspark.sql import functions as F

    invalid = column.isNotNull() & (
        F.isnan(column) | (column <= F.lit(0.0)) | (column > F.lit(_MAX_FINITE_DOUBLE))
    )
    return invalid if allow_null else invalid | column.isNull()


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
    return F.when(without_leading_zeroes == F.lit(""), F.lit("0")).otherwise(without_leading_zeroes)


def _validate_expected_metrics(expected_row_count: int, expected_unit_sales_total: int) -> None:
    if type(expected_row_count) is not int or expected_row_count < 1:
        raise ValueError("expected_row_count must be a positive integer")
    if type(expected_unit_sales_total) is not int or expected_unit_sales_total < 0:
        raise ValueError("expected_unit_sales_total must be a nonnegative integer")


def _validate_exact_columns(dataframe: DataFrame, expected: Sequence[str], label: str) -> None:
    actual = tuple(dataframe.columns)
    expected_tuple = tuple(expected)
    if len(actual) != len(set(actual)) or actual != expected_tuple:
        missing = [field for field in expected_tuple if field not in actual]
        extra = [field for field in actual if field not in expected_tuple]
        raise SalesEnrichmentError(
            f"{label} columns must be exactly {expected_tuple}; missing={missing}, extra={extra}"
        )


def _validate_required_columns(dataframe: DataFrame, required: Sequence[str], label: str) -> None:
    actual = tuple(dataframe.columns)
    missing = [field for field in required if field not in actual]
    if len(actual) != len(set(actual)) or missing:
        raise SalesEnrichmentError(f"{label} is missing required columns: {missing}")


def _validate_types(
    dataframe: DataFrame,
    fields: Sequence[str],
    expected_types: Sequence[object],
    label: str,
) -> None:
    for field, expected_type in zip(fields, expected_types, strict=True):
        actual_type = dataframe.schema[field].dataType
        if actual_type != expected_type:
            raise SalesEnrichmentError(
                f"{label} field {field!r} must have type {expected_type.simpleString()}; "
                f"received {actual_type.simpleString()}"
            )


def _reject_duplicate_grain(dataframe: DataFrame, keys: Sequence[str], label: str) -> None:
    from pyspark.sql import functions as F

    duplicates = dataframe.groupBy(*keys).count().where(F.col("count") > F.lit(1))
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
        raise SalesEnrichmentError(f"{category}; samples={values}")


__all__ = [
    "SalesEnrichmentError",
    "SalesEnrichmentMetrics",
    "enrich_sales_daily",
    "price_coverage",
    "validate_sales_enriched",
]
