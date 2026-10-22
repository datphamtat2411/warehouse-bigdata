"""Pure Spark aggregation of canonical M5 daily sales observations."""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal
from functools import reduce
from operator import or_
from typing import TYPE_CHECKING, Final, NamedTuple

from warehouse_bigdata.contracts.datasets import (
    CATEGORY_MONTHLY_SALES,
    SALES_DAILY,
    STORE_DAILY_SALES,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


_MAX_DIAGNOSTIC_SAMPLES: Final = 8
_LONG_MAX: Final = 9_223_372_036_854_775_807
_SALES_FIELDS: Final = SALES_DAILY.field_names
_STORE_FIELDS: Final = STORE_DAILY_SALES.field_names
_CATEGORY_FIELDS: Final = CATEGORY_MONTHLY_SALES.field_names
_SALES_IDENTIFIER_FIELDS: Final = (
    "item_id",
    "dept_id",
    "cat_id",
    "store_id",
    "state_id",
    "series_id",
)


class SalesAggregationError(ValueError):
    """Raised when sales input or aggregate output violates the task contract."""


class SalesAggregationResult(NamedTuple):
    """Named aggregate outputs for one source snapshot scope."""

    store_daily_sales: DataFrame
    category_monthly_sales: DataFrame


def aggregate_sales(
    sales_daily: DataFrame,
    *,
    source_snapshot_id: str,
) -> SalesAggregationResult:
    """Aggregate one validated ``SALES_DAILY`` snapshot without changing observations."""

    _validate_snapshot_id(source_snapshot_id)
    _validate_sales_daily(sales_daily)
    source_total = _decimal_total(sales_daily, "unit_sales", "SALES_DAILY")

    store_daily_sales = _build_store_daily_sales(sales_daily, source_snapshot_id)
    category_monthly_sales = _build_category_monthly_sales(sales_daily, source_snapshot_id)

    _validate_store_daily_sales(
        store_daily_sales,
        sales_daily,
        source_snapshot_id=source_snapshot_id,
        source_total=source_total,
    )
    _validate_category_monthly_sales(
        category_monthly_sales,
        sales_daily,
        source_snapshot_id=source_snapshot_id,
        source_total=source_total,
    )
    return SalesAggregationResult(store_daily_sales, category_monthly_sales)


def validate_store_daily_sales(
    store_daily_sales: DataFrame,
    sales_daily: DataFrame,
    *,
    source_snapshot_id: str,
) -> None:
    """Validate a store aggregate against its complete scoped ``SALES_DAILY`` input."""

    _validate_snapshot_id(source_snapshot_id)
    _validate_sales_daily(sales_daily)
    _validate_store_daily_sales(
        store_daily_sales,
        sales_daily,
        source_snapshot_id=source_snapshot_id,
        source_total=_decimal_total(sales_daily, "unit_sales", "SALES_DAILY"),
    )


def validate_category_monthly_sales(
    category_monthly_sales: DataFrame,
    sales_daily: DataFrame,
    *,
    source_snapshot_id: str,
) -> None:
    """Validate the category/month Contract v1 aggregate against scoped source facts."""

    _validate_snapshot_id(source_snapshot_id)
    _validate_sales_daily(sales_daily)
    _validate_category_monthly_sales(
        category_monthly_sales,
        sales_daily,
        source_snapshot_id=source_snapshot_id,
        source_total=_decimal_total(sales_daily, "unit_sales", "SALES_DAILY"),
    )


def _build_store_daily_sales(sales_daily: DataFrame, source_snapshot_id: str) -> DataFrame:
    from pyspark.sql import functions as F
    from pyspark.sql.types import LongType

    aggregated = sales_daily.groupBy("store_id", "date").agg(
        F.sum(_decimal_units("unit_sales")).alias("_total_units_decimal"),
        F.countDistinct("series_id").cast(LongType()).alias("observed_series_count"),
    )
    _validate_integer_aggregate_range(
        aggregated,
        "_total_units_decimal",
        ("store_id", "date", "_total_units_decimal"),
        "store_daily_sales total_units",
    )
    return aggregated.select(
        F.lit(source_snapshot_id).alias("source_snapshot_id"),
        F.col("store_id"),
        F.col("date"),
        F.col("_total_units_decimal").cast(LongType()).alias("total_units"),
        F.col("observed_series_count"),
    ).select(*_STORE_FIELDS)


def _build_category_monthly_sales(sales_daily: DataFrame, source_snapshot_id: str) -> DataFrame:
    from pyspark.sql import functions as F
    from pyspark.sql.types import LongType

    with_month = sales_daily.withColumn("_month_start", F.trunc(F.col("date"), "month"))
    aggregated = with_month.groupBy("cat_id", "_month_start").agg(
        F.sum(_decimal_units("unit_sales")).alias("_total_units_decimal"),
    )
    _validate_integer_aggregate_range(
        aggregated,
        "_total_units_decimal",
        ("cat_id", "_month_start", "_total_units_decimal"),
        "category_monthly_sales total_units",
    )
    return aggregated.select(
        F.lit(source_snapshot_id).alias("source_snapshot_id"),
        F.col("cat_id"),
        F.col("_month_start").alias("month_start"),
        F.col("_total_units_decimal").cast(LongType()).alias("total_units"),
    ).select(*_CATEGORY_FIELDS)


def _validate_store_daily_sales(
    store_daily_sales: DataFrame,
    sales_daily: DataFrame,
    *,
    source_snapshot_id: str,
    source_total: Decimal,
) -> None:
    from pyspark.sql.types import DateType, LongType, StringType

    _validate_output_schema(
        store_daily_sales,
        _STORE_FIELDS,
        (
            StringType(),
            StringType(),
            DateType(),
            LongType(),
            LongType(),
        ),
        "STORE_DAILY_SALES",
    )
    _validate_output_rows(
        store_daily_sales,
        identifier_fields=("source_snapshot_id", "store_id"),
        numeric_fields=("total_units", "observed_series_count"),
        source_snapshot_id=source_snapshot_id,
        label="STORE_DAILY_SALES",
    )

    expected = _expected_store_values(sales_daily)
    _validate_output_keys(
        store_daily_sales,
        expected,
        ("store_id", "date"),
        "STORE_DAILY_SALES",
    )
    _validate_store_values(store_daily_sales, expected)
    _validate_reconciled_total(
        store_daily_sales,
        source_total,
        "STORE_DAILY_SALES",
    )


def _validate_category_monthly_sales(
    category_monthly_sales: DataFrame,
    sales_daily: DataFrame,
    *,
    source_snapshot_id: str,
    source_total: Decimal,
) -> None:
    from pyspark.sql.types import DateType, LongType, StringType

    _validate_output_schema(
        category_monthly_sales,
        _CATEGORY_FIELDS,
        (StringType(), StringType(), DateType(), LongType()),
        "category_monthly_sales",
    )
    _validate_output_rows(
        category_monthly_sales,
        identifier_fields=("source_snapshot_id", "cat_id"),
        numeric_fields=("total_units",),
        source_snapshot_id=source_snapshot_id,
        label="category_monthly_sales",
    )

    expected = _expected_category_values(sales_daily)
    _validate_output_keys(
        category_monthly_sales,
        expected,
        ("cat_id", "month_start"),
        "category_monthly_sales",
    )
    _validate_category_values(category_monthly_sales, expected)
    _validate_reconciled_total(
        category_monthly_sales,
        source_total,
        "category_monthly_sales",
    )


def _validate_output_schema(
    dataframe: DataFrame,
    expected_fields: Sequence[str],
    expected_types: Sequence[object],
    label: str,
) -> None:
    actual_fields = tuple(dataframe.columns)
    expected_fields = tuple(expected_fields)
    if actual_fields != expected_fields:
        raise SalesAggregationError(
            f"{label} fields must be exactly {expected_fields}; received {actual_fields}"
        )
    for field, expected_type in zip(dataframe.schema.fields, expected_types, strict=True):
        if field.dataType != expected_type:
            raise SalesAggregationError(
                f"{label} field {field.name!r} must have type {expected_type.simpleString()}; "
                f"received {field.dataType.simpleString()}"
            )


def _validate_output_rows(
    dataframe: DataFrame,
    *,
    identifier_fields: Sequence[str],
    numeric_fields: Sequence[str],
    source_snapshot_id: str,
    label: str,
) -> None:
    from pyspark.sql import functions as F

    null_fields = (*identifier_fields, *numeric_fields)
    _raise_for_rows(
        dataframe,
        reduce(or_, (F.col(field).isNull() for field in null_fields)),
        null_fields,
        f"null {label} field",
    )
    for field in identifier_fields:
        _raise_for_rows(
            dataframe,
            F.trim(F.col(field)) == F.lit(""),
            (field,),
            f"blank {label} identifier {field}",
        )
    _raise_for_rows(
        dataframe,
        F.col("source_snapshot_id") != F.lit(source_snapshot_id),
        ("source_snapshot_id",),
        f"inconsistent {label} source snapshot",
    )
    _raise_for_rows(
        dataframe,
        reduce(or_, (F.col(field) < F.lit(0) for field in numeric_fields)),
        numeric_fields,
        f"negative {label} quantity",
    )


def _validate_output_keys(
    dataframe: DataFrame,
    expected: DataFrame,
    keys: Sequence[str],
    label: str,
) -> None:
    from pyspark.sql import functions as F

    duplicate_keys = dataframe.groupBy(*keys).count().where(F.col("count") > F.lit(1))
    _raise_for_rows(duplicate_keys, F.lit(True), (*keys, "count"), f"duplicate {label} key")

    actual_keys = dataframe.select(*keys)
    expected_keys = expected.select(*keys)
    _raise_for_rows(
        expected_keys.join(actual_keys, list(keys), "left_anti"),
        F.lit(True),
        keys,
        f"missing {label} key",
    )
    _raise_for_rows(
        actual_keys.join(expected_keys, list(keys), "left_anti"),
        F.lit(True),
        keys,
        f"unexpected {label} key",
    )

    expected_count = expected_keys.count()
    actual_count = dataframe.count()
    if actual_count != expected_count:
        raise SalesAggregationError(
            f"{label} row count mismatch: expected {expected_count}, received {actual_count}"
        )


def _validate_store_values(store_daily_sales: DataFrame, expected: DataFrame) -> None:
    from pyspark.sql import functions as F

    comparison = _join_output_to_expected(
        store_daily_sales,
        expected,
        ("store_id", "date"),
        ("total_units", "observed_series_count"),
    )
    mismatched = comparison.where(
        (F.col("actual_total_units") != F.col("expected_total_units"))
        | (F.col("actual_observed_series_count") != F.col("expected_observed_series_count"))
    )
    _raise_for_rows(
        mismatched,
        F.lit(True),
        (
            "store_id",
            "date",
            "actual_total_units",
            "expected_total_units",
            "actual_observed_series_count",
            "expected_observed_series_count",
        ),
        "STORE_DAILY_SALES value mismatch",
    )


def _validate_category_values(category_monthly_sales: DataFrame, expected: DataFrame) -> None:
    from pyspark.sql import functions as F

    comparison = _join_output_to_expected(
        category_monthly_sales,
        expected,
        ("cat_id", "month_start"),
        ("total_units",),
    )
    mismatched = comparison.where(F.col("actual_total_units") != F.col("expected_total_units"))
    _raise_for_rows(
        mismatched,
        F.lit(True),
        ("cat_id", "month_start", "actual_total_units", "expected_total_units"),
        "category_monthly_sales value mismatch",
    )


def _join_output_to_expected(
    actual: DataFrame,
    expected: DataFrame,
    keys: Sequence[str],
    values: Sequence[str],
) -> DataFrame:
    from pyspark.sql import functions as F

    actual_alias = actual.alias("actual")
    expected_alias = expected.alias("expected")
    join_condition = [F.col(f"actual.{key}") == F.col(f"expected.{key}") for key in keys]
    selections = [F.col(f"actual.{key}").alias(key) for key in keys]
    for field in values:
        selections.append(F.col(f"actual.{field}").cast("decimal(38,0)").alias(f"actual_{field}"))
        selections.append(F.col(f"expected._expected_{field}").alias(f"expected_{field}"))
    return actual_alias.join(expected_alias, join_condition, "inner").select(*selections)


def _expected_store_values(sales_daily: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F

    return (
        sales_daily.groupBy("store_id", "date")
        .agg(
            F.sum(_decimal_units("unit_sales")).alias("_expected_total_units"),
            F.countDistinct("series_id").cast("long").alias("_expected_observed_series_count"),
        )
        .select(
            "store_id",
            "date",
            F.col("_expected_total_units"),
            F.col("_expected_observed_series_count"),
        )
    )


def _expected_category_values(sales_daily: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F

    return (
        sales_daily.withColumn("month_start", F.trunc(F.col("date"), "month"))
        .groupBy("cat_id", "month_start")
        .agg(
            F.sum(_decimal_units("unit_sales")).alias("_expected_total_units"),
        )
        .select("cat_id", "month_start", F.col("_expected_total_units"))
    )


def _validate_reconciled_total(
    aggregate: DataFrame,
    source_total: Decimal,
    label: str,
) -> None:
    aggregate_total = _decimal_total(aggregate, "total_units", label)
    if aggregate_total != source_total:
        raise SalesAggregationError(
            f"{label} total mismatch: expected {source_total}, received {aggregate_total}"
        )


def _validate_sales_daily(sales_daily: DataFrame) -> None:
    from pyspark.sql import functions as F
    from pyspark.sql.types import DateType, IntegerType, LongType, StringType

    _validate_output_schema(
        sales_daily,
        _SALES_FIELDS,
        (
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
        ),
        "SALES_DAILY",
    )
    if sales_daily.limit(1).count() == 0:
        raise SalesAggregationError("SALES_DAILY must contain at least one row")

    _raise_for_rows(
        sales_daily,
        reduce(or_, (F.col(field).isNull() for field in _SALES_FIELDS)),
        _SALES_FIELDS,
        "null SALES_DAILY field",
    )
    for field in _SALES_IDENTIFIER_FIELDS:
        _raise_for_rows(
            sales_daily,
            F.trim(F.col(field)) == F.lit(""),
            (field,),
            f"blank SALES_DAILY identifier {field}",
        )

    day_index_text = F.regexp_extract(F.col("day_key"), r"^d_([1-9][0-9]*)$", 1)
    parsed_day_index = day_index_text.cast(IntegerType())
    valid_day = F.coalesce(
        F.col("day_key").rlike(r"^d_[1-9][0-9]*$")
        & F.col("day_index").between(1, _LONG_MAX)
        & parsed_day_index.isNotNull()
        & (F.col("day_index") == parsed_day_index),
        F.lit(False),
    )
    _raise_for_rows(
        sales_daily,
        ~valid_day,
        ("day_key", "day_index"),
        "invalid SALES_DAILY day key or index",
    )
    _raise_for_rows(
        sales_daily,
        (F.col("wm_yr_wk") < F.lit(0)) | (F.col("unit_sales") < F.lit(0)),
        ("wm_yr_wk", "unit_sales"),
        "negative SALES_DAILY numeric value",
    )

    _reject_duplicate_grain(sales_daily, ("item_id", "store_id", "day_key"), "day key")
    _reject_duplicate_grain(sales_daily, ("item_id", "store_id", "date"), "date")

    multiple_series = (
        sales_daily.groupBy("item_id", "store_id")
        .agg(F.countDistinct("series_id").alias("series_count"))
        .where(F.col("series_count") != F.lit(1))
    )
    _raise_for_rows(
        multiple_series,
        F.lit(True),
        ("item_id", "store_id", "series_count"),
        "inconsistent SALES_DAILY series mapping",
    )
    multiple_pairs = (
        sales_daily.groupBy("series_id")
        .agg(F.countDistinct("item_id", "store_id").alias("pair_count"))
        .where(F.col("pair_count") != F.lit(1))
    )
    _raise_for_rows(
        multiple_pairs,
        F.lit(True),
        ("series_id", "pair_count"),
        "ambiguous SALES_DAILY series identifier",
    )


def _validate_snapshot_id(source_snapshot_id: str) -> None:
    if not isinstance(source_snapshot_id, str) or not source_snapshot_id.strip():
        raise SalesAggregationError("source_snapshot_id must be a nonempty string")


def _validate_integer_aggregate_range(
    dataframe: DataFrame,
    field: str,
    diagnostic_fields: Sequence[str],
    label: str,
) -> None:
    from pyspark.sql import functions as F
    from pyspark.sql.types import DecimalType

    maximum = F.lit(str(_LONG_MAX)).cast(DecimalType(38, 0))
    _raise_for_rows(
        dataframe,
        F.col(field).isNull() | (F.col(field) < F.lit(0)) | (F.col(field) > maximum),
        diagnostic_fields,
        f"{label} is outside the nonnegative Long range",
    )


def _decimal_units(field: str):
    from pyspark.sql import functions as F
    from pyspark.sql.types import DecimalType

    return F.col(field).cast(DecimalType(38, 0))


def _decimal_total(dataframe: DataFrame, field: str, label: str) -> Decimal:
    from pyspark.sql import functions as F
    from pyspark.sql.types import DecimalType

    value = dataframe.agg(F.sum(F.col(field).cast(DecimalType(38, 0))).alias("total")).first()[
        "total"
    ]
    if value is None:
        raise SalesAggregationError(f"{label} total is not representable as an exact decimal")
    return Decimal(value)


def _reject_duplicate_grain(dataframe: DataFrame, keys: Sequence[str], label: str) -> None:
    from pyspark.sql import functions as F

    duplicates = dataframe.groupBy(*keys).count().where(F.col("count") > F.lit(1))
    _raise_for_rows(duplicates, F.lit(True), (*keys, "count"), f"duplicate SALES_DAILY {label}")


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
        raise SalesAggregationError(f"{category}; samples={values}")


__all__ = [
    "SalesAggregationError",
    "SalesAggregationResult",
    "aggregate_sales",
    "validate_category_monthly_sales",
    "validate_store_daily_sales",
]
