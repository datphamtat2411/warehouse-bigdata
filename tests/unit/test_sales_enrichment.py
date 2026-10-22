"""Focused Spark checks for M04 T02 sales enrichment."""

from __future__ import annotations

import os
from datetime import date

import pytest

from warehouse_bigdata.processing.sales_enrichment import (
    SalesEnrichmentError,
    enrich_sales_daily,
    price_coverage,
    validate_sales_enriched,
)

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

SALES_FIELDS = (
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
ENRICHED_FIELDS = SALES_FIELDS + (
    "sell_price",
    "price_missing",
    "wday",
    "month",
    "year",
    "snap_applicable",
    "is_zero_sale",
    "event_name_1",
    "event_type_1",
    "event_name_2",
    "event_type_2",
)
CALENDAR_FIELDS = (
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
PRICE_FIELDS = ("store_id", "item_id", "wm_yr_wk", "sell_price")


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("warehouse-bigdata-sales-enrichment-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def _sales_frame(spark: SparkSession, rows: list[tuple[object, ...]] | None = None):
    schema = StructType(
        [
            StructField("item_id", StringType(), False),
            StructField("dept_id", StringType(), False),
            StructField("cat_id", StringType(), False),
            StructField("store_id", StringType(), False),
            StructField("state_id", StringType(), False),
            StructField("series_id", StringType(), False),
            StructField("day_key", StringType(), False),
            StructField("day_index", IntegerType(), False),
            StructField("date", DateType(), False),
            StructField("wm_yr_wk", IntegerType(), False),
            StructField("unit_sales", LongType(), False),
        ]
    )
    values = rows or [
        (
            "item_1",
            "dept_1",
            "cat_1",
            "CA_1",
            "CA",
            "item_1_CA_1",
            "d_1",
            1,
            date(2011, 1, 29),
            11101,
            0,
        ),
        (
            "item_1",
            "dept_1",
            "cat_1",
            "TX_1",
            "TX",
            "item_1_TX_1",
            "d_2",
            2,
            date(2011, 1, 30),
            11102,
            2,
        ),
        (
            "item_2",
            "dept_2",
            "cat_2",
            "WI_1",
            "WI",
            "item_2_WI_1",
            "d_3",
            3,
            date(2011, 1, 31),
            11103,
            4,
        ),
    ]
    return spark.createDataFrame(values, schema)


def _calendar_frame(spark: SparkSession, rows: list[tuple[object, ...]] | None = None):
    schema = StructType([StructField(field, StringType(), True) for field in CALENDAR_FIELDS])
    values = rows or [
        (
            "2011-01-29",
            "11101",
            "Saturday",
            "1",
            "1",
            "2011",
            "d_1",
            "Super Bowl",
            "Sporting",
            "",
            "",
            "1",
            "0",
            "0",
        ),
        (
            "2011-01-30",
            "11102",
            "Sunday",
            "2",
            "1",
            "2011",
            "d_2",
            "",
            "",
            "",
            "",
            "0",
            "1",
            "0",
        ),
        (
            "2011-01-31",
            "11103",
            "Monday",
            "3",
            "1",
            "2011",
            "d_3",
            "",
            "",
            "",
            "",
            "0",
            "0",
            "1",
        ),
    ]
    return spark.createDataFrame(values, schema)


def _price_frame(spark: SparkSession, rows: list[tuple[object, ...]] | None = None):
    schema = StructType([StructField(field, StringType(), True) for field in PRICE_FIELDS])
    values = rows or [
        ("CA_1", "item_1", "11101", "2.50"),
        ("WI_1", "item_2", "11103", "1.25"),
    ]
    return spark.createDataFrame(values, schema)


def _enriched(spark: SparkSession):
    return enrich_sales_daily(_sales_frame(spark), _calendar_frame(spark), _price_frame(spark))


def test_enrichment_emits_exact_sales_enriched_v1_and_preserves_sales(spark: SparkSession) -> None:
    result = _enriched(spark)

    assert result.schema.names == list(ENRICHED_FIELDS)
    assert [field.dataType for field in result.schema.fields] == [
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
    ]
    assert result.count() == 3
    assert result.agg(F.sum("unit_sales")).first()[0] == 6
    assert {
        (row.day_key, row.date, row.wm_yr_wk, row.unit_sales)
        for row in result.select("day_key", "date", "wm_yr_wk", "unit_sales").collect()
    } == {
        ("d_1", date(2011, 1, 29), 11101, 0),
        ("d_2", date(2011, 1, 30), 11102, 2),
        ("d_3", date(2011, 1, 31), 11103, 4),
    }


def test_enrichment_selects_state_snap_and_normalizes_nullable_events(
    spark: SparkSession,
) -> None:
    rows = {
        row.store_id: row.asDict()
        for row in _enriched(spark).select(
            "store_id",
            "snap_applicable",
            "event_name_1",
            "event_type_1",
            "event_name_2",
            "event_type_2",
        ).collect()
    }

    assert rows["CA_1"]["snap_applicable"] is True
    assert rows["TX_1"]["snap_applicable"] is True
    assert rows["WI_1"]["snap_applicable"] is True
    assert rows["CA_1"]["event_name_1"] == "Super Bowl"
    assert rows["CA_1"]["event_type_1"] == "Sporting"
    assert rows["CA_1"]["event_name_2"] is None
    assert rows["TX_1"]["event_name_1"] is None


def test_zero_sales_are_retained_and_price_missing_is_nullable_and_flagged(
    spark: SparkSession,
) -> None:
    result = _enriched(spark)
    rows = {
        row.store_id: row
        for row in result.where("store_id = 'CA_1' OR store_id = 'TX_1'").collect()
    }

    assert rows["CA_1"].is_zero_sale is True
    assert rows["CA_1"].unit_sales == 0
    assert rows["CA_1"].sell_price == 2.5
    assert rows["CA_1"].price_missing is False
    assert rows["TX_1"].sell_price is None
    assert rows["TX_1"].price_missing is True


def test_price_coverage_reports_distributed_counts_and_match_rates(spark: SparkSession) -> None:
    coverage = {
        (row.store_id, row.cat_id, row.wm_yr_wk): row.asDict()
        for row in price_coverage(_enriched(spark)).collect()
    }

    assert coverage[("CA_1", "cat_1", 11101)]["row_count"] == 1
    assert coverage[("CA_1", "cat_1", 11101)]["matched_price_count"] == 1
    assert coverage[("CA_1", "cat_1", 11101)]["missing_price_count"] == 0
    assert coverage[("CA_1", "cat_1", 11101)]["price_match_rate"] == 1.0
    assert coverage[("TX_1", "cat_1", 11102)]["price_match_rate"] == 0.0


@pytest.mark.parametrize(
    "mutation",
    [
        lambda rows: [*rows, rows[0]],
        lambda rows: [("2011-02-30", *row[1:]) if row[6] == "d_1" else row for row in rows],
        lambda rows: [row for row in rows if row[6] != "d_3"],
        lambda rows: [
            (*row[:1], "11199", *row[2:]) if row[6] == "d_1" else row for row in rows
        ],
    ],
    ids=["duplicate-day", "invalid-date", "incomplete-coverage", "calendar-mismatch"],
)
def test_invalid_calendar_inputs_fail_before_enrichment(spark: SparkSession, mutation) -> None:
    rows = mutation([tuple(row) for row in _calendar_frame(spark).collect()])
    with pytest.raises(SalesEnrichmentError):
        enrich_sales_daily(_sales_frame(spark), _calendar_frame(spark, rows), _price_frame(spark))


@pytest.mark.parametrize(
    "price_row",
    [
        ("CA_1", "item_1", "11101", "bad"),
        ("CA_1", "item_1", "11101", "NaN"),
        ("CA_1", "item_1", "11101", "Infinity"),
        ("CA_1", "item_1", "11101", "0"),
        ("CA_1", "item_1", "11101", "-1"),
    ],
    ids=["malformed", "nan", "infinity", "zero", "negative"],
)
def test_invalid_price_values_fail(spark: SparkSession, price_row: tuple[str, ...]) -> None:
    with pytest.raises(SalesEnrichmentError):
        enrich_sales_daily(
            _sales_frame(spark),
            _calendar_frame(spark),
            _price_frame(spark, [price_row]),
        )


def test_duplicate_price_keys_fail_and_valid_prices_do_not_multiply_rows(
    spark: SparkSession,
) -> None:
    duplicate = _price_frame(
        spark,
        [
            ("CA_1", "item_1", "11101", "2.50"),
            ("CA_1", "item_1", "11101", "2.75"),
        ],
    )
    with pytest.raises(SalesEnrichmentError):
        enrich_sales_daily(_sales_frame(spark), _calendar_frame(spark), duplicate)

    assert enrich_sales_daily(
        _sales_frame(spark), _calendar_frame(spark), _price_frame(spark)
    ).count() == _sales_frame(spark).count()


@pytest.mark.parametrize(
    "mutation",
    [
        lambda result: result.unionByName(result.limit(1)),
        lambda result: result.withColumn(
            "unit_sales",
            F.when(result.store_id == "CA_1", F.lit(99).cast(LongType())).otherwise(
                result.unit_sales
            ),
        ),
        lambda result: result.withColumn(
            "price_missing",
            F.when(result.store_id == "TX_1", F.lit(False)).otherwise(result.price_missing),
        ),
        lambda result: result.withColumn(
            "is_zero_sale",
            F.when(result.store_id == "CA_1", F.lit(False)).otherwise(result.is_zero_sale),
        ),
    ],
    ids=["duplicate-grain", "tampered-total", "tampered-price-flag", "tampered-zero-flag"],
)
def test_output_validation_rejects_tampered_cardinality_totals_and_flags(
    spark: SparkSession, mutation
) -> None:
    with pytest.raises(SalesEnrichmentError):
        validate_sales_enriched(mutation(_enriched(spark)), 3, 6)


def test_sales_input_validation_rejects_null_negative_and_duplicate_grain(
    spark: SparkSession,
) -> None:
    null_sales = _sales_frame(spark).withColumn(
        "item_id", F.when(F.col("store_id") == "CA_1", F.lit(None)).otherwise(F.col("item_id"))
    )
    with pytest.raises(SalesEnrichmentError):
        enrich_sales_daily(null_sales, _calendar_frame(spark), _price_frame(spark))

    negative_sales = _sales_frame(spark).withColumn(
        "unit_sales", F.when(F.col("store_id") == "CA_1", F.lit(-1)).otherwise(F.col("unit_sales"))
    )
    with pytest.raises(SalesEnrichmentError):
        enrich_sales_daily(negative_sales, _calendar_frame(spark), _price_frame(spark))

    duplicate_sales = _sales_frame(spark).unionByName(_sales_frame(spark).limit(1))
    with pytest.raises(SalesEnrichmentError):
        enrich_sales_daily(duplicate_sales, _calendar_frame(spark), _price_frame(spark))
