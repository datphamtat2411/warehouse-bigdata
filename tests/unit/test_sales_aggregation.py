"""Focused Spark checks for M04 T03 sales aggregation."""

from __future__ import annotations

import os
from datetime import date

import pytest

from tests.fixtures.loader import load_context
from warehouse_bigdata.contracts.datasets import STORE_DAILY_SALES
from warehouse_bigdata.contracts.validation import validate_dataset
from warehouse_bigdata.processing.sales_aggregation import (
    SalesAggregationError,
    aggregate_sales,
    validate_category_monthly_sales,
    validate_store_daily_sales,
)

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    DateType,
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
STORE_FIELDS = (
    "source_snapshot_id",
    "store_id",
    "date",
    "total_units",
    "observed_series_count",
)
CATEGORY_FIELDS = ("source_snapshot_id", "cat_id", "month_start", "total_units")


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("warehouse-bigdata-sales-aggregation-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def _source_rows() -> list[dict[str, object]]:
    return [
        {
            "item_id": "item_1",
            "dept_id": "dept_1",
            "cat_id": "cat_1",
            "store_id": "store_a",
            "state_id": "CA",
            "series_id": "item_1_store_a",
            "day_key": "d_1",
            "day_index": 1,
            "date": date(2026, 1, 31),
            "wm_yr_wk": 2605,
            "unit_sales": 0,
        },
        {
            "item_id": "item_2",
            "dept_id": "dept_1",
            "cat_id": "cat_1",
            "store_id": "store_a",
            "state_id": "CA",
            "series_id": "item_2_store_a",
            "day_key": "d_1",
            "day_index": 1,
            "date": date(2026, 1, 31),
            "wm_yr_wk": 2605,
            "unit_sales": 3,
        },
        {
            "item_id": "item_1",
            "dept_id": "dept_1",
            "cat_id": "cat_1",
            "store_id": "store_a",
            "state_id": "CA",
            "series_id": "item_1_store_a",
            "day_key": "d_2",
            "day_index": 2,
            "date": date(2026, 2, 1),
            "wm_yr_wk": 2605,
            "unit_sales": 2,
        },
        {
            "item_id": "item_3",
            "dept_id": "dept_2",
            "cat_id": "cat_1",
            "store_id": "store_b",
            "state_id": "TX",
            "series_id": "item_3_store_b",
            "day_key": "d_1",
            "day_index": 1,
            "date": date(2026, 1, 31),
            "wm_yr_wk": 2605,
            "unit_sales": 4,
        },
        {
            "item_id": "item_4",
            "dept_id": "dept_2",
            "cat_id": "cat_2",
            "store_id": "store_b",
            "state_id": "TX",
            "series_id": "item_4_store_b",
            "day_key": "d_2",
            "day_index": 2,
            "date": date(2026, 2, 1),
            "wm_yr_wk": 2605,
            "unit_sales": 0,
        },
        {
            "item_id": "item_5",
            "dept_id": "dept_2",
            "cat_id": "cat_2",
            "store_id": "store_b",
            "state_id": "TX",
            "series_id": "item_5_store_b",
            "day_key": "d_3",
            "day_index": 3,
            "date": date(2026, 2, 2),
            "wm_yr_wk": 2605,
            "unit_sales": 5,
        },
    ]


def _sales_frame(
    spark: SparkSession,
    rows: list[dict[str, object]] | None = None,
    *,
    extra_columns: tuple[str, ...] = (),
):
    fields = [
        StructField("item_id", StringType(), True),
        StructField("dept_id", StringType(), True),
        StructField("cat_id", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("state_id", StringType(), True),
        StructField("series_id", StringType(), True),
        StructField("day_key", StringType(), True),
        StructField("day_index", IntegerType(), True),
        StructField("date", DateType(), True),
        StructField("wm_yr_wk", IntegerType(), True),
        StructField("unit_sales", LongType(), True),
    ]
    fields.extend(StructField(field, StringType(), True) for field in extra_columns)
    columns = [field.name for field in fields]
    source_rows = rows if rows is not None else _source_rows()
    values = [tuple(row.get(column) for column in columns) for row in source_rows]
    return spark.createDataFrame(values, StructType(fields))


def _aggregates(spark: SparkSession, snapshot: str = "snapshot-001"):
    return aggregate_sales(_sales_frame(spark), source_snapshot_id=snapshot)


def test_aggregation_emits_contract_store_rows_and_provisional_category_rows(
    spark: SparkSession,
) -> None:
    result = _aggregates(spark)

    assert result.store_daily_sales.schema.names == list(STORE_FIELDS)
    assert [field.dataType for field in result.store_daily_sales.schema.fields] == [
        StringType(),
        StringType(),
        DateType(),
        LongType(),
        LongType(),
    ]
    assert result.category_monthly_sales.schema.names == list(CATEGORY_FIELDS)
    assert [field.dataType for field in result.category_monthly_sales.schema.fields] == [
        StringType(),
        StringType(),
        DateType(),
        LongType(),
    ]

    store_rows = {
        (row.store_id, row.date): (row.total_units, row.observed_series_count)
        for row in result.store_daily_sales.collect()
    }
    assert store_rows == {
        ("store_a", date(2026, 1, 31)): (3, 2),
        ("store_a", date(2026, 2, 1)): (2, 1),
        ("store_b", date(2026, 1, 31)): (4, 1),
        ("store_b", date(2026, 2, 1)): (0, 1),
        ("store_b", date(2026, 2, 2)): (5, 1),
    }
    assert all(row.source_snapshot_id == "snapshot-001" for row in result.store_daily_sales.collect())
    assert {
        (row.cat_id, row.month_start): row.total_units
        for row in result.category_monthly_sales.collect()
    } == {
        ("cat_1", date(2026, 1, 1)): 7,
        ("cat_1", date(2026, 2, 1)): 2,
        ("cat_2", date(2026, 2, 1)): 5,
    }
    assert result.store_daily_sales.agg(F.sum("total_units")).first()[0] == 14
    assert result.category_monthly_sales.agg(F.sum("total_units")).first()[0] == 14


def test_snapshot_argument_is_required_nonempty_and_never_read_from_source(
    spark: SparkSession,
) -> None:
    sales = _sales_frame(spark)
    with pytest.raises(TypeError):
        aggregate_sales(sales)  # type: ignore[call-arg]

    for snapshot in (None, "", "   ", 7):
        with pytest.raises(SalesAggregationError):
            aggregate_sales(sales, source_snapshot_id=snapshot)  # type: ignore[arg-type]

    with pytest.raises(SalesAggregationError):
        aggregate_sales(
            _sales_frame(spark, extra_columns=("source_snapshot_id",)),
            source_snapshot_id="snapshot-001",
        )


@pytest.mark.parametrize(
    "mutation",
    [
        lambda frame: frame.limit(0),
        lambda frame: frame.withColumn("item_id", F.lit(None).cast(StringType())),
        lambda frame: frame.withColumn("date", F.lit(None).cast(DateType())),
        lambda frame: frame.withColumn("unit_sales", F.lit(-1).cast(LongType())),
        lambda frame: frame.withColumn("wm_yr_wk", F.lit(-1).cast(IntegerType())),
        lambda frame: frame.withColumn("day_key", F.lit("d_0")),
        lambda frame: frame.withColumn("day_index", F.lit(99).cast(IntegerType())),
        lambda frame: frame.withColumn("unit_sales", F.lit(1.5)),
        lambda frame: frame.withColumn("date", F.lit("2026-01-31")),
        lambda frame: frame.unionByName(frame.limit(1)),
        lambda frame: frame.withColumn(
            "series_id",
            F.when(F.col("date") == F.lit(date(2026, 2, 1)), F.lit("different-series"))
            .otherwise(F.col("series_id")),
        ),
    ],
    ids=[
        "empty",
        "null-identifier",
        "null-date",
        "negative-sales",
        "negative-retail-week",
        "invalid-day-key",
        "mismatched-day-index",
        "fractional-sales",
        "wrong-date-type",
        "duplicate-source-fact",
        "inconsistent-series-mapping",
    ],
)
def test_invalid_or_duplicate_sales_daily_facts_are_rejected(spark: SparkSession, mutation) -> None:
    with pytest.raises(SalesAggregationError):
        aggregate_sales(mutation(_sales_frame(spark)), source_snapshot_id="snapshot-001")


def test_source_date_and_day_key_grains_are_both_checked(spark: SparkSession) -> None:
    rows = _source_rows()
    rows[2]["day_key"] = rows[0]["day_key"]
    rows[2]["day_index"] = rows[0]["day_index"]
    with pytest.raises(SalesAggregationError):
        aggregate_sales(_sales_frame(spark, rows), source_snapshot_id="snapshot-001")


def test_integer_output_overflow_is_rejected_without_truncation(spark: SparkSession) -> None:
    rows = _source_rows()
    rows[0]["unit_sales"] = 9_223_372_036_854_775_807
    rows[1]["unit_sales"] = 9_223_372_036_854_775_807

    with pytest.raises(SalesAggregationError):
        aggregate_sales(_sales_frame(spark, rows), source_snapshot_id="snapshot-001")


def test_distributed_output_validators_reject_tampered_store_rows(
    spark: SparkSession,
) -> None:
    source = _sales_frame(spark)
    result = aggregate_sales(source, source_snapshot_id="snapshot-001")
    valid = result.store_daily_sales

    mutations = (
        valid.unionByName(valid.limit(1)),
        valid.filter(~((F.col("store_id") == "store_a") & (F.col("date") == F.lit(date(2026, 1, 31)))),),
        valid.withColumn(
            "store_id",
            F.when(F.col("store_id") == "store_a", F.lit("unexpected-store")).otherwise(
                F.col("store_id")
            ),
        ),
        valid.withColumn("source_snapshot_id", F.lit("wrong-snapshot")),
        valid.withColumn("total_units", F.lit(-1).cast(LongType())),
        valid.withColumn(
            "total_units",
            F.when(F.col("store_id") == "store_a", F.lit(99)).otherwise(F.col("total_units")),
        ),
        valid.withColumn(
            "observed_series_count",
            F.when(F.col("store_id") == "store_a", F.lit(None).cast(LongType())).otherwise(
                F.col("observed_series_count")
            ),
        ),
    )
    for tampered in mutations:
        with pytest.raises(SalesAggregationError):
            validate_store_daily_sales(
                tampered,
                source,
                source_snapshot_id="snapshot-001",
            )


def test_distributed_category_validator_rejects_missing_and_tampered_rows(
    spark: SparkSession,
) -> None:
    source = _sales_frame(spark)
    result = aggregate_sales(source, source_snapshot_id="snapshot-001")

    missing = result.category_monthly_sales.filter(~(F.col("cat_id") == "cat_2"))
    tampered = result.category_monthly_sales.withColumn(
        "total_units",
        F.when(F.col("cat_id") == "cat_1", F.lit(99)).otherwise(F.col("total_units")),
    )
    for invalid in (missing, tampered):
        with pytest.raises(SalesAggregationError):
            validate_category_monthly_sales(
                invalid,
                source,
                source_snapshot_id="snapshot-001",
            )


def test_store_result_is_accepted_by_the_existing_catalog_validator(spark: SparkSession) -> None:
    snapshot = load_context()["lineage"]["source_snapshot_id"]
    result = aggregate_sales(_sales_frame(spark), source_snapshot_id=snapshot)

    validate_dataset(
        STORE_DAILY_SALES,
        (
            {
                **row.asDict(),
                "date": row.date.isoformat(),
            }
            for row in result.store_daily_sales.collect()
        ),
        lineage_context={"source_snapshot_id": snapshot},
    )


def test_separate_snapshot_invocations_remain_isolated(spark: SparkSession) -> None:
    source = _sales_frame(spark)
    first = aggregate_sales(source, source_snapshot_id="snapshot-a")
    second = aggregate_sales(source, source_snapshot_id="snapshot-b")

    assert {row.source_snapshot_id for row in first.store_daily_sales.collect()} == {"snapshot-a"}
    assert {row.source_snapshot_id for row in second.store_daily_sales.collect()} == {"snapshot-b"}
    assert {row.source_snapshot_id for row in first.category_monthly_sales.collect()} == {"snapshot-a"}
    assert {row.source_snapshot_id for row in second.category_monthly_sales.collect()} == {"snapshot-b"}
