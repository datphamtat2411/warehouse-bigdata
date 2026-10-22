"""Focused Spark checks for M04 T01 sales normalization."""

from __future__ import annotations

import os
from datetime import date

import pytest

from warehouse_bigdata.processing.sales_normalization import (
    NormalizationError,
    SalesNormalizationConfig,
    normalize_sales_daily,
    validate_sales_daily,
)

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

from pyspark.sql import SparkSession
from pyspark.sql.types import (  # noqa: E402
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

EVALUATION_DAYS = 1_941
SALES_METADATA = ("id", "item_id", "dept_id", "cat_id", "store_id", "state_id")
CONTRACT_COLUMNS = (
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


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("warehouse-bigdata-sales-normalization-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def _sales_rows() -> list[dict[str, str]]:
    values = [
        {
            "id": "item_1_CA_1",
            "item_id": "item_1",
            "dept_id": "dept_1",
            "cat_id": "cat_1",
            "store_id": "CA_1",
            "state_id": "CA",
            "d_1": "0",
            "d_2": "2",
            "d_3": "0",
            "d_4": "4",
        },
        {
            "id": "item_1_TX_1",
            "item_id": "item_1",
            "dept_id": "dept_1",
            "cat_id": "cat_1",
            "store_id": "TX_1",
            "state_id": "TX",
            "d_1": "1",
            "d_2": "0",
            "d_3": "3",
            "d_4": "0",
        },
        {
            "id": "item_2_CA_1",
            "item_id": "item_2",
            "dept_id": "dept_2",
            "cat_id": "cat_2",
            "store_id": "CA_1",
            "state_id": "CA",
            "d_1": "5",
            "d_2": "0",
            "d_3": "0",
            "d_4": "1",
        },
    ]
    for row in values:
        for day in range(1, EVALUATION_DAYS + 1):
            row.setdefault(f"d_{day}", "0")
    return values


def _sales_frame(
    spark: SparkSession,
    rows: list[dict[str, object]] | None = None,
    *,
    drop_columns: set[str] | None = None,
    extra_columns: tuple[str, ...] = (),
) -> object:
    columns = [*SALES_METADATA, *(f"d_{day}" for day in range(1, EVALUATION_DAYS + 1))]
    columns = [column for column in columns if column not in (drop_columns or set())]
    columns.extend(extra_columns)
    schema = StructType([StructField(column, StringType(), True) for column in columns])
    source_rows = rows if rows is not None else _sales_rows()
    values = [
        tuple(row.get(column, "0") for column in columns)
        for row in source_rows
    ]
    return spark.createDataFrame(values, schema)


def _calendar_rows() -> list[dict[str, str]]:
    dates = ("2011-01-29", "2011-01-30", "2011-01-31", "2011-02-01")
    return [
        {
            "d": f"d_{day}",
            "date": dates[day - 1],
            "wm_yr_wk": f"{11100 + day}",
        }
        for day in range(1, 5)
    ]


def _calendar_frame(spark: SparkSession, rows: list[dict[str, object]] | None = None) -> object:
    schema = StructType(
        [
            StructField("date", StringType(), True),
            StructField("wm_yr_wk", StringType(), True),
            StructField("d", StringType(), True),
        ]
    )
    source_rows = rows if rows is not None else _calendar_rows()
    return spark.createDataFrame(
        [(row.get("date"), row.get("wm_yr_wk"), row.get("d")) for row in source_rows],
        schema,
    )


def _config(*, days_per_chunk: int = 2) -> SalesNormalizationConfig:
    return SalesNormalizationConfig(day_start=1, day_end=4, days_per_chunk=days_per_chunk)


def _normalized(spark: SparkSession, *, days_per_chunk: int = 2) -> object:
    return normalize_sales_daily(_sales_frame(spark), _calendar_frame(spark), config=_config(days_per_chunk=days_per_chunk))


def test_normalization_emits_sales_daily_v1_and_preserves_zero_observations(
    spark: SparkSession,
) -> None:
    result = _normalized(spark)

    assert result.schema.names == list(CONTRACT_COLUMNS)
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
    ]
    assert result.count() == 12
    assert result.agg({"unit_sales": "sum"}).first()[0] == 16
    assert result.filter("unit_sales = 0").count() == 6
    rows = [row.asDict() for row in result.orderBy("series_id", "day_index").collect()]
    assert rows[0] == {
        "item_id": "item_1",
        "dept_id": "dept_1",
        "cat_id": "cat_1",
        "store_id": "CA_1",
        "state_id": "CA",
        "series_id": "item_1_CA_1",
        "day_key": "d_1",
        "day_index": 1,
        "date": date(2011, 1, 29),
        "wm_yr_wk": 11101,
        "unit_sales": 0,
    }
    assert {row["day_key"] for row in rows} == {"d_1", "d_2", "d_3", "d_4"}
    assert "sale_year" not in result.schema.names
    assert "source_snapshot_id" not in result.schema.names


def test_chunk_widths_produce_the_same_contract_rows(spark: SparkSession) -> None:
    expected = sorted(
        tuple(row[column] for column in CONTRACT_COLUMNS)
        for row in _normalized(spark, days_per_chunk=1).collect()
    )
    for width in (2, 128):
        actual = sorted(
            tuple(row[column] for column in CONTRACT_COLUMNS)
            for row in _normalized(spark, days_per_chunk=width).collect()
        )
        assert actual == expected


@pytest.mark.parametrize(
    "kwargs",
    [
        {"day_start": 0},
        {"day_end": 1_942},
        {"day_start": 4, "day_end": 3},
        {"days_per_chunk": 0},
        {"days_per_chunk": 1_942},
    ],
)
def test_invalid_day_configuration_is_rejected(kwargs: dict[str, int]) -> None:
    with pytest.raises(ValueError):
        SalesNormalizationConfig(**kwargs)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda rows: rows.__setitem__(1, {**rows[1], "id": rows[0]["id"]}),
        lambda rows: rows.__setitem__(1, {**rows[1], "item_id": rows[0]["item_id"], "store_id": rows[0]["store_id"]}),
    ],
    ids=["duplicate-id", "duplicate-series-key"],
)
def test_duplicate_source_identity_is_rejected(spark: SparkSession, mutation) -> None:
    rows = _sales_rows()
    mutation(rows)
    with pytest.raises(NormalizationError):
        normalize_sales_daily(_sales_frame(spark, rows), _calendar_frame(spark), config=_config())


@pytest.mark.parametrize("field", SALES_METADATA)
@pytest.mark.parametrize("value", [None, "", "   "])
def test_blank_source_identifiers_are_rejected(
    spark: SparkSession, field: str, value: object
) -> None:
    rows = _sales_rows()
    rows[0][field] = value
    with pytest.raises(NormalizationError):
        normalize_sales_daily(_sales_frame(spark, rows), _calendar_frame(spark), config=_config())


@pytest.mark.parametrize(
    "change",
    [
        lambda spark: _sales_frame(spark, drop_columns={"d_1941"}),
        lambda spark: _sales_frame(spark, extra_columns=("d_1942",)),
    ],
    ids=["missing-evaluation-day", "validation-history-day"],
)
def test_sales_width_must_be_the_complete_evaluation_source(spark: SparkSession, change) -> None:
    with pytest.raises(NormalizationError):
        normalize_sales_daily(change(spark), _calendar_frame(spark), config=_config())


@pytest.mark.parametrize("value", [None, "", "bad", "-1", "1.5", "9223372036854775808"])
def test_invalid_sales_values_are_rejected_before_casting(
    spark: SparkSession, value: object
) -> None:
    rows = _sales_rows()
    rows[0]["d_1"] = value
    with pytest.raises(NormalizationError):
        normalize_sales_daily(_sales_frame(spark, rows), _calendar_frame(spark), config=_config())


@pytest.mark.parametrize(
    "rows",
    [
        lambda: [row for row in _calendar_rows() if row["d"] != "d_4"],
        lambda: [*_calendar_rows(), _calendar_rows()[0]],
        lambda: [*_calendar_rows(), {**_calendar_rows()[0], "date": "2011-01-29", "d": "d_9"}],
        lambda: [{**row, "date": "2011-02-30"} if row["d"] == "d_1" else row for row in _calendar_rows()],
        lambda: [{**row, "wm_yr_wk": "1.5"} if row["d"] == "d_1" else row for row in _calendar_rows()],
        lambda: [{**row, "d": "day_1"} if row["d"] == "d_1" else row for row in _calendar_rows()],
    ],
    ids=[
        "incomplete-selected-coverage",
        "duplicate-day",
        "duplicate-date",
        "invalid-date",
        "invalid-retail-week",
        "invalid-day-key",
    ],
)
def test_invalid_calendar_mapping_is_rejected(spark: SparkSession, rows) -> None:
    with pytest.raises(NormalizationError):
        normalize_sales_daily(_sales_frame(spark), _calendar_frame(spark, rows()), config=_config())


@pytest.mark.parametrize("mutation", ["duplicate", "missing", "null", "total", "zero-count"])
def test_distributed_output_quality_gate_rejects_tampering(
    spark: SparkSession, mutation: str
) -> None:
    result = _normalized(spark)
    if mutation == "duplicate":
        tampered = result.unionByName(result.limit(1))
    elif mutation == "missing":
        tampered = result.filter(~((result.series_id == "item_1_CA_1") & (result.day_key == "d_1")))
    elif mutation == "null":
        from pyspark.sql import functions as F

        tampered = result.withColumn(
            "unit_sales",
            F.when((F.col("series_id") == "item_1_CA_1") & (F.col("day_key") == "d_1"), F.lit(None)).otherwise(
                F.col("unit_sales")
            ),
        )
    elif mutation == "total":
        from pyspark.sql import functions as F

        tampered = result.withColumn(
            "unit_sales",
            F.when((F.col("series_id") == "item_1_CA_1") & (F.col("day_key") == "d_2"), F.lit(99)).otherwise(
                F.col("unit_sales")
            ),
        )
    else:
        with pytest.raises(NormalizationError):
            validate_sales_daily(
                result,
                expected_series_count=3,
                selected_day_keys=["d_1", "d_2", "d_3", "d_4"],
                expected_unit_sales_total=16,
                expected_zero_sales_count=5,
            )
        return

    with pytest.raises(NormalizationError):
        validate_sales_daily(
            tampered,
            expected_series_count=3,
            selected_day_keys=["d_1", "d_2", "d_3", "d_4"],
            expected_unit_sales_total=16,
            expected_zero_sales_count=6,
        )


def test_validation_rejects_output_domain_violation(spark: SparkSession) -> None:
    from pyspark.sql import functions as F

    result = _normalized(spark).withColumn("unit_sales", F.lit(-1).cast(LongType()))
    with pytest.raises(NormalizationError):
        validate_sales_daily(
            result,
            expected_series_count=3,
            selected_day_keys=["d_1", "d_2", "d_3", "d_4"],
            expected_unit_sales_total=16,
            expected_zero_sales_count=6,
        )
