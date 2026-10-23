"""Focused checks for the approved M05-T01 historical feature contract."""

from __future__ import annotations

import os
from datetime import date, timedelta

import pytest

from warehouse_bigdata.contracts.datasets import FORECAST_FEATURES, SALES_ENRICHED
from warehouse_bigdata.contracts.validation import validate_dataset
from warehouse_bigdata.forecasting.features import (
    HistoricalFeatureError,
    build_historical_features,
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

ORIGIN = date(2011, 1, 29)
INPUT_TYPES = (
    *([StringType()] * 7),
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
    *([StringType()] * 4),
)
INPUT_SCHEMA = StructType(
    [
        StructField(name, kind, True)
        for name, kind in zip(SALES_ENRICHED.field_names, INPUT_TYPES, strict=True)
    ]
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("warehouse-bigdata-historical-feature-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def _rows(days=range(1, 36), *, item="item_1", store="CA_1", zero=False, prices=None):
    rows = []
    for day in days:
        target = ORIGIN + timedelta(days=day - 1)
        price = 2.5 if prices is None else prices.get(day)
        rows.append(
            dict(
                zip(
                    SALES_ENRICHED.field_names,
                    (
                        item,
                        "dept_1",
                        "cat_1",
                        store,
                        store[:2],
                        f"{item}_{store}",
                        f"d_{day}",
                        day,
                        target,
                        11101 + (day - 1) // 7,
                        0 if zero else day,
                        price,
                        price is None,
                        (target.weekday() + 2) % 7 + 1,
                        target.month,
                        target.year,
                        False,
                        zero,
                        "Recorded event",
                        "Cultural",
                        None,
                        None,
                    ),
                    strict=True,
                )
            )
        )
    return rows


def _frame(spark, rows=None):
    return spark.createDataFrame(_rows() if rows is None else rows, INPUT_SCHEMA)


def _build(frame, **kwargs):
    return build_historical_features(
        frame, source_snapshot_id="snapshot_1", feature_version="historical_v1", **kwargs
    )


def _validate_catalog_records(records):
    # The bounded catalog validator accepts serialized ISO dates, while Spark
    # DateType correctly collects as Python dates. Adapt only at this test boundary.
    validate_dataset(
        FORECAST_FEATURES,
        [
            {
                name: value.isoformat() if isinstance(value, date) else value
                for name, value in record.items()
            }
            for record in records
        ],
    )


@pytest.fixture(scope="module")
def ordinary(spark):
    return _build(_frame(spark))


def test_hand_calculation_and_frozen_contract(ordinary):
    features = ordinary.features
    assert tuple(features.columns) == FORECAST_FEATURES.field_names
    assert [field.dataType for field in features.schema] == [
        *([StringType()] * 6),
        DateType(),
        DateType(),
        LongType(),
        LongType(),
        LongType(),
        DoubleType(),
        DoubleType(),
        IntegerType(),
        IntegerType(),
        BooleanType(),
        BooleanType(),
        BooleanType(),
        DoubleType(),
        LongType(),
    ]
    rows = [row.asDict() for row in features.collect()]
    assert len(rows) == 7
    _validate_catalog_records(rows)
    row = next(row for row in rows if row["target_date"] == ORIGIN + timedelta(days=34))
    assert (row["lag_1"], row["lag_7"], row["lag_28"]) == (34, 28, 7)
    assert (row["rolling_mean_7"], row["rolling_mean_28"]) == (31.0, 20.5)
    assert row["label"] == 35
    assert row["as_of_date"] == row["target_date"] - timedelta(days=1)
    assert (row["source_snapshot_id"], row["feature_version"], row["feature_role"]) == (
        "snapshot_1",
        "historical_v1",
        "train",
    )
    assert all(row["label"] is not None for row in rows)
    assert len({(row["item_id"], row["store_id"], row["target_date"]) for row in rows}) == 7
    # The source has a price even on d_1, but it is not yet known as of d_1 - 1.
    assert sum(row.price_missing_count for row in ordinary.coverage.collect()) == 1


def test_iso_calendar_and_unverified_recorded_events_are_disabled(ordinary):
    rows = ordinary.features.collect()
    assert {row.day_of_week for row in rows} == set(range(1, 8))
    assert {row.month for row in rows} == {2, 3}
    for row in rows:
        assert row.day_of_week == row.target_date.isoweekday()
        assert row.month == row.target_date.month
        assert row.is_weekend == (row.target_date.weekday() >= 5)
        assert row.event_flag is False
    # d_29 is Saturday: M5 wday=1 must not become ISO Monday=1.
    first = min(rows, key=lambda row: row.target_date)
    assert first.day_of_week == 6


def test_target_and_future_sales_prices_and_events_cannot_change_predictors(spark, ordinary):
    rows = _rows()
    for row in rows:
        if row["day_index"] >= 34:
            row.update(
                unit_sales=0,
                is_zero_sale=True,
                sell_price=999.0,
                price_missing=False,
                event_name_1="Changed event",
            )
    changed = (
        _build(_frame(spark, rows))
        .features.where(F.col("target_date") == F.lit(ORIGIN + timedelta(days=33)))
        .first()
        .asDict()
    )
    original = (
        ordinary.features.where(F.col("target_date") == F.lit(ORIGIN + timedelta(days=33)))
        .first()
        .asDict()
    )
    assert original.pop("label") == 34
    assert changed.pop("label") == 0
    assert changed == original


def test_as_of_prices_carry_across_nulls_weeks_and_demand_window_with_series_isolation(spark):
    rows = (
        _rows(range(1, 37), item="carried", prices={1: 1.5, 29: 2.0, 35: 8.0})
        + _rows(range(1, 37), item="unknown", prices={})
        + _rows(range(1, 37), item="target_only", prices={35: 7.0, 36: 9.0})
        + _rows(range(1, 37), item="carried", store="TX_1", zero=True, prices={2: 4.0})
    )
    # Deliberately shuffled source: neither row order nor another store may supply history.
    result = _build(_frame(spark, rows).orderBy(F.desc("day_index"), "store_id"))
    found = {
        (r.item_id, r.store_id, (r.target_date - ORIGIN).days + 1): r
        for r in result.features.collect()
    }
    assert found["carried", "CA_1", 29].last_known_price == 1.5
    assert found["carried", "CA_1", 30].last_known_price == 2.0
    assert found["carried", "CA_1", 35].last_known_price == 2.0
    assert found["carried", "CA_1", 36].last_known_price == 8.0
    assert found["target_only", "CA_1", 35].last_known_price is None
    assert found["target_only", "CA_1", 35].price_missing is True
    assert found["target_only", "CA_1", 36].last_known_price == 7.0
    for (item, store, _), row in found.items():
        assert row.price_missing == (row.last_known_price is None)
        if item == "unknown":
            assert row.last_known_price is None
        if store == "TX_1":
            assert row.last_known_price == 4.0
            assert (row.lag_1, row.lag_7, row.lag_28, row.label) == (0, 0, 0, 0)
            assert (row.rolling_mean_7, row.rolling_mean_28) == (0.0, 0.0)
    _validate_catalog_records([r.asDict() for r in found.values()])
    plan = result.features._jdf.queryExecution().executedPlan().toString()
    assert "PythonUDF" not in plan and "BatchEvalPython" not in plan


def test_eligibility_uses_calendar_days_reports_gaps_and_recovers(spark):
    complete_days = list(range(1, 66))
    rows = (
        _rows([day for day in complete_days if day != 35], item="gap", prices={})
        + _rows(range(25, 56), item="late", prices={})
        + _rows(range(1, 36), item="zero", zero=True, prices={})
    )
    result = _build(_frame(spark, rows))
    diagnostics = result.eligibility.collect()
    assert len(diagnostics) == len(rows)
    found = {(r.item_id, r.day_index): r for r in diagnostics}
    assert ("gap", 35) not in found
    assert found["gap", 28].status == "LOOKBACK_ONLY"
    assert found["gap", 28].feature_role is None
    assert found["gap", 29].status == "ELIGIBLE"
    assert found["gap", 36].status == "DISCONTINUOUS_HISTORY"
    assert found["gap", 36].prior_day_count == 27
    assert found["gap", 36].missing_day_count == 1
    assert found["gap", 63].status == "DISCONTINUOUS_HISTORY"
    assert found["gap", 64].status == "ELIGIBLE"
    assert found["late", 28].status == "LOOKBACK_ONLY"
    assert found["late", 29].status == "INSUFFICIENT_HISTORY"
    assert found["late", 29].prior_day_count == 4
    assert found["late", 29].missing_day_count == 24
    assert found["late", 52].status == "INSUFFICIENT_HISTORY"
    assert found["late", 53].status == "ELIGIBLE"
    assert found["zero", 29].status == "ELIGIBLE"
    assert all(
        r.source_snapshot_id == "snapshot_1" and r.feature_version == "historical_v1"
        for r in diagnostics
    )
    emitted = {(r.item_id, r.target_date) for r in result.features.collect()}
    assert emitted == {(r.item_id, r.target_date) for r in diagnostics if r.status == "ELIGIBLE"}
    coverage = result.coverage.collect()
    assert sum(r.observed_count for r in coverage) == len(rows)
    assert sum(r.candidate_count for r in coverage) == sum(r.day_index >= 29 for r in diagnostics)
    assert sum(r.eligible_count for r in coverage) == len(emitted)
    assert sum(r.excluded_count for r in coverage) == sum(
        r.day_index >= 29 and r.status != "ELIGIBLE" for r in diagnostics
    )
    assert sum(r.price_missing_count for r in coverage) == len(rows)
    for group in coverage:
        expected = [
            r
            for r in diagnostics
            if (r.feature_role, r.status) == (group.feature_role, group.status)
        ]
        assert group.observed_count == len(expected)
        assert group.candidate_count == group.eligible_count + group.excluded_count
        assert group.source_snapshot_id == "snapshot_1"
        assert group.feature_version == "historical_v1"


def test_role_boundaries_include_context_and_final_labeled_day(spark):
    days = [*range(1, 30), *range(1857, 1942)]
    result = _build(_frame(spark, _rows(days)))
    features = {(r.target_date - ORIGIN).days + 1: r for r in result.features.collect()}
    roles = {day: row.feature_role for day, row in features.items()}
    assert 28 not in roles
    assert {day: roles[day] for day in (29, 1885, 1886, 1913, 1914, 1941)} == {
        29: "train",
        1885: "train",
        1886: "validation",
        1913: "validation",
        1914: "test",
        1941: "test",
    }
    # Warmup for a supplied late segment is excluded rather than filled from older rows.
    assert 1857 not in roles and 1884 not in roles
    for day in (1886, 1914, 1941):
        assert features[day].lag_1 == day - 1
        assert features[day].lag_28 == day - 28
        assert features[day].label == day


@pytest.mark.parametrize(
    "days", [range(1, 29), range(100, 110), []], ids=["lookback-only", "truncated", "empty"]
)
def test_zero_eligible_outputs_keep_contract_schema(spark, days):
    result = _build(_frame(spark, _rows(days)))
    assert tuple(result.features.columns) == FORECAST_FEATURES.field_names
    assert result.features.count() == 0
    assert result.eligibility.count() == len(days)
    assert sum(r.observed_count for r in result.coverage.collect()) == len(days)


@pytest.mark.parametrize(
    "field,value",
    [
        ("unit_sales", -1),
        ("unit_sales", None),
        ("date", None),
        ("day_key", "d_01"),
        ("day_key", "d_bad"),
        ("day_key", "d_" + "9" * 40),
        ("day_index", 0),
        ("day_index", 1942),
        ("day_key", "d_2"),
        ("sell_price", float("nan")),
        ("sell_price", float("inf")),
        ("sell_price", float("-inf")),
        ("sell_price", 0.0),
        ("sell_price", -1.0),
        ("price_missing", True),
        ("price_missing", None),
        ("is_zero_sale", True),
        ("item_id", " "),
        ("store_id", None),
        ("state_id", "NY"),
        ("event_name_1", " "),
        ("wm_yr_wk", -1),
        ("wday", 0),
        ("month", 13),
        ("year", 0),
        ("snap_applicable", None),
    ],
)
def test_corrupt_observations_are_rejected_eagerly(spark, field, value):
    rows = _rows(range(1, 3))
    rows[0][field] = value
    with pytest.raises(HistoricalFeatureError):
        _build(_frame(spark, rows))


@pytest.mark.parametrize(
    "kind",
    [
        "duplicate",
        "duplicate-date",
        "mapping-across-series",
        "wrong-spacing",
        "dept",
        "category",
        "state",
        "series",
        "ambiguous-series",
    ],
)
def test_duplicate_mapping_and_metadata_corruption_are_rejected(spark, kind):
    rows = _rows(range(1, 4))
    if kind == "duplicate":
        rows.append(rows[0].copy())
    elif kind == "duplicate-date":
        rows[1]["date"] = rows[0]["date"]
    elif kind == "mapping-across-series":
        extra = _rows(range(1, 2), item="other")
        extra[0]["date"] += timedelta(days=1)
        rows += extra
    elif kind == "wrong-spacing":
        rows[1]["date"] += timedelta(days=5)
    elif kind == "ambiguous-series":
        extra = _rows(range(1, 2), item="other")
        extra[0]["series_id"] = rows[0]["series_id"]
        rows += extra
    else:
        field = {
            "dept": "dept_id",
            "category": "cat_id",
            "state": "state_id",
            "series": "series_id",
        }[kind]
        rows[1][field] = "TX" if kind == "state" else "changed"
    # Keep recorded calendar fields valid so these checks exercise mapping and
    # grain integrity rather than an unrelated source-calendar-field failure.
    for row in rows:
        target = row["date"]
        row.update(wday=(target.weekday() + 2) % 7 + 1, month=target.month, year=target.year)
    with pytest.raises(HistoricalFeatureError):
        _build(_frame(spark, rows))


def test_future_canonical_day_is_rejected_even_with_consistent_calendar_and_key(spark):
    with pytest.raises(HistoricalFeatureError):
        _build(_frame(spark, _rows(range(1941, 1943))))


@pytest.mark.parametrize(
    "mutation",
    [
        lambda frame: frame.drop("event_type_2"),
        lambda frame: frame.withColumn("extra", F.lit(1)),
        lambda frame: frame.withColumn("unit_sales", F.col("unit_sales").cast("int")),
        lambda frame: frame.select(*reversed(frame.columns)),
        lambda frame: frame.select("*", F.col("item_id")),
    ],
)
def test_exact_input_schema_is_required(spark, mutation):
    with pytest.raises(HistoricalFeatureError):
        _build(mutation(_frame(spark, _rows(range(1, 2)))))


@pytest.mark.parametrize(
    "name,value",
    [
        ("source_snapshot_id", ""),
        ("source_snapshot_id", "  "),
        ("source_snapshot_id", None),
        ("feature_version", "\t"),
        ("feature_version", 1),
    ],
)
def test_explicit_nonblank_lineage_is_required(spark, name, value):
    kwargs = {"source_snapshot_id": "snapshot_1", "feature_version": "historical_v1"}
    kwargs[name] = value
    with pytest.raises(HistoricalFeatureError):
        build_historical_features(_frame(spark, _rows(range(1, 2))), **kwargs)
