"""Focused M05-T02 checks using caller-supplied, bounded recursive states."""

from __future__ import annotations

import copy
import math
import os
from datetime import date, datetime, timedelta

import pytest

from warehouse_bigdata.contracts.datasets import FORECAST_FEATURES, SALES_ENRICHED
from warehouse_bigdata.forecasting.features import build_historical_features
from warehouse_bigdata.forecasting.scoring_features import (
    AS_OF_HISTORY_SCHEMA_V1,
    SCORING_FEATURE_SCHEMA_V1,
    ScoringFeatureError,
    build_scoring_features,
    prepare_as_of_history,
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
ISSUE = ORIGIN + timedelta(days=33)
INPUT_SCHEMA = StructType(
    [
        StructField(name, kind, True)
        for name, kind in zip(
            SALES_ENRICHED.field_names,
            (
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
            ),
            strict=True,
        )
    ]
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("warehouse-bigdata-scoring-feature-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def _rows(days=range(1, 42), *, item="item_1", store="CA_1", prices=None, zero=False):
    result = []
    for day in days:
        target = ORIGIN + timedelta(days=day - 1)
        price = 2.5 if prices is None else prices.get(day)
        result.append(
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
                        "Unverified recorded event",
                        "Cultural",
                        None,
                        None,
                    ),
                    strict=True,
                )
            )
        )
    return result


def _frame(spark, rows):
    return spark.createDataFrame(rows, INPUT_SCHEMA)


def _prepare(spark, rows=None, scope=None, issue=ISSUE):
    return prepare_as_of_history(
        _frame(spark, _rows() if rows is None else rows),
        issue_date=issue,
        source_snapshot_id="snapshot_1",
        feature_version="features_v1",
        series_scope=spark.createDataFrame(
            [("item_1", "CA_1")] if scope is None else scope,
            "item_id string, store_id string",
        ),
    )


def _score(spark, state, horizon=1, issue=ISSUE, calendar=None):
    target = issue + timedelta(days=horizon)
    return build_scoring_features(
        state,
        issue_date=issue,
        target_date=target,
        known_calendar=(
            spark.createDataFrame([(target,)], "date date") if calendar is None else calendar
        ),
    )


def _state_frame(spark, rows):
    return spark.createDataFrame(rows, AS_OF_HISTORY_SCHEMA_V1)


def _advance(record, predictions):
    """Test input construction only: production state updates belong to M06."""
    record = copy.deepcopy(record)
    issue = record["as_of_date"]
    for horizon, value in enumerate(predictions, start=1):
        record["history"] = record["history"][1:] + [
            {
                "date": issue + timedelta(days=horizon),
                "value": value,
                "origin": "predicted_unit_sales",
                "prediction_issue_date": issue,
            }
        ]
    return record


def _reconcile(result, candidates, eligible):
    rows = result.coverage.collect()
    assert sum(r.candidate_count for r in rows) == candidates
    assert sum(r.eligible_count for r in rows) == eligible
    assert sum(r.excluded_count for r in rows) == candidates - eligible
    assert all(r.candidate_count == r.eligible_count + r.excluded_count for r in rows)


@pytest.fixture(scope="module")
def prepared(spark):
    result = _prepare(spark)
    result.state.persist()
    assert result.state.count() == 1
    yield result
    result.state.unpersist()


def test_first_horizon_matches_every_t01_predictor_and_contract(spark, prepared):
    result = _score(spark, prepared.state)
    scoring = result.features.first().asDict()
    historical = (
        build_historical_features(
            _frame(spark, _rows(range(1, 36))),
            source_snapshot_id="snapshot_1",
            feature_version="features_v1",
        )
        .features.where(F.col("target_date") == ISSUE + timedelta(days=1))
        .first()
        .asDict()
    )
    assert historical.pop("label") == 35
    assert historical.pop("feature_role") == "train"
    assert scoring.pop("feature_role") == "scoring"
    assert scoring == historical
    assert result.features.schema.simpleString() == SCORING_FEATURE_SCHEMA_V1.simpleString()
    assert tuple(result.features.columns) == tuple(
        n for n in FORECAST_FEATURES.field_names if n != "label"
    )
    assert FORECAST_FEATURES.version == "1.0"
    assert [f.logical_type for f in FORECAST_FEATURES.fields if f.name.startswith("lag_")] == [
        "NonNegativeInt",
        "NonNegativeInt",
        "NonNegativeInt",
    ]
    assert all(result.features.schema[f"lag_{n}"].dataType == DoubleType() for n in (1, 7, 28))
    assert result.eligibility.first().reason_codes == []
    _reconcile(result, 1, 1)
    plan = result.features._jdf.queryExecution().executedPlan().toString()
    assert "InMemoryTableScan" in plan
    assert all(token not in plan for token in ("PythonUDF", "BatchEvalPython", "ArrowEvalPython"))


def test_all_horizons_preserve_fractional_history_and_frozen_context(spark, prepared):
    original = prepared.state.first().asDict(recursive=True)
    predictions = [40.5, 3.125, 0.0, 18.25, 7.75, 2.625]
    for horizon in range(1, 8):
        record = _advance(original, predictions[: horizon - 1])
        result = _score(spark, _state_frame(spark, [record]), horizon)
        row = result.features.first()
        values = [entry["value"] for entry in record["history"]]
        assert (row.lag_1, row.lag_7, row.lag_28) == (values[-1], values[-7], values[-28])
        assert row.rolling_mean_7 == pytest.approx(sum(values[-7:]) / 7)
        assert row.rolling_mean_28 == pytest.approx(sum(values) / 28)
        assert all(math.isfinite(value) for value in values)
        assert (row.as_of_date, row.last_known_price, row.price_missing) == (ISSUE, 2.5, False)
        assert (row.source_snapshot_id, row.feature_version, row.dept_id) == (
            "snapshot_1",
            "features_v1",
            "dept_1",
        )
        assert result.eligibility.first().prediction_count == horizon - 1
        if horizon == 2:
            assert (row.lag_1, row.lag_7, row.lag_28) == (40.5, 29.0, 8.0)
            assert row.rolling_mean_7 == pytest.approx(229.5 / 7)
            assert row.rolling_mean_28 == pytest.approx(607.5 / 28)
        if horizon == 7:
            assert row.rolling_mean_7 == pytest.approx((34 + sum(predictions)) / 7)
    assert prepared.state.first().asDict(recursive=True) == original


def test_future_poisoning_and_removal_cannot_affect_state_or_scoring(spark, prepared):
    rows = _rows()
    for row in rows:
        if row["date"] > ISSUE:
            row.update(
                unit_sales=-999,
                sell_price=float("nan"),
                price_missing=None,
                dept_id="future identity",
                series_id="future series",
                event_name_1="Future",
            )
    expected = prepared.state.collect()
    for source in (rows, [row for row in rows if row["date"] <= ISSUE]):
        changed = _prepare(spark, source)
        assert changed.state.collect() == expected
        assert (
            _score(spark, changed.state).features.collect()
            == _score(
                spark,
                prepared.state,
            ).features.collect()
        )


def test_price_reduction_zeros_shuffled_input_and_series_isolation(spark):
    rows = (
        _rows(item="on_issue", prices={1: 1.0, 34: 7.0, 35: 999.0})
        + _rows(item="old", prices={1: 1.25})
        + _rows(item="unknown", prices={})
        + _rows(item="old", store="TX_1", prices={2: 4.0}, zero=True)
    )
    result = _prepare(
        spark,
        list(reversed(rows)),
        [
            ("on_issue", "CA_1"),
            ("old", "CA_1"),
            ("unknown", "CA_1"),
            ("old", "TX_1"),
        ],
    )
    found = {(r.item_id, r.store_id): r for r in result.state.collect()}
    assert found["on_issue", "CA_1"].last_known_price == 7.0
    assert found["old", "CA_1"].last_known_price == 1.25
    assert found["unknown", "CA_1"].last_known_price is None
    assert found["unknown", "CA_1"].price_missing is True
    assert found["old", "TX_1"].last_known_price == 4.0
    assert all(entry.value == 0.0 for entry in found["old", "TX_1"].history)
    assert all(len(row.history) == 28 for row in found.values())
    assert [entry.date for entry in found["old", "CA_1"].history] == [
        ISSUE - timedelta(days=n) for n in reversed(range(28))
    ]
    _reconcile(result, 4, 4)


def test_preparation_defects_keep_candidates_and_all_reasons(spark):
    cases = {
        "healthy": _rows(item="healthy"),
        "short": _rows(range(25, 35), item="short"),
        "gap": _rows([n for n in range(1, 35) if n != 20], item="gap"),
        "duplicate": _rows(range(1, 35), item="duplicate"),
        "unstable": _rows(range(1, 35), item="unstable"),
        "negative": _rows(range(1, 35), item="negative"),
        "null": _rows(range(1, 35), item="null"),
        "nan_price": _rows(range(1, 35), item="nan_price"),
        "inf_price": _rows(range(1, 35), item="inf_price"),
        "bad_mapping": _rows(range(1, 35), item="bad_mapping"),
        "null_date": _rows(range(1, 35), item="null_date"),
    }
    cases["duplicate"] += [cases["duplicate"][-1].copy()] * 80
    cases["unstable"][0]["dept_id"] = "changed"
    cases["negative"][-1]["unit_sales"] = -1
    cases["null"][-1]["unit_sales"] = None
    cases["nan_price"][0]["sell_price"] = float("nan")
    cases["inf_price"][0]["sell_price"] = float("inf")
    cases["bad_mapping"][-1]["day_index"] = 3
    cases["null_date"][-1]["date"] = None
    result = _prepare(spark, sum(cases.values(), []), [(key, "CA_1") for key in [*cases, "absent"]])
    found = {r.item_id: r for r in result.eligibility.collect()}
    assert len(found) == len(cases) + 1
    assert found["healthy"].reason_codes == []
    for item, reason in {
        "absent": "MISSING_HISTORY",
        "short": "INSUFFICIENT_HISTORY",
        "gap": "DISCONTINUOUS_HISTORY",
        "duplicate": "DUPLICATE_DATE",
        "unstable": "INVALID_SERIES",
        "negative": "INVALID_VALUE",
        "null": "INVALID_VALUE",
        "nan_price": "INVALID_VALUE",
        "inf_price": "INVALID_VALUE",
        "bad_mapping": "INVALID_SERIES",
        "null_date": "INVALID_SERIES",
    }.items():
        assert reason in found[item].reason_codes
        assert found[item].status == "EXCLUDED"
    assert found["gap"].missing_day_count == 1
    assert found["short"].history_count == 10
    assert found["absent"].missing_day_count == 28
    assert all(r.reason_codes == sorted(set(r.reason_codes)) for r in found.values())
    assert [r.item_id for r in result.state.collect()] == ["healthy"]
    _reconcile(result, len(found), 1)


def test_ambiguous_audit_identity_is_excluded_without_affecting_healthy_series(spark):
    rows = _rows(item="a") + _rows(item="b") + _rows(item="healthy")
    for row in rows:
        if row["item_id"] in ("a", "b"):
            row["series_id"] = "shared"
    result = _prepare(spark, rows, [(item, "CA_1") for item in ("a", "b", "healthy")])
    found = {r.item_id: r.reason_codes for r in result.eligibility.collect()}
    assert found["healthy"] == []
    assert all("INVALID_SERIES" in found[item] for item in ("a", "b"))


def test_scoring_defects_are_per_series_and_do_not_sort_or_fill(spark, prepared):
    original = _advance(prepared.state.first().asDict(recursive=True), [40.5])
    cases = {
        key: copy.deepcopy(original)
        for key in (
            "healthy",
            "negative",
            "null",
            "nan",
            "inf",
            "minus_inf",
            "observed_future",
            "wrong_prediction_issue",
            "predicted_past",
            "reordered",
            "duplicate",
            "skipped",
            "premature",
            "wrong_issue",
            "blank_lineage",
            "bad_price",
            "missing_flag",
            "overflow",
            "missing",
            "oversized",
            "invalid_identity",
            "duplicate_state",
        )
    }
    for key, record in cases.items():
        record.update(item_id=key, series_id=key)
    for key, value in (
        ("negative", -1.0),
        ("null", None),
        ("nan", float("nan")),
        ("inf", float("inf")),
        ("minus_inf", float("-inf")),
    ):
        cases[key]["history"][-1]["value"] = value
    cases["observed_future"]["history"][-1].update(
        origin="observed_m5_unit_sales",
        prediction_issue_date=None,
    )
    cases["wrong_prediction_issue"]["history"][-1]["prediction_issue_date"] = ISSUE - timedelta(
        days=1
    )
    cases["predicted_past"]["history"][0].update(
        origin="predicted_unit_sales", prediction_issue_date=ISSUE
    )
    cases["reordered"]["history"].reverse()
    cases["duplicate"]["history"][-2] = cases["duplicate"]["history"][-1].copy()
    cases["skipped"]["history"].pop(10)
    cases["premature"]["history"][-1]["date"] += timedelta(days=1)
    cases["wrong_issue"]["as_of_date"] -= timedelta(days=1)
    cases["blank_lineage"]["feature_version"] = " "
    cases["bad_price"]["last_known_price"] = float("inf")
    cases["missing_flag"]["price_missing"] = True
    for entry in cases["overflow"]["history"]:
        entry["value"] = 1.7e308
    cases["missing"]["history"] = None
    cases["oversized"]["history"] *= 3
    cases["invalid_identity"]["dept_id"] = None
    result = _score(spark, _state_frame(spark, [*cases.values(), cases["duplicate_state"]]), 2)
    found = {r.item_id: r for r in result.eligibility.collect()}
    assert len(found) == len(cases)
    expected = {
        **dict.fromkeys(
            (
                "negative",
                "null",
                "nan",
                "inf",
                "minus_inf",
                "bad_price",
                "missing_flag",
                "overflow",
            ),
            "INVALID_VALUE",
        ),
        **dict.fromkeys(
            ("observed_future", "wrong_prediction_issue", "predicted_past"), "INVALID_PROVENANCE"
        ),
        **dict.fromkeys(("reordered", "premature", "oversized"), "NON_SEQUENTIAL_HISTORY"),
        "duplicate": "DUPLICATE_DATE",
        "skipped": "DISCONTINUOUS_HISTORY",
        "wrong_issue": "LINEAGE_MISMATCH",
        "blank_lineage": "LINEAGE_MISMATCH",
        "missing": "MISSING_HISTORY",
        "invalid_identity": "INVALID_SERIES",
        "duplicate_state": "INVALID_SERIES",
    }
    for item, reason in expected.items():
        assert reason in found[item].reason_codes, (item, found[item])
        assert found[item].reason_codes == sorted(set(found[item].reason_codes))
    assert [r.item_id for r in result.features.collect()] == ["healthy"]
    _reconcile(result, len(cases), 1)


def test_calendar_boundaries_final_demo_and_no_labels(spark):
    issue = ORIGIN + timedelta(days=1940)
    prepared = _prepare(spark, _rows(range(1800, 1942)), issue=issue)
    record = prepared.state.first().asDict(recursive=True)
    assert len(record["history"]) == 28
    for horizon in range(1, 8):
        row = _score(
            spark, _state_frame(spark, [_advance(record, [0.25] * (horizon - 1))]), horizon, issue
        ).features.first()
        assert row.day_of_week == row.target_date.isoweekday()
        assert row.month == row.target_date.month
        assert row.is_weekend == (row.day_of_week >= 6)
        assert row.event_flag is False
        assert "label" not in row.asDict()
    # Ordinary d_35 and d_36 straddle a month boundary; d_29 is Saturday.
    for day in (28, 34, 35):
        at = ORIGIN + timedelta(days=day - 1)
        state = _prepare(spark, _rows(range(1, day + 1)), issue=at).state
        row = _score(spark, state, issue=at).features.first()
        assert row.month == row.target_date.month
        assert row.day_of_week == row.target_date.isoweekday()
        assert row.is_weekend == (row.target_date.isoweekday() >= 6)


def test_empty_schemas_and_missing_candidates(spark):
    empty = _prepare(spark, [], [])
    assert empty.state.schema.simpleString() == AS_OF_HISTORY_SCHEMA_V1.simpleString()
    assert empty.state.count() == empty.eligibility.count() == empty.coverage.count() == 0
    scoring = _score(spark, empty.state)
    assert scoring.features.schema.simpleString() == SCORING_FEATURE_SCHEMA_V1.simpleString()
    assert scoring.features.count() == scoring.eligibility.count() == scoring.coverage.count() == 0
    missing = _prepare(spark, [], [("missing", "CA_1")])
    assert missing.state.count() == 0
    assert "MISSING_HISTORY" in missing.eligibility.first().reason_codes
    _reconcile(missing, 1, 0)


def test_invalid_arguments_and_incompatible_schemas_raise(spark, prepared):
    sales = _frame(spark, _rows(range(1, 2)))
    scope = spark.createDataFrame([("item_1", "CA_1")], "item_id string, store_id string")
    options = dict(
        issue_date=ISSUE, source_snapshot_id="snapshot_1", feature_version="v1", series_scope=scope
    )
    for key, value in (
        ("issue_date", "2011-03-03"),
        ("issue_date", datetime(2011, 3, 3)),
        ("source_snapshot_id", " "),
        ("feature_version", None),
    ):
        with pytest.raises(ScoringFeatureError):
            prepare_as_of_history(sales, **{**options, key: value})
    for changed in (
        sales.drop("unit_sales"),
        sales.withColumn("unit_sales", F.col("unit_sales").cast("double")),
    ):
        with pytest.raises(ScoringFeatureError):
            prepare_as_of_history(changed, **options)
    with pytest.raises(ScoringFeatureError):
        prepare_as_of_history(sales, **{**options, "series_scope": scope.union(scope)})
    for horizon in (0, 8):
        with pytest.raises(ScoringFeatureError):
            _score(spark, prepared.state, horizon)
    for state in (prepared.state.drop("history"), prepared.state.withColumn("label", F.lit(0))):
        with pytest.raises(ScoringFeatureError):
            _score(spark, state)
    target = ISSUE + timedelta(days=1)
    calendars = [
        spark.createDataFrame([], "date date"),
        spark.createDataFrame([(ISSUE,)], "date date"),
        spark.createDataFrame([(target,), (target,)], "date date"),
        spark.createDataFrame([(target,), (None,)], "date date"),
        spark.createDataFrame([(target, 1.0)], "date date, price double"),
        spark.createDataFrame([(target, "event")], "date date, event string"),
        spark.createDataFrame([(str(target),)], "date string"),
    ]
    for calendar in calendars:
        with pytest.raises(ScoringFeatureError):
            _score(spark, prepared.state, calendar=calendar)
