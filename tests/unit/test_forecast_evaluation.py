"""Synthetic chronological pairing and hand-calculated evaluation checks."""

from datetime import date, datetime, timedelta, timezone
import math

import pytest
from pyspark.sql import functions as F

from tests.unit.test_scoring_features import spark as local_spark, _frame, _rows
from warehouse_bigdata.forecasting.evaluation import (
    ORIGINS, EvaluationContext, align_origin, aggregate_metrics, expected_keys,
    freeze_protocol, resolve_origins, seasonal_naive, validate_freeze, validate_metrics,
)
from warehouse_bigdata.forecasting.recursive_forecasting import ForecastRunContext, DAILY_TYPES
from warehouse_bigdata.contracts.datasets import FORECAST_DAILY, FORECAST_METRICS
from warehouse_bigdata.forecasting.scoring_features import prepare_as_of_history


@pytest.fixture(scope="module")
def spark(local_spark):
    local_spark.conf.set("spark.sql.session.timeZone", "UTC")
    return local_spark


def context(kind="validation"):
    return EvaluationContext("eval", "model", "snapshot", "v1", kind)


def test_locked_calendar_and_bounded_forecast_id(spark):
    calendar = spark.createDataFrame([(date(2011, 1, 29) + timedelta(days=d-1), d)
                                     for d in range(1885, 1942)], "target_date date, day_index int")
    for kind, indices in ORIGINS.items():
        origins = resolve_origins(calendar, kind)
        assert [d for d, _ in origins] == list(indices)
        assert all(context(kind).forecast_id(d) == context(kind).forecast_id(d) for d in indices)
        assert len(context(kind).forecast_id(indices[0])) <= 128
    with pytest.raises(ValueError, match="calendar"):
        resolve_origins(calendar.where("day_index != 1913"), "test")
    assert context().window == (date(2016, 3, 28), date(2016, 4, 24))
    assert context("test").window == (date(2016, 4, 25), date(2016, 5, 22))


def test_freeze_rejects_model_membership_policy_code_and_seed_changes():
    frozen = freeze_protocol(model_metadata={"manifest": {"artifact_checksum": "checksum"},
        "input_binding": {"training": "selected"}}, cohort_digest="cohort", seed=42,
        code_revision="code", source_binding={"source_snapshot_id": "snapshot"})
    validate_freeze(frozen, frozen)
    for key in ("model", "cohort_digest", "seed", "code_revision", "policy", "source_binding"):
        with pytest.raises(ValueError, match="freeze"):
            validate_freeze(frozen, {**frozen, key: "poison"})


def test_baseline_uses_original_observations_and_later_origins_reveal_history(spark):
    issue = date(2016, 3, 27)
    scope = spark.createDataFrame([("item_1", "CA_1")], "item_id string, store_id string")
    rows = _rows(range(1858, 1893))
    poisoned = [{**r, "unit_sales": 900000, "sell_price": 999.0} if r["date"] > issue else r for r in rows]
    outputs = []
    for source in (rows, poisoned):
        prepared = prepare_as_of_history(_frame(spark, source), issue_date=issue,
            source_snapshot_id="snapshot", feature_version="v1", series_scope=scope)
        result = seasonal_naive(prepared.state, issue_date=issue)
        values = result.predictions.orderBy("horizon").collect()
        assert values[0].baseline_source_date == issue - timedelta(days=6)
        assert values[-1].baseline_source_date == issue
        assert result.exceptions.count() == 0
        outputs.append([r.predicted_units for r in values])
    assert outputs[0] == outputs[1] == list(map(float, range(1879, 1886)))
    later = prepare_as_of_history(_frame(spark, poisoned), issue_date=issue + timedelta(days=7),
        source_snapshot_id="snapshot", feature_version="v1", series_scope=scope)
    assert seasonal_naive(later.state, issue_date=issue + timedelta(days=7)).predictions.first().predicted_units == 900000
    missing = prepared.state.withColumn("history", F.slice("history", 1, 21))
    failed = seasonal_naive(missing, issue_date=issue)
    assert failed.predictions.count() == 0
    assert {r.reason_code for r in failed.exceptions.collect()} == {"MISSING_BASELINE_HISTORY"}


def origin_fixture(spark):
    ctx = context()
    issue = ctx.window[0] - timedelta(days=1)
    scope = spark.createDataFrame([("a", "CA_1"), ("b", "CA_1")], "item_id string, store_id string")
    run = ForecastRunContext(ctx.forecast_id(1885), "model", "snapshot", "v1", issue, "validation",
                             datetime(2026, 1, 1, tzinfo=timezone.utc))
    schema = ", ".join(f"{name} {kind}" for name, kind in zip(FORECAST_DAILY.field_names, DAILY_TYPES))
    rf = spark.createDataFrame([(run.forecast_run_id, "model", "snapshot", "v1", issue,
        issue + timedelta(days=h), h, "a", "CA_1", 3.0, 3.0, "validation", run.created_at)
        for h in range(1, 8)], schema)
    baseline = rf.select("source_snapshot_id", "feature_version", "item_id", "store_id", "issue_date",
        F.col("forecast_date").alias("target_date"), "horizon", F.lit(1.0).alias("predicted_units"),
        F.date_sub("forecast_date", 7).alias("baseline_source_date"),
        F.lit("observed_m5_unit_sales").alias("provenance"))
    labels = spark.createDataFrame([("a", "CA_1", issue + timedelta(days=h), float(h)) for h in (1, 2, 3)],
                                   "item_id string, store_id string, date date, unit_sales double")
    eligibility = scope.withColumn("status", F.when(F.col("item_id") == "a", "ELIGIBLE").otherwise("EXCLUDED"))
    return ctx, run, scope, rf, baseline, labels, eligibility


def test_partial_labels_pair_intersection_coverage_and_duplicate_rejection(spark):
    ctx, run, scope, rf, baseline, labels, eligibility = origin_fixture(spark)
    expected = expected_keys(scope, [(1885, run.issue_date)])
    aligned = align_origin(expected, rf, baseline.where("horizon != 2"), labels, eligibility,
                           run_context=run)
    assert {r.horizon for r in aligned.paired.collect()} == {1, 3}
    assert aligned.audit.count() == 14
    coverage = {r.method_id: r for r in aligned.coverage.collect()}
    assert coverage["random_forest"].candidate_rows == 14
    assert coverage["random_forest"].eligible_rows == 7
    assert coverage["random_forest"].predicted_rows == 7
    assert coverage["random_forest"].label_aligned_rows == 3
    assert coverage["random_forest"].paired_rows == 2
    assert coverage["seasonal_naive"].predicted_rows == 6
    assert coverage["random_forest"].fully_dropped_series == 1
    assert coverage["random_forest"].partially_paired_series == 1
    assert aligned.exceptions.where("reason_code = 'MISSING_LABEL'").count() == 11
    for bad_rf, bad_labels in ((rf.unionByName(rf), labels), (rf, labels.unionByName(labels))):
        with pytest.raises(ValueError, match="duplicate"):
            align_origin(expected, bad_rf, baseline, bad_labels, eligibility, run_context=run)


def test_paired_metrics_pool_sufficient_statistics_and_undefined_slices(spark):
    paired = spark.createDataFrame([(1, 0.0, 2.0, 1.0), (1, 10.0, 6.0, 9.0), (7, 0.0, 0.0, 2.0)],
        "horizon int, actual_units double, rf_prediction double, baseline_prediction double")
    metrics, stats = aggregate_metrics(paired, context())
    assert tuple(metrics.columns) == FORECAST_METRICS.field_names
    assert metrics.count() == 48
    values = {(r.method_id, r.metric_name, r.horizon): r for r in metrics.collect()}
    assert values["random_forest", "MAE", 0].metric_value == 2.0
    assert values["random_forest", "RMSE", 0].metric_value == pytest.approx(math.sqrt(20/3))
    assert values["random_forest", "WAPE", 0].metric_value == 60.0
    assert values["seasonal_naive", "WAPE", 0].metric_value == 40.0
    assert values["random_forest", "WAPE", 7].metric_value is None
    assert values["random_forest", "MAE", 2].observation_count == 0
    assert values["random_forest", "MAE", 2].metric_value is None
    assert all(r.forecast_run_id is None for r in values.values())
    assert values["seasonal_naive", "MAE", 0].model_run_id is None
    assert stats.where("method_id = 'random_forest' AND horizon = 0").first().sum_squared_error == 20.0
    validate_metrics(metrics, paired, context())
    with pytest.raises(ValueError, match="metrics"):
        validate_metrics(metrics.withColumn("metric_value", F.lit(42.0)), paired, context())
    empty, _ = aggregate_metrics(paired.limit(0), context())
    assert empty.count() == 48
    assert empty.where("metric_value IS NOT NULL OR observation_count != 0 OR sum_actual_units != 0").count() == 0
