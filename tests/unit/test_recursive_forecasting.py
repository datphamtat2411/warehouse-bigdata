"""J06 sequential fractional feedback and complete-series publication checks."""

from datetime import UTC, datetime, timedelta

import pytest
from pyspark.sql import functions as F

from tests.unit.test_scoring_features import ISSUE, _prepare, spark as local_spark
from warehouse_bigdata.forecasting.recursive_forecasting import (
    ForecastError, ForecastRunContext, forecast_recursive, validate_forecast_daily,
)


@pytest.fixture(scope="module")
def spark(local_spark):
    previous = local_spark.conf.get("spark.sql.session.timeZone")
    local_spark.conf.set("spark.sql.session.timeZone", "UTC")
    yield local_spark
    local_spark.conf.set("spark.sql.session.timeZone", previous)


def context():
    return ForecastRunContext("forecast-1", "model-1", "snapshot_1", "features_v1",
                              ISSUE, "validation", datetime(2026, 1, 1, tzinfo=UTC))


def calendar(spark):
    return spark.createDataFrame([(ISSUE + timedelta(days=h),) for h in range(1, 8)], "date date")


class FractionalModel:
    def transform(self, frame):
        return frame.withColumn("raw_prediction", F.col("lag_1") + 0.125 +
                                F.coalesce(F.col("last_known_price"), F.lit(2.5)) - 2.5 +
                                F.when(F.col("dept_id") == "POISON", 999.0).otherwise(0.0))


def test_j06_seven_fractional_feedback_cutoff_and_cleanup(spark):
    from tests.unit.test_scoring_features import _rows
    poisoned = _rows()
    for row in poisoned:
        if row["date"] > ISSUE:
            row.update(unit_sales=999999, sell_price=999999.0, dept_id="POISON", series_id="POISON")
    result = forecast_recursive(FractionalModel(), _prepare(spark, poisoned),
                                known_calendar=calendar(spark), run_context=context())
    try:
        rows = result.daily.orderBy("horizon").collect()
        assert [r.predicted_units for r in rows] == [34 + h * 0.125 for h in range(1, 8)]
        assert result.exceptions.count() == 0
        assert result.coverage.first().successful_series == 1
        validate_forecast_daily(result.daily, run_context=context(), series_scope=result.series_scope)
        with pytest.raises(ForecastError, match="complete|horizon"):
            validate_forecast_daily(result.daily.where("horizon < 7"), run_context=context(),
                                    series_scope=result.series_scope)
        with pytest.raises(ForecastError, match="duplicate|unique"):
            validate_forecast_daily(result.daily.unionByName(result.daily), run_context=context(),
                                    series_scope=result.series_scope)
    finally:
        result.release()
    assert not result.daily.is_cached


@pytest.mark.parametrize("raw", [-2.5, float("nan"), float("inf"), None])
def test_j06_clipping_nonfinite_and_zero_coverage(spark, raw):
    class Model:
        def transform(self, frame):
            return frame.withColumn("raw_prediction", F.lit(raw).cast("double"))
    result = forecast_recursive(Model(), _prepare(spark), known_calendar=calendar(spark), run_context=context())
    try:
        if raw == -2.5:
            assert result.daily.count() == 7
            assert result.daily.where("predicted_units != 0.0").count() == 0
        else:
            assert result.daily.count() == 0
            assert result.coverage.first().excluded_series == 1
            assert result.exceptions.first().reason_code == "NONFINITE_PREDICTION"
    finally:
        result.release()


def test_j06_late_failure_removes_partial_series_and_missing_calendar_fails(spark):
    class Model:
        def transform(self, frame):
            return frame.withColumn("raw_prediction", F.when(F.col("target_date") == ISSUE + timedelta(days=4),
                                                             float("nan")).otherwise(0.375))
    result = forecast_recursive(Model(), _prepare(spark), known_calendar=calendar(spark), run_context=context())
    try:
        assert result.daily.count() == 0
        assert result.exceptions.first().horizon == 4
    finally:
        result.release()
    with pytest.raises(ValueError, match="calendar|coverage"):
        forecast_recursive(Model(), _prepare(spark), known_calendar=calendar(spark).where("1=0"),
                           run_context=context())


def test_j06_saved_t01_preprocessing_compatibility_without_inference_fit(spark, tmp_path, monkeypatch):
    from pyspark.ml import PipelineModel
    from warehouse_bigdata.forecasting.training import TrainingConfig, build_training_pipeline
    from warehouse_bigdata.forecasting.model_artifacts import validate_scoring_pipeline
    from warehouse_bigdata.forecasting.scoring_features import build_scoring_features
    prepared = _prepare(spark)
    features = build_scoring_features(prepared.state, issue_date=ISSUE, target_date=ISSUE + timedelta(days=1),
                                      known_calendar=calendar(spark)).features
    # Only fixture construction fits; inference uses the native saved T01 stages.
    fixture = features.crossJoin(spark.range(20)).drop("id").withColumn("label", F.lit(0.375))
    fitted = build_training_pipeline(TrainingConfig()).fit(fixture)
    fitted.write().save(str(tmp_path / "model"))
    loaded = PipelineModel.load(str(tmp_path / "model"))
    validate_scoring_pipeline(loaded)
    monkeypatch.setattr("pyspark.ml.Pipeline.fit", lambda *a, **k: pytest.fail("inference refit"))
    unknown = features.withColumn("store_id", F.lit("unseen")).withColumn("dept_id", F.lit("unseen"))
    assert loaded.transform(unknown).select("raw_prediction").first().raw_prediction == 0.375
    result = forecast_recursive(loaded, prepared, known_calendar=calendar(spark), run_context=context())
    try:
        assert result.daily.count() == 7
    finally:
        result.release()


def test_j06_missing_history_is_explicit_and_structural_pipeline_errors_fail(spark):
    prepared = _prepare(spark, scope=[("item_1", "CA_1"), ("missing", "CA_1")])
    result = forecast_recursive(FractionalModel(), prepared, known_calendar=calendar(spark), run_context=context())
    try:
        assert result.daily.count() == 7
        assert result.coverage.first().excluded_series == 1
        assert result.exceptions.where("item_id = 'missing' AND reason_code = 'MISSING_HISTORY'").count() == 1
    finally:
        result.release()
    class Broken:
        def transform(self, frame):
            return frame
    with pytest.raises(ForecastError, match="raw_prediction"):
        forecast_recursive(Broken(), prepared, known_calendar=calendar(spark), run_context=context())
