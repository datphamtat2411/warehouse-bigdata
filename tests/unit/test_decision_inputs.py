"""Bounded synthetic fixtures for the read-only J07 input boundary."""

from copy import deepcopy
from datetime import date, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from warehouse_bigdata.optimization import decision_inputs as inputs


DAY = date(2016, 5, 22)


def fixture_rows():
    stores = list(inputs.EXPECTED_STORES)
    pairs = [(f"sku{i}", store) for i in range(10) for store in stores]
    selector = dict(
        schema_version="small-artifacts-v1", dataset_scope="demo_sample",
        source_version="sha256:" + "d" * 64, source_snapshot_id="small-snapshot",
        selection_digest="sha256:" + "a" * 64, feature_version="features-v1",
        model_run_id="model", forecast_run_id="forecast",
        **{f"j0{i}_pipeline_run_id": f"j0{i}-run" for i in range(2, 7)},
    )
    scope = dict(
        dataset_scope="demo_sample", selection_digest=selector["selection_digest"],
        selected_sku_count=10, selected_series_count=100, selected_store_ids=stores,
        historical_day_count=1941, expected_observation_count=194100,
        original_series_count=30490,
        selected_series=[dict(item_id=i, store_id=s) for i, s in pairs],
    )
    forecasts = [dict(
        forecast_run_id="forecast", model_run_id="model", source_snapshot_id="small-snapshot",
        feature_version="features-v1", issue_date=DAY.isoformat(),
        forecast_date=(DAY + timedelta(days=h)).isoformat(), horizon=h,
        item_id=i, store_id=s, raw_prediction=10.25, predicted_units=10.25,
        run_type="forward_demo", created_at="2016-05-22T00:00:00Z",
    ) for i, s in pairs[:2] for h in range(1, 8)]
    history = [dict(source_snapshot_id="small-snapshot", item_id=i, store_id=s,
                    date=DAY - timedelta(days=d), unit_sales=10)
               for i, s in pairs[:2] for d in range(7)]
    cohort = [dict(item_id=f"sku{i}", stores=stores, status="SELECTED",
                   selection_digest=selector["selection_digest"]) for i in range(10)]
    exceptions = [dict(item_id=i, store_id=s, stage="PREPARATION", horizon=0,
                       reason_code="INCOMPLETE_HISTORY") for i, s in pairs[2:]]
    return dict(selector=selector, scope=scope, forecast_rows=forecasts,
                history_rows=history, cohort_rows=cohort, exception_rows=exceptions,
                coverage_rows=[dict(candidate_series=100, successful_series=2,
                                    excluded_series=98, row_count=14)],
                upstream_evidence={"j03": "sha256:" + "b" * 64,
                                   "j06": "sha256:" + "c" * 64})


def test_bind_success_exclusions_and_immutable_evidence():
    rows = fixture_rows()
    result = inputs.bind_decision_inputs(**rows)
    assert len(result.predictions) == len(result.observed_sales) == 14
    assert len(result.eligible_pairs) == 2
    assert result.coverage["excluded_series"] == 98
    assert result.coverage["reason_counts"]["INCOMPLETE_HISTORY"] == 98
    assert len(result.exclusions) == 98
    rows["selector"]["model_run_id"] = "changed"
    assert result.selector["model_run_id"] == "model"
    with pytest.raises(TypeError):
        result.scope["selected_series"][0]["item_id"] = "changed"


@pytest.mark.parametrize("mutation", [
    lambda r: r["forecast_rows"].pop(),
    lambda r: r["forecast_rows"].append(r["forecast_rows"][0]),
    lambda r: r["forecast_rows"][0].update(model_run_id="wrong"),
    lambda r: r["forecast_rows"][0].update(issue_date="2016-05-21"),
    lambda r: r["forecast_rows"][0].update(forecast_date="2016-06-01"),
    lambda r: r["forecast_rows"][0].update(predicted_units=float("nan")),
    lambda r: r["forecast_rows"][0].update(extra="unexpected"),
    lambda r: r["history_rows"].pop(),
    lambda r: r["history_rows"].append(r["history_rows"][0]),
    lambda r: r["history_rows"][0].update(date=DAY + timedelta(days=1)),
    lambda r: r["history_rows"][0].update(source_snapshot_id="wrong"),
    lambda r: r["history_rows"][0].update(unit_sales=True),
    lambda r: r["history_rows"][0].update(unit_sales=-1),
    lambda r: r["exception_rows"].pop(),
    lambda r: r["exception_rows"][0].update(reason_code=""),
    lambda r: r["exception_rows"].append(r["exception_rows"][0]),
    lambda r: r["coverage_rows"][0].update(successful_series=3),
    lambda r: r["cohort_rows"][0].update(selection_digest="wrong"),
    lambda r: r["cohort_rows"].append(r["cohort_rows"][0]),
    lambda r: r["scope"].update(selection_digest="wrong"),
    lambda r: r["selector"].update(dataset_scope="full_m5"),
    lambda r: r["selector"].pop("forecast_run_id"),
])
def test_reject_malformed_or_unexplained_inputs(mutation):
    rows = fixture_rows()
    mutation(rows)
    with pytest.raises(ValueError):
        inputs.bind_decision_inputs(**rows)


@pytest.mark.parametrize("field", ["forecast_rows", "history_rows"])
def test_row_bounds(field):
    rows = fixture_rows()
    rows[field] = rows[field][:1] * 701
    with pytest.raises(ValueError, match="700"):
        inputs.bind_decision_inputs(**rows)


def test_collection_checks_count_before_collect():
    frame = Mock()
    frame.count.return_value = 701
    with pytest.raises(ValueError, match="700"):
        inputs.collect_bounded(frame, 700, "forecast")
    frame.collect.assert_not_called()


def test_complete_700_row_boundary():
    rows = fixture_rows()
    forecast, sale = rows["forecast_rows"][0], rows["history_rows"][0]
    rows["forecast_rows"] = [dict(forecast, item_id=p["item_id"], store_id=p["store_id"],
        horizon=h, forecast_date=(DAY + timedelta(days=h)).isoformat())
        for p in rows["scope"]["selected_series"] for h in range(1, 8)]
    rows["history_rows"] = [dict(sale, item_id=p["item_id"], store_id=p["store_id"],
        date=DAY - timedelta(days=d))
        for p in rows["scope"]["selected_series"] for d in range(7)]
    rows["exception_rows"] = []
    rows["coverage_rows"] = [dict(candidate_series=100, successful_series=100,
                                  excluded_series=0, row_count=700)]
    result = inputs.bind_decision_inputs(**rows)
    assert len(result.predictions) == len(result.observed_sales) == 700


def test_history_reader_filters_and_binds_source_before_collection(monkeypatch):
    from pyspark.sql import functions as F
    from warehouse_bigdata.processing import enrichment_analytics_snapshot as j03

    rows = fixture_rows()
    sales, keys, joined, window, projected = (Mock() for _ in range(5))
    sales.join.return_value = joined
    joined.where.return_value = window
    window.columns = ["item_id", "store_id", "date", "unit_sales"]
    window.withColumn.return_value = window
    window.select.return_value = projected
    evidence = dict(source_snapshot_id="small-snapshot")
    reader = Mock(return_value=(sales, evidence))
    monkeypatch.setattr(j03, "read_validated_sales_enriched_snapshot", reader)
    column = Mock()
    monkeypatch.setattr(F, "col", lambda n: column)
    monkeypatch.setattr(F, "lit", lambda v: v)
    collect = Mock(return_value=rows["history_rows"])
    monkeypatch.setattr(inputs, "collect_bounded", collect)
    spark = Mock()
    spark.createDataFrame.return_value = keys
    actual, bound = inputs.read_history(spark, rows["selector"], {("sku0", "CA_1")},
                                       client=Mock(), config=Mock())
    sales.join.assert_called_once_with(keys, ["item_id", "store_id"], "left_semi")
    column.between.assert_called_once_with(DAY - timedelta(days=6), DAY)
    window.withColumn.assert_called_once_with("source_snapshot_id", "small-snapshot")
    collect.assert_called_once_with(projected, 700, "history")
    assert actual == rows["history_rows"] and bound == evidence
    assert reader.call_args.kwargs["pipeline_run_id"] == "j03-run"
    sales.collect.assert_not_called()


def test_missing_environment_selector(monkeypatch):
    monkeypatch.delenv("WAREHOUSE_SMALL_ARTIFACTS", raising=False)
    with pytest.raises(ValueError, match="WAREHOUSE_SMALL_ARTIFACTS"):
        inputs.read_decision_inputs(Mock(), client=Mock(), config=Mock())


def test_upstream_failure_precedes_data_reads(monkeypatch):
    rows = fixture_rows()
    monkeypatch.setattr(inputs, "load_small_artifacts", lambda: rows["selector"])
    preflight = Mock(side_effect=ValueError("unsealed or non-VALIDATED"))
    monkeypatch.setattr(inputs, "validate_small_publications", preflight)
    spark = Mock()
    with pytest.raises(ValueError, match="non-VALIDATED"):
        inputs.read_decision_inputs(spark, client=Mock(), config=Mock())
    spark.read.parquet.assert_not_called()
    assert preflight.call_args.kwargs["through"] == "j06"


def test_adapter_binds_selected_reads_without_writes(monkeypatch):
    rows = fixture_rows()
    monkeypatch.setattr(inputs, "load_small_artifacts", lambda: rows["selector"])
    preflight = Mock()
    monkeypatch.setattr(inputs, "validate_small_publications", preflight)
    monkeypatch.setattr(inputs, "read_scope", lambda *a, **k: rows["scope"])
    manifest = {"row_count": 14}
    monkeypatch.setattr(inputs, "read_json", lambda c, p: manifest)
    validate = Mock()
    monkeypatch.setattr(inputs, "validate_forecast_evidence", validate)
    monkeypatch.setattr(inputs, "read_history", Mock(return_value=(rows["history_rows"],
        dict(scope=rows["scope"], source_snapshot_id="small-snapshot",
             source_version=rows["selector"]["source_version"], pipeline_run_id="j03-run", metadata_digests={}))))
    names = dict(forecast_daily="forecast_rows", cohort_selection="cohort_rows",
                 coverage="coverage_rows", coverage_exceptions="exception_rows")
    frames = {name: Mock() for name in names}
    monkeypatch.setattr(inputs, "collect_bounded", lambda f, *a: deepcopy(
        rows[names[next(n for n, frame in frames.items() if frame is f)]]))
    monkeypatch.setattr(inputs, "require_forecast_schema", Mock())
    spark = Mock()
    spark.conf.get.return_value = "UTC"
    spark.read.parquet.side_effect = lambda p: frames[p.rsplit("/", 1)[-1]]
    client = SimpleNamespace(read_remote=Mock())
    result = inputs.read_decision_inputs(spark, client=client,
        config=SimpleNamespace(base_path="/warehouse", uri="hdfs://test"))
    assert len(result.predictions) == 14
    assert validate.call_args.kwargs["pipeline_run_id"] == "j06-run"
    assert all("forecast" in call.args[0] for call in spark.read.parquet.call_args_list)
    assert not any("write" in str(call) for call in spark.mock_calls)


def test_physical_schema_rejected():
    frame = SimpleNamespace(columns=["wrong"], schema=[])
    with pytest.raises(ValueError, match="schema"):
        inputs.require_forecast_schema(frame)
