"""Tiny persisted evaluation with a frozen inference double, without fitting."""

import json
from datetime import date, timedelta
from types import SimpleNamespace

import pytest
from pyspark.sql import functions as F

from tests.unit.test_scoring_features import spark as local_spark, _frame, _rows
from tests.unit.test_feature_snapshot import FeatureLocalHdfs
from tests.unit.test_hdfs_namespace import CONFIG
from warehouse_bigdata.forecasting import evaluation_snapshot as module
from warehouse_bigdata.forecasting.evaluation import cohort_content_digest, freeze_protocol
from warehouse_bigdata.forecasting.recursive_forecasting import DAILY_TYPES
from warehouse_bigdata.contracts.datasets import FORECAST_DAILY


@pytest.fixture(scope="module")
def spark(local_spark):
    local_spark.conf.set("spark.sql.session.timeZone", "UTC")
    return local_spark


@pytest.mark.parametrize("corrupt", [False, True, "test", "freeze"])
def test_four_origins_frozen_model_parquet_boundary_and_failed_retry(spark, tmp_path, monkeypatch, corrupt):
    hdfs = FeatureLocalHdfs(tmp_path)
    staging = CONFIG.base_path + "/staging/pipeline_run_id=execution"
    hdfs.mkdir_parents(staging)
    source = _frame(spark, _rows(range(1858, 1942), item="item_000", store="CA_0"))
    upstream = {"verified": "j03"}
    calendar = spark.createDataFrame([(date(2011, 1, 29) + timedelta(days=d-1), d)
        for d in range(1885, 1942)], "target_date date, day_index int")
    feature = dict(completion_digest="c", dataset_manifest_digest="d", input_binding_digest="b",
                   upstream_evidence=upstream, calendar_mapping=calendar)
    metadata = {"manifest": dict(training_end="2016-03-27", validation_start="2016-03-28",
        validation_end="2016-04-24", test_start="2016-04-25", test_end="2016-05-22",
        forecast_strategy="recursive_7_day", artifact_checksum="sha256:" + "b"*64),
        "input_binding": {"upstream_pipeline_run_id": "j04", "upstream_evidence": {
            k: feature[k] for k in ("completion_digest", "dataset_manifest_digest", "input_binding_digest")}}}
    monkeypatch.setattr(module, "read_validated_model_metadata", lambda **k: metadata)
    monkeypatch.setattr(module, "read_validated_feature_context", lambda *a, **k: feature)
    monkeypatch.setattr(module, "read_validated_sales_enriched_snapshot", lambda *a, **k: (source, upstream))
    selection = spark.createDataFrame([(f"item_{i:03}", [f"CA_{s}" for s in range(10)], "SELECTED",
        "dept_1", "cat_1", 1, 42) for i in range(100)],
        "item_id string, stores array<string>, status string, dept_id string, cat_id string, volume_tertile int, seed int")
    monkeypatch.setattr(module, "select_forecast_cohort", lambda *a, **k: SimpleNamespace(evidence=selection))
    calls, released, loads = [], [], []
    frozen_model = object()
    monkeypatch.setattr(module, "_load_frozen_model", lambda *a, **k: loads.append(1) or frozen_model)
    def score(model, prepared, *, known_calendar, run_context):
        assert model is frozen_model
        assert hdfs.exists(staging + "/input_binding.json")
        assert json.loads(hdfs.read_remote(staging + "/input_binding.json"))["freeze"]
        ctx = run_context
        calls.append(ctx.issue_date)
        assert prepared.state.select(F.array_max(F.transform("history", lambda e: e["date"]))).first()[0] == ctx.issue_date
        rows = [(ctx.forecast_run_id, ctx.model_run_id, ctx.source_snapshot_id, ctx.feature_version,
            ctx.issue_date, ctx.issue_date + timedelta(days=h), h, "item_000", "CA_0", 2.0, 2.0,
            ctx.run_type, ctx.created_at) for h in range(1, 8)]
        daily = spark.createDataFrame(rows, ", ".join(f"{n} {t}" for n, t in zip(FORECAST_DAILY.field_names, DAILY_TYPES)))
        exceptions = prepared.eligibility.where("status = 'EXCLUDED'").select("item_id", "store_id",
            F.lit("PREPARATION").alias("stage"), F.lit(0).alias("horizon"), F.explode("reason_codes").alias("reason_code"))
        return SimpleNamespace(daily=daily, exceptions=exceptions, release=lambda: released.append(ctx.issue_date))
    monkeypatch.setattr(module, "forecast_recursive", score)
    monkeypatch.setattr(module, "artifact_uri", lambda config, path: str(hdfs._local(path)))
    monkeypatch.setattr(module, "packaged_code_revision", lambda: "sha256:" + "c"*64)
    monkeypatch.setattr(spark.sparkContext, "master", "spark://spark-master:7077")
    read = module._read_parquet
    if corrupt is True:
        def corrupted(session, uri):
            frame = read(session, uri)
            return frame.withColumn("predicted_units", F.lit(999.0)) if uri.endswith("/forecast_daily") else frame
        monkeypatch.setattr(module, "_read_parquet", corrupted)
    args = dict(source_version="sha256:" + "a"*64, source_snapshot_id="snapshot", j03_pipeline_run_id="j03",
        j04_pipeline_run_id="j04", feature_version="v1", model_run_id="model", evaluation_run_id="eval",
        pipeline_run_id="execution", run_type="validation", hdfs_config=CONFIG, client=hdfs,
        spark_master="spark://spark-master:7077")
    if corrupt in ("test", "freeze"):
        args.update(run_type="test", validation_evaluation_run_id="selected-validation")
        source_binding = {k: args[k] for k in ("source_version", "source_snapshot_id", "j03_pipeline_run_id",
            "j04_pipeline_run_id", "feature_version", "model_run_id")}
        source_binding.update(j03_evidence=upstream,
            j04_evidence={k: v for k, v in feature.items() if k != "calendar_mapping"})
        frozen = freeze_protocol(model_metadata=metadata, cohort_digest=cohort_content_digest(selection), seed=42,
            code_revision="sha256:" + "c"*64, source_binding=source_binding)
        if corrupt == "freeze":
            frozen["code_revision"] = "poison"
        monkeypatch.setattr(module, "read_validated_validation", lambda *a: {
            "artifact_path": "/selected-validation", "manifest": {"selected": "sealed"}, "binding": {"freeze": frozen}})
        monkeypatch.setattr(module, "select_forecast_cohort", lambda *a, **k: pytest.fail("test reselected the frozen cohort"))
        monkeypatch.setattr(module, "_read_parquet", lambda session, uri: selection
            if uri == str(hdfs._local("/selected-validation/cohort_selection")) else read(session, uri))
    result = module.run_forecast_evaluation(spark, **args)
    if corrupt == "freeze":
        assert result.status == "FAILED" and "freeze" in str(result.failure_details)
        assert calls == loads == []
        assert not hdfs.exists(staging + "/publication/request.json")
    elif corrupt is True:
        assert result.status == "FAILED"
        assert "persisted forecast_daily" in str(result.failure_details)
        assert hdfs.exists(result.failure_report_path)
        assert not hdfs.exists(staging + "/publication/request.json")
    else:
        assert result.status == "STAGED", result.failure_details
        start = date(2016, 4, 24) if corrupt == "test" else date(2016, 3, 27)
        assert calls == [start + timedelta(days=7*i) for i in range(4)]
        assert loads == [1] and released == calls
        candidate = staging + "/candidates/evaluation"
        assert spark.read.parquet(str(hdfs._local(candidate + "/metrics"))).count() == 48
        assert spark.read.parquet(str(hdfs._local(candidate + "/paired"))).count() == 28
        assert "pipeline_run_id" not in json.loads(hdfs.read_remote(candidate + "/evaluation_manifest.json"))
        before = hdfs.read_remote(staging + "/input_binding.json")
        retry = module.run_forecast_evaluation(spark, **args)
        assert retry.status == "FAILED" and "frozen evidence" in str(retry.failure_details)
        assert len(calls) == 4 and hdfs.read_remote(staging + "/input_binding.json") == before
