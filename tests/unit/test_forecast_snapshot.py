"""J06 input timing, explicit windows and frozen lineage gates."""

from datetime import date

import pytest
from tests.unit.test_scoring_features import spark as local_spark

from warehouse_bigdata.forecasting.forecast_snapshot import (
    reconcile_forecast_inputs, validate_forecast_parameters, validate_issue_window,
)


@pytest.fixture(scope="module")
def spark(local_spark):
    previous = local_spark.conf.get("spark.sql.session.timeZone")
    local_spark.conf.set("spark.sql.session.timeZone", "UTC")
    yield local_spark
    local_spark.conf.set("spark.sql.session.timeZone", previous)


def identities():
    return dict(source_version="sha256:" + "a" * 64, source_snapshot_id="snapshot",
                j03_pipeline_run_id="j03", j04_pipeline_run_id="j04", feature_version="v1",
                model_run_id="model", forecast_run_id="forecast", pipeline_run_id="j06",
                issue_date=date(2016, 3, 27), run_type="validation", seed=42)


def test_j06_parameters_and_historical_forward_windows():
    validate_forecast_parameters(**identities())
    manifest = dict(training_end="2016-03-27", validation_start="2016-03-28", validation_end="2016-04-24",
                    test_start="2016-04-25", test_end="2016-05-22", forecast_strategy="recursive_7_day")
    validate_issue_window(date(2016, 3, 27), "validation", manifest)
    validate_issue_window(date(2016, 5, 22), "forward_demo", manifest)
    for issue, kind in [(date(2016, 3, 26), "validation"), (date(2016, 4, 22), "validation"),
                        (date(2016, 5, 21), "forward_demo")]:
        with pytest.raises(ValueError):
            validate_issue_window(issue, kind, manifest)
    with pytest.raises(ValueError):
        validate_forecast_parameters(**{**identities(), "forecast_run_id": "../unsafe"})


def test_j06_lineage_requires_exact_model_j04_and_j03_bindings():
    upstream = dict(source_version=identities()["source_version"], source_snapshot_id="snapshot",
                    pipeline_run_id="j03", raw_manifest_digest="digest", metadata_digests={"marker": "digest"})
    feature = dict(completion_digest="completion", dataset_manifest_digest="dataset", input_binding_digest="binding",
                   upstream_evidence=upstream)
    model = {"input_binding": {"upstream_pipeline_run_id": "j04", "upstream_evidence": {
        k: feature[k] for k in ("completion_digest", "dataset_manifest_digest", "input_binding_digest")}}}
    reconcile_forecast_inputs(model, feature, upstream, j04_pipeline_run_id="j04")
    for key in ("completion_digest", "dataset_manifest_digest", "input_binding_digest"):
        with pytest.raises(ValueError, match="binding|lineage"):
            reconcile_forecast_inputs(model, {**feature, key: "poison"}, upstream, j04_pipeline_run_id="j04")
    with pytest.raises(ValueError, match="binding|lineage"):
        reconcile_forecast_inputs(model, feature, {**upstream, "raw_manifest_digest": "poison"},
                                  j04_pipeline_run_id="j04")


@pytest.mark.parametrize("corrupt", [False, True])
def test_j06_snapshot_actual_parquet_readback_and_frozen_retry(spark, tmp_path, monkeypatch, corrupt):
    import json
    from datetime import timedelta
    from pyspark.sql import functions as F
    from tests.unit.test_feature_snapshot import FeatureLocalHdfs
    from tests.unit.test_hdfs_namespace import CONFIG
    from tests.unit.test_forecast_cohort import sales, END
    from tests.unit.test_scoring_features import _frame, _rows
    from warehouse_bigdata.common.hdfs_paths import render_staging_path
    from warehouse_bigdata.forecasting import forecast_snapshot as module
    from warehouse_bigdata.forecasting.forecast_cohort import select_forecast_cohort

    hdfs = FeatureLocalHdfs(tmp_path)
    staging = render_staging_path(CONFIG.base_path, "j06")
    hdfs.mkdir_parents(staging)
    args = identities()
    upstream = {"verified": "selected-j03"}
    context = {"completion_digest": "c", "dataset_manifest_digest": "d", "input_binding_digest": "b",
               "upstream_evidence": upstream,
               "calendar_mapping": spark.createDataFrame([(END + timedelta(days=h), 1885 + h) for h in range(8)],
                                                          "target_date date, day_index int")}
    manifest = dict(training_end="2016-03-27", validation_start="2016-03-28", validation_end="2016-04-24",
                    test_start="2016-04-25", test_end="2016-05-22", forecast_strategy="recursive_7_day")
    model_metadata = {"manifest": manifest, "input_binding": {"upstream_pipeline_run_id": "j04", "upstream_evidence": {
        k: context[k] for k in ("completion_digest", "dataset_manifest_digest", "input_binding_digest")}}}
    monkeypatch.setattr(module, "read_validated_model_metadata", lambda **k: model_metadata)
    monkeypatch.setattr(module, "read_validated_feature_context", lambda *a, **k: context)
    source = _frame(spark, _rows(range(1858, 1886), item="item_000", store="CA_0"))
    monkeypatch.setattr(module, "read_validated_sales_enriched_snapshot", lambda *a, **k: (source, upstream))
    selected = select_forecast_cohort(sales(spark, 100), training_end=END, seed=42)
    monkeypatch.setattr(module, "select_forecast_cohort", lambda *a, **k: selected)
    class Model:
        def transform(self, frame):
            return frame.withColumn("raw_prediction", F.lit(0.375))
    monkeypatch.setattr(module, "load_validated_model", lambda *a, **k: Model())
    monkeypatch.setattr(module, "artifact_uri", lambda config, path: str(hdfs._local(path)))
    monkeypatch.setattr(module, "packaged_code_revision", lambda: "sha256:" + "c" * 64)
    monkeypatch.setattr(spark.sparkContext, "master", "spark://spark-master:7077")
    read = module._read_parquet
    if corrupt:
        def corrupted(session, uri):
            frame = read(session, uri)
            return frame.withColumn("predicted_units", F.lit(999.0)) if uri.endswith("/forecast_daily") else frame
        monkeypatch.setattr(module, "_read_parquet", corrupted)
    result = module.run_forecast_snapshot(spark, **args, hdfs_config=CONFIG, client=hdfs,
                                         spark_master="spark://spark-master:7077")
    if corrupt:
        assert result.status == "FAILED"
        assert "persisted forecast_daily" in str(result.failure_details)
        assert hdfs.exists(result.failure_report_path)
        assert not hdfs.exists(staging + "/publication/request.json")
    else:
        assert result.status == "STAGED", result.failure_details
        quality = json.loads(hdfs.read_remote(result.quality_report_path))
        assert quality["successful_series"] == 1 and quality["excluded_series"] == 999
        assert quality["row_count"] == 7 and quality["seven_day_total_units"] == 2.625
        assert "pipeline_run_id" not in json.loads(hdfs.read_remote(staging + "/candidates/forecast/scoring_manifest.json"))
        before = hdfs.read_remote(staging + "/input_binding.json")
        retry = module.run_forecast_snapshot(spark, **args, hdfs_config=CONFIG, client=hdfs,
                                            spark_master="spark://spark-master:7077")
        assert retry.status == "FAILED" and "frozen evidence" in str(retry.failure_details)
        assert hdfs.read_remote(staging + "/input_binding.json") == before
