"""Spark entry points must establish HDFS access before creating SparkContext."""

from __future__ import annotations

import sys
from types import ModuleType, SimpleNamespace

import pytest

from warehouse_bigdata.jobs import build_forecast_features, enrich_analytics, normalize_sales_daily

HDFS_URI = "hdfs://hdfs-namenode:8020"
EVENT_LOG = HDFS_URI + "/data/warehouse/staging/pipeline_run_id=run-001/events"


class _Builder:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    def master(self, value: str) -> _Builder:
        self.events.append("builder.master")
        return self

    def appName(self, value: str) -> _Builder:
        self.events.append("builder.appName")
        return self

    def config(self, key: str, value: str) -> _Builder:
        self.events.append(f"builder.config:{key}")
        return self

    def getOrCreate(self) -> SimpleNamespace:
        self.events.append("spark.getOrCreate")
        return SimpleNamespace(
            sparkContext=SimpleNamespace(
                setJobGroup=lambda *args: self.events.append("spark.setJobGroup")
            ),
            stop=lambda: self.events.append("spark.stop"),
        )


@pytest.mark.parametrize(
    ("module", "runner_name", "processor_name"),
    [
        (normalize_sales_daily, "run_normalize_sales_daily", "run_normalization_snapshot"),
        (enrich_analytics, "run_enrich_analytics", "run_enrichment_analytics_snapshot"),
        (build_forecast_features, "run_build_forecast_features", "run_feature_snapshot"),
    ],
)
def test_run_jobs_prepare_acl_before_spark_context(
    monkeypatch: pytest.MonkeyPatch,
    module: ModuleType,
    runner_name: str,
    processor_name: str,
) -> None:
    events: list[str] = []
    builder = _Builder(events)
    monkeypatch.setitem(
        sys.modules,
        "pyspark.sql",
        SimpleNamespace(SparkSession=SimpleNamespace(builder=builder)),
    )
    monkeypatch.setitem(sys.modules, "pyspark", ModuleType("pyspark"))
    monkeypatch.setattr(module, "SparkHdfsCliClient", lambda: object())
    monkeypatch.setattr(module, "validate_publication_namespace", lambda *args: None)
    monkeypatch.setattr(
        module,
        "prepare_spark_run_permissions",
        lambda *args, **kwargs: events.append("permissions.prepare"),
    )
    monkeypatch.setattr(module, processor_name, lambda *args, **kwargs: "result")

    runner = getattr(module, runner_name)
    extra = ({"upstream_pipeline_run_id": "j03-run", "feature_version": "historical_v1"}
             if module is build_forecast_features else {})
    result = runner(
        source_version="sha256:" + "a" * 64,
        source_snapshot_id="snapshot-001",
        pipeline_run_id="run-001",
        hdfs_uri=HDFS_URI,
        spark_master="spark://spark-master:7077",
        event_log_dir=EVENT_LOG,
        **extra,
    )

    assert result == "result"
    assert events.index("permissions.prepare") < events.index("spark.getOrCreate")
