"""J05: train and stage one immutable global Random Forest PipelineModel."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict

from warehouse_bigdata.common.config import load_hdfs_config, load_spark_config
from warehouse_bigdata.common.hdfs_namespace import validate_publication_namespace
from warehouse_bigdata.common.hdfs_permissions import prepare_spark_run_permissions
from warehouse_bigdata.common.spark_hdfs_client import SparkHdfsCliClient
from warehouse_bigdata.forecasting.training import TrainingConfig, TrainingError
from warehouse_bigdata.forecasting.training_snapshot import run_model_training, validate_training_parameters


def run_train_forecast_model(*, source_version, source_snapshot_id, upstream_pipeline_run_id,
        feature_version, model_run_id, pipeline_run_id, hdfs_uri, spark_master,
        training_cap=150000, sampling_seed=42, resource_justification=None,
        pilot_evidence_uri=None, event_log_dir=None):
    identities = dict(source_version=source_version, source_snapshot_id=source_snapshot_id,
        upstream_pipeline_run_id=upstream_pipeline_run_id, feature_version=feature_version,
        model_run_id=model_run_id, pipeline_run_id=pipeline_run_id)
    validate_training_parameters(**identities)
    config = TrainingConfig(training_cap, sampling_seed, resource_justification, pilot_evidence_uri)
    if training_cap > 10000 and not (pilot_evidence_uri or "").strip():
        raise TrainingError("target training requires an explicit measured pilot evidence reference")
    hdfs_config = load_hdfs_config({"WAREHOUSE_HDFS_URI": hdfs_uri})
    spark_config = load_spark_config({"WAREHOUSE_SPARK_MASTER_URL": spark_master})
    if not spark_config.master_url.startswith("spark://"):
        raise TrainingError("J05 requires an explicit Spark Standalone master")
    hdfs = SparkHdfsCliClient()
    validate_publication_namespace(hdfs, hdfs_config)
    prepare_spark_run_permissions(hdfs, hdfs_config, pipeline_run_id=pipeline_run_id, event_log_dir=event_log_dir)
    from pyspark.sql import SparkSession

    builder = (SparkSession.builder.master(spark_config.master_url)
               .appName("warehouse-train-forecast-model-" + pipeline_run_id).config("spark.ui.enabled", "false"))
    if event_log_dir is not None:
        builder = (builder.config("spark.eventLog.enabled", "true").config("spark.eventLog.dir", event_log_dir)
                   .config("spark.eventLog.compress", "false").config("spark.eventLog.rolling.enabled", "false"))
    spark = builder.getOrCreate()
    try:
        return run_model_training(spark, **identities, config=config, hdfs_config=hdfs_config,
                                  client=hdfs, spark_master=spark_master)
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("source-version", "source-snapshot-id", "upstream-pipeline-run-id", "feature-version",
                 "model-run-id", "pipeline-run-id", "hdfs-uri", "spark-master"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--training-cap", type=int, default=150000)
    parser.add_argument("--sampling-seed", type=int, default=42)
    parser.add_argument("--resource-justification")
    parser.add_argument("--pilot-evidence-uri")
    parser.add_argument("--event-log-dir")
    result = run_train_forecast_model(**vars(parser.parse_args()))
    print("J05_RESULT=" + json.dumps(asdict(result), sort_keys=True, separators=(",", ":")))
    raise SystemExit(0 if result.status == "STAGED" else 1)


if __name__ == "__main__":
    main()
