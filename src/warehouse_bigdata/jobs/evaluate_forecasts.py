"""J05/J06: stage a locked four-origin paired forecast evaluation phase."""

import argparse
import json
from dataclasses import asdict

from warehouse_bigdata.common.config import load_hdfs_config, load_spark_config
from warehouse_bigdata.common.hdfs_namespace import validate_publication_namespace
from warehouse_bigdata.common.hdfs_permissions import prepare_spark_run_permissions
from warehouse_bigdata.common.scope_evidence import SCOPED_METADATA_LIMIT
from warehouse_bigdata.common.spark_hdfs_client import SparkHdfsCliClient
from warehouse_bigdata.forecasting.evaluation_snapshot import (
    run_forecast_evaluation,
    validate_evaluation_parameters,
)
from warehouse_bigdata.forecasting.recursive_forecasting import ForecastError


def run_evaluate_forecasts(
    *,
    source_version,
    source_snapshot_id,
    j03_pipeline_run_id,
    j04_pipeline_run_id,
    feature_version,
    model_run_id,
    evaluation_run_id,
    pipeline_run_id,
    run_type,
    hdfs_uri,
    spark_master,
    seed=42,
    validation_evaluation_run_id=None,
    event_log_dir=None,
    dataset_scope=None,
    confirm_large_workload=False,
):
    identities = dict(
        source_version=source_version,
        source_snapshot_id=source_snapshot_id,
        j03_pipeline_run_id=j03_pipeline_run_id,
        j04_pipeline_run_id=j04_pipeline_run_id,
        feature_version=feature_version,
        model_run_id=model_run_id,
        evaluation_run_id=evaluation_run_id,
        pipeline_run_id=pipeline_run_id,
        run_type=run_type,
        validation_evaluation_run_id=validation_evaluation_run_id,
    )
    validate_evaluation_parameters(**identities, seed=seed)
    hdfs_config = load_hdfs_config({"WAREHOUSE_HDFS_URI": hdfs_uri})
    spark_config = load_spark_config({"WAREHOUSE_SPARK_MASTER_URL": spark_master})
    if not spark_config.master_url.startswith("spark://"):
        raise ForecastError("evaluation requires an explicit Spark Standalone master")
    hdfs = SparkHdfsCliClient(metadata_limit=SCOPED_METADATA_LIMIT)
    validate_publication_namespace(hdfs, hdfs_config)
    prepare_spark_run_permissions(
        hdfs, hdfs_config, pipeline_run_id=pipeline_run_id, event_log_dir=event_log_dir
    )
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master(spark_master)
        .appName("warehouse-evaluate-forecasts-" + pipeline_run_id)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
    )
    if event_log_dir is not None:
        builder = (
            builder.config("spark.eventLog.enabled", "true")
            .config("spark.eventLog.dir", event_log_dir)
            .config("spark.eventLog.compress", "false")
            .config("spark.eventLog.rolling.enabled", "false")
        )
    spark = builder.getOrCreate()
    try:
        return run_forecast_evaluation(
            spark,
            **identities,
            hdfs_config=hdfs_config,
            client=hdfs,
            spark_master=spark_master,
            seed=seed,
            event_log_dir=event_log_dir,
            dataset_scope=dataset_scope,
            confirm_large_workload=confirm_large_workload,
        )
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in (
        "source-version",
        "source-snapshot-id",
        "j03-pipeline-run-id",
        "j04-pipeline-run-id",
        "feature-version",
        "model-run-id",
        "evaluation-run-id",
        "pipeline-run-id",
        "hdfs-uri",
        "spark-master",
    ):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--run-type", choices=("validation", "test"), required=True)
    parser.add_argument("--validation-evaluation-run-id")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--event-log-dir")
    parser.add_argument("--dataset-scope", choices=("demo_sample", "full_m5"))
    parser.add_argument("--confirm-large-workload", action="store_true")
    result = run_evaluate_forecasts(**vars(parser.parse_args()))
    print("EVALUATION_RESULT=" + json.dumps(asdict(result), sort_keys=True, separators=(",", ":")))
    raise SystemExit(0 if result.status == "STAGED" else 1)


if __name__ == "__main__":
    main()
