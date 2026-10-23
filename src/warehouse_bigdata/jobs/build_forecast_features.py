"""J04: build and stage an immutable historical FORECAST_FEATURES v1.0 snapshot."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict

from warehouse_bigdata.common.config import load_hdfs_config, load_spark_config
from warehouse_bigdata.common.hdfs_namespace import validate_publication_namespace
from warehouse_bigdata.common.hdfs_permissions import prepare_spark_run_permissions
from warehouse_bigdata.common.spark_hdfs_client import SparkHdfsCliClient
from warehouse_bigdata.forecasting.feature_snapshot import (
    FeatureSnapshotResult,
    run_feature_snapshot,
    validate_feature_snapshot_parameters,
)


def run_build_forecast_features(
    *, source_version: str, source_snapshot_id: str, upstream_pipeline_run_id: str,
    feature_version: str, pipeline_run_id: str, hdfs_uri: str, spark_master: str,
    event_log_dir: str | None = None, output_partitions: int = 16,
    max_records_per_file: int = 1_000_000,
) -> FeatureSnapshotResult:
    """Prepare execution-scoped permissions before SparkContext; orchestrate only."""
    validate_feature_snapshot_parameters(
        source_version=source_version, source_snapshot_id=source_snapshot_id,
        upstream_pipeline_run_id=upstream_pipeline_run_id, feature_version=feature_version,
        pipeline_run_id=pipeline_run_id, output_partitions=output_partitions,
        max_records_per_file=max_records_per_file,
    )
    hdfs_config = load_hdfs_config({"WAREHOUSE_HDFS_URI": hdfs_uri})
    spark_config = load_spark_config({"WAREHOUSE_SPARK_MASTER_URL": spark_master})
    if not spark_config.master_url.startswith("spark://"):
        raise ValueError("J04 requires an explicit Spark Standalone master")
    hdfs = SparkHdfsCliClient()
    validate_publication_namespace(hdfs, hdfs_config)
    prepare_spark_run_permissions(hdfs, hdfs_config, pipeline_run_id=pipeline_run_id,
                                  event_log_dir=event_log_dir)
    from pyspark.sql import SparkSession
    builder = (SparkSession.builder.master(spark_config.master_url)
               .appName(f"warehouse-build-forecast-features-{pipeline_run_id}")
               .config("spark.ui.enabled", "false"))
    if event_log_dir is not None:
        builder = (builder.config("spark.eventLog.enabled", "true")
                   .config("spark.eventLog.dir", event_log_dir)
                   .config("spark.eventLog.compress", "false")
                   .config("spark.eventLog.rolling.enabled", "false"))
    spark = builder.getOrCreate()
    try:
        spark.sparkContext.setJobGroup(pipeline_run_id + ":features", "J04 historical feature snapshot", True)
        return run_feature_snapshot(
            spark, source_version=source_version, source_snapshot_id=source_snapshot_id,
            upstream_pipeline_run_id=upstream_pipeline_run_id, feature_version=feature_version,
            pipeline_run_id=pipeline_run_id, hdfs_config=hdfs_config,
            spark_master=spark_config.master_url, client=hdfs, output_partitions=output_partitions,
            max_records_per_file=max_records_per_file,
        )
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("source-version", "source-snapshot-id", "upstream-pipeline-run-id", "feature-version",
                 "pipeline-run-id", "hdfs-uri", "spark-master"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--event-log-dir")
    parser.add_argument("--output-partitions", type=int, default=16)
    parser.add_argument("--max-records-per-file", type=int, default=1_000_000)
    result = run_build_forecast_features(**vars(parser.parse_args()))
    print("J04_RESULT=" + json.dumps(asdict(result), sort_keys=True, separators=(",", ":")))
    raise SystemExit(0 if result.status in {"STAGED", "REUSED"} else 1)


if __name__ == "__main__":
    main()
