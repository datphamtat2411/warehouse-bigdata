"""Spark entry point for the immutable Contract v1 M5 sales snapshot."""

from __future__ import annotations

import argparse
import json

from warehouse_bigdata.common.config import load_hdfs_config, load_spark_config
from warehouse_bigdata.common.hdfs_namespace import validate_publication_namespace
from warehouse_bigdata.common.hdfs_permissions import prepare_spark_run_permissions
from warehouse_bigdata.common.scope_evidence import SCOPED_METADATA_LIMIT
from warehouse_bigdata.common.spark_hdfs_client import SparkHdfsCliClient
from warehouse_bigdata.contracts.dataset_scope import DEMO_SAMPLE, FULL_M5, require_scope_execution
from warehouse_bigdata.processing.normalization_snapshot import (
    NormalizationSnapshotConfig,
    NormalizationSnapshotResult,
    run_normalization_snapshot,
)


def run_normalize_sales_daily(
    *,
    source_version: str,
    source_snapshot_id: str,
    pipeline_run_id: str,
    hdfs_uri: str,
    spark_master: str,
    days_per_chunk: int = 128,
    expected_day_count: int = 1_941,
    event_log_dir: str | None = None,
    dataset_scope: str = DEMO_SAMPLE,
    confirm_large_workload: bool = False,
) -> NormalizationSnapshotResult:
    """Run one explicitly identified normalization attempt on HDFS/Spark."""
    require_scope_execution(dataset_scope, confirm_large_workload=confirm_large_workload)
    if expected_day_count != 1941:
        raise ValueError(
            "real-M5 execution requires all 1941 days; reduced-history workflows are retired"
        )
    hdfs_config = load_hdfs_config({"WAREHOUSE_HDFS_URI": hdfs_uri})
    spark_config = load_spark_config({"WAREHOUSE_SPARK_MASTER_URL": spark_master})
    hdfs = SparkHdfsCliClient(metadata_limit=SCOPED_METADATA_LIMIT)
    validate_publication_namespace(hdfs, hdfs_config)
    prepare_spark_run_permissions(
        hdfs,
        hdfs_config,
        pipeline_run_id=pipeline_run_id,
        event_log_dir=event_log_dir,
    )

    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master(spark_config.master_url)
        .appName(f"warehouse-sales-daily-{pipeline_run_id}")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")
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
        spark.sparkContext.setJobGroup(
            f"{pipeline_run_id}:normalization", "M04 J02 normalization and immutable write", True
        )
        return run_normalization_snapshot(
            spark,
            source_version=source_version,
            source_snapshot_id=source_snapshot_id,
            pipeline_run_id=pipeline_run_id,
            hdfs_config=hdfs_config,
            spark_master=spark_config.master_url,
            client=hdfs,
            defer_publication=True,
            dataset_scope=dataset_scope,
            confirm_large_workload=confirm_large_workload,
            config=NormalizationSnapshotConfig(
                days_per_chunk=days_per_chunk,
                expected_series_count=100 if dataset_scope == DEMO_SAMPLE else 30490,
                expected_day_count=expected_day_count,
            ),
        )
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-version", required=True, metavar="SHA256_ID")
    parser.add_argument("--source-snapshot-id", required=True, metavar="ID")
    parser.add_argument("--pipeline-run-id", required=True, metavar="ID")
    parser.add_argument("--hdfs-uri", required=True, metavar="HDFS_URI")
    parser.add_argument("--spark-master", required=True, metavar="SPARK_URI")
    parser.add_argument("--days-per-chunk", type=int, default=128, metavar="DAYS")
    parser.add_argument("--expected-day-count", type=int, default=1_941, metavar="DAYS")
    parser.add_argument("--event-log-dir", metavar="HDFS_URI")
    parser.add_argument("--dataset-scope", choices=(DEMO_SAMPLE, FULL_M5), default=DEMO_SAMPLE)
    parser.add_argument("--confirm-large-workload", action="store_true")
    args = parser.parse_args()
    result = run_normalize_sales_daily(
        source_version=args.source_version,
        source_snapshot_id=args.source_snapshot_id,
        pipeline_run_id=args.pipeline_run_id,
        hdfs_uri=args.hdfs_uri,
        spark_master=args.spark_master,
        days_per_chunk=args.days_per_chunk,
        expected_day_count=args.expected_day_count,
        event_log_dir=args.event_log_dir,
        dataset_scope=args.dataset_scope,
        confirm_large_workload=args.confirm_large_workload,
    )
    print(f"candidate_status={result.status}")
    print(f"source_version={result.source_version}")
    print(f"source_snapshot_id={result.source_snapshot_id}")
    print(f"pipeline_run_id={result.pipeline_run_id}")
    if result.raw_path is not None:
        print(f"raw_path={result.raw_path}")
    print(f"curated_path={result.curated_path}")
    if result.quality_report_path is not None:
        print(f"quality_report_path={result.quality_report_path}")
    if result.run_manifest_path is not None:
        print(f"run_manifest_path={result.run_manifest_path}")
    print(f"timings={json.dumps(dict(result.timings), sort_keys=True, separators=(',', ':'))}")
    for detail in result.failure_details:
        print(f"failure={detail}")
    raise SystemExit(0 if result.status in {"STAGED", "REUSED"} else 1)


if __name__ == "__main__":
    main()
