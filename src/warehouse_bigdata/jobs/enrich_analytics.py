"""Spark entry point for immutable M04 enrichment and analytics artifacts."""

from __future__ import annotations

import argparse

from warehouse_bigdata.common.config import load_hdfs_config, load_spark_config
from warehouse_bigdata.common.hdfs_client import HdfsCliClient
from warehouse_bigdata.processing.enrichment_analytics_snapshot import (
    EnrichmentAnalyticsSnapshotResult,
    run_enrichment_analytics_snapshot,
)


def run_enrich_analytics(
    *,
    source_version: str,
    source_snapshot_id: str,
    pipeline_run_id: str,
    hdfs_uri: str,
    spark_master: str,
) -> EnrichmentAnalyticsSnapshotResult:
    """Run one explicitly identified J03 attempt on HDFS/Spark."""
    hdfs_config = load_hdfs_config({"WAREHOUSE_HDFS_URI": hdfs_uri})
    spark_config = load_spark_config({"WAREHOUSE_SPARK_MASTER_URL": spark_master})

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master(spark_config.master_url)
        .appName(f"warehouse-enrich-analytics-{pipeline_run_id}")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        return run_enrichment_analytics_snapshot(
            spark,
            source_version=source_version,
            source_snapshot_id=source_snapshot_id,
            pipeline_run_id=pipeline_run_id,
            hdfs_config=hdfs_config,
            spark_master=spark_config.master_url,
            client=HdfsCliClient(),
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
    args = parser.parse_args()
    result = run_enrich_analytics(
        source_version=args.source_version,
        source_snapshot_id=args.source_snapshot_id,
        pipeline_run_id=args.pipeline_run_id,
        hdfs_uri=args.hdfs_uri,
        spark_master=args.spark_master,
    )
    print(f"status={result.status}")
    print(f"source_version={result.source_version}")
    print(f"source_snapshot_id={result.source_snapshot_id}")
    print(f"pipeline_run_id={result.pipeline_run_id}")
    if result.raw_path is not None:
        print(f"raw_path={result.raw_path}")
    print(f"sales_daily_path={result.sales_daily_path}")
    print(f"curated_path={result.curated_path}")
    if result.quality_report_path is not None:
        print(f"quality_report_path={result.quality_report_path}")
    if result.failure_report_path is not None:
        print(f"failure_report_path={result.failure_report_path}")
    if result.run_manifest_path is not None:
        print(f"run_manifest_path={result.run_manifest_path}")
    for detail in result.failure_details:
        print(f"failure={detail}")
    raise SystemExit(0 if result.status in {"VALIDATED", "REUSED"} else 1)


if __name__ == "__main__":
    main()
