"""Spark entry point for independent M04 output reconciliation."""

from __future__ import annotations

import argparse
import json

from warehouse_bigdata.common.config import load_hdfs_config, load_spark_config
from warehouse_bigdata.common.hdfs_client import HdfsCliClient
from warehouse_bigdata.common.hdfs_paths import (
    render_curated_category_monthly_sales_path,
    render_curated_sales_daily_path,
    render_curated_sales_enriched_path,
    render_curated_store_daily_sales_path,
    render_enrichment_quality_path,
    render_raw_m5_path,
    render_staging_path,
)
from warehouse_bigdata.common.hdfs_permissions import prepare_spark_run_permissions
from warehouse_bigdata.processing.enrichment_analytics_snapshot import (
    validate_enrichment_analytics_completion_marker,
)
from warehouse_bigdata.processing.m04_verification import run_m04_verification

RECORD_PREFIX = "WAREHOUSE_M04 "


def run_verify_m04_outputs(
    *,
    source_version: str,
    source_snapshot_id: str,
    normalization_run_id: str,
    enrichment_run_id: str,
    pipeline_run_id: str,
    hdfs_uri: str,
    spark_master: str,
    expected_series_count: int = 100,
    expected_day_count: int = 1_941,
    event_log_dir: str | None = None,
) -> dict[str, object]:
    """Run the independent verifier against explicit immutable lineage paths."""
    hdfs_config = load_hdfs_config({"WAREHOUSE_HDFS_URI": hdfs_uri})
    spark_config = load_spark_config({"WAREHOUSE_SPARK_MASTER_URL": spark_master})
    hdfs = HdfsCliClient()
    prepare_spark_run_permissions(
        hdfs,
        hdfs_config,
        pipeline_run_id=pipeline_run_id,
        event_log_dir=event_log_dir,
    )

    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master(spark_config.master_url)
        .appName(f"warehouse-m04-verify-{pipeline_run_id}")
        .config("spark.ui.enabled", "false")
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
            f"{pipeline_run_id}:verification", "M04 independent Parquet reconciliation", True
        )
        output_paths = {
            "sales_daily": render_curated_sales_daily_path(
                hdfs_config.base_path, source_snapshot_id
            ),
            "sales_enriched": render_curated_sales_enriched_path(
                hdfs_config.base_path, source_snapshot_id
            ),
            "store_daily_sales": render_curated_store_daily_sales_path(
                hdfs_config.base_path, source_snapshot_id
            ),
            "category_monthly_sales": render_curated_category_monthly_sales_path(
                hdfs_config.base_path, source_snapshot_id
            ),
        }
        validate_enrichment_analytics_completion_marker(
            hdfs,
            hdfs_config,
            source_version=source_version,
            source_snapshot_id=source_snapshot_id,
            pipeline_run_id=enrichment_run_id,
            curated_output_paths={
                name: path for name, path in output_paths.items() if name != "sales_daily"
            },
            quality_path=render_enrichment_quality_path(
                render_staging_path(hdfs_config.base_path, enrichment_run_id)
            ),
        )
        result = run_m04_verification(
            spark,
            source_version=source_version,
            source_snapshot_id=source_snapshot_id,
            raw_path=render_raw_m5_path(hdfs_config.base_path, source_version),
            output_paths=output_paths,
            hdfs_config=hdfs_config,
            client=hdfs,
            expected_series_count=expected_series_count,
            expected_day_count=expected_day_count,
            normalization_run_id=normalization_run_id,
            enrichment_run_id=enrichment_run_id,
            hash_output_files=True,
        )
        return {
            **result.as_dict(),
            "normalization_run_id": normalization_run_id,
            "enrichment_run_id": enrichment_run_id,
            "pipeline_run_id": pipeline_run_id,
            "output_paths": output_paths,
        }
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-version", required=True, metavar="SHA256_ID")
    parser.add_argument("--source-snapshot-id", required=True, metavar="ID")
    parser.add_argument("--normalization-run-id", required=True, metavar="ID")
    parser.add_argument("--enrichment-run-id", required=True, metavar="ID")
    parser.add_argument("--pipeline-run-id", required=True, metavar="ID")
    parser.add_argument("--hdfs-uri", required=True, metavar="HDFS_URI")
    parser.add_argument("--spark-master", required=True, metavar="SPARK_URI")
    parser.add_argument("--expected-series-count", type=int, default=100, metavar="COUNT")
    parser.add_argument("--expected-day-count", type=int, default=1_941, metavar="DAYS")
    parser.add_argument("--event-log-dir", metavar="HDFS_URI")
    args = parser.parse_args()
    result = run_verify_m04_outputs(
        source_version=args.source_version,
        source_snapshot_id=args.source_snapshot_id,
        normalization_run_id=args.normalization_run_id,
        enrichment_run_id=args.enrichment_run_id,
        pipeline_run_id=args.pipeline_run_id,
        hdfs_uri=args.hdfs_uri,
        spark_master=args.spark_master,
        expected_series_count=args.expected_series_count,
        expected_day_count=args.expected_day_count,
        event_log_dir=args.event_log_dir,
    )
    print(RECORD_PREFIX + json.dumps(result, ensure_ascii=True, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
