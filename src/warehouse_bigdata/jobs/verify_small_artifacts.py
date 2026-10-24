"""Read-only distributed M5 Small readback; never builds upstream artifacts."""

import argparse
import json
import logging

from warehouse_bigdata.common.config import load_hdfs_config
from warehouse_bigdata.common.hdfs_paths import (
    render_curated_category_monthly_sales_path,
    render_curated_sales_daily_path,
    render_curated_sales_enriched_path,
    render_curated_store_daily_sales_path,
    render_raw_m5_path,
)
from warehouse_bigdata.common.scope_evidence import SCOPED_METADATA_LIMIT, read_scope
from warehouse_bigdata.common.small_artifacts import STAGES, VERIFY_STAGES, validate_small_artifacts
from warehouse_bigdata.common.spark_hdfs_client import SparkHdfsCliClient
from warehouse_bigdata.common.validation_timing import validation_phase
from warehouse_bigdata.contracts.dataset_scope import ScopeError


def verify(
    spark,
    selector,
    *,
    through="test",
    client,
    config,
    verification_run_id=None,
    spark_master="spark://spark-master:7077",
    event_log_dir=None,
    expected_application_id=None,
    expected_application_name=None,
):
    from warehouse_bigdata.common.small_preflight import validate_small_publications
    from warehouse_bigdata.ingestion.hdfs_raw import validate_hdfs_raw_snapshot
    from warehouse_bigdata.processing.m04_verification import run_m04_verification

    if through not in VERIFY_STAGES:
        raise ScopeError("readback requires a completion stage from J03 through test")
    validate_small_artifacts(selector, through=through)
    validate_small_publications(selector, through=through, client=client, config=config)
    stages = STAGES[: STAGES.index(through) + 1]
    snapshot, source = selector["source_snapshot_id"], selector["source_version"]
    paths = {
        name: renderer(config.base_path, snapshot)
        for name, renderer in (
            ("sales_daily", render_curated_sales_daily_path),
            ("sales_enriched", render_curated_sales_enriched_path),
            ("store_daily_sales", render_curated_store_daily_sales_path),
            ("category_monthly_sales", render_curated_category_monthly_sales_path),
        )
    }
    scope = read_scope(
        client,
        paths["sales_daily"],
        source_version=source,
        source_snapshot_id=snapshot,
        expected_digest=selector["selection_digest"],
    )
    if scope["dataset_scope"] != "demo_sample" or scope["original_series_count"] != 30490:
        raise ScopeError("real-data verification requires canonical M5 Small source lineage")
    with validation_phase("raw.integrity"):
        validate_hdfs_raw_snapshot(source, client=client, config=config)
    report = dict(
        dataset_scope="demo_sample",
        selection_digest=scope["selection_digest"],
        processing=run_m04_verification(
            spark,
            source_version=source,
            source_snapshot_id=snapshot,
            raw_path=render_raw_m5_path(config.base_path, source),
            output_paths=paths,
            hdfs_config=config,
            client=client,
            expected_series_count=100,
            expected_day_count=1941,
            normalization_run_id=selector["j02_pipeline_run_id"],
            enrichment_run_id=selector["j03_pipeline_run_id"],
            hash_output_files=True,
        ).as_dict(),
    )
    features = None
    if "j04" in stages:
        from warehouse_bigdata.forecasting.feature_input import (
            read_validated_forecast_features_snapshot,
        )

        features, evidence = read_validated_forecast_features_snapshot(
            spark,
            source_version=source,
            source_snapshot_id=snapshot,
            upstream_pipeline_run_id=selector["j04_pipeline_run_id"],
            feature_version=selector["feature_version"],
            hdfs_config=config,
            client=client,
        )
        if evidence["scope"] != scope:
            raise ScopeError("feature scope differs from canonical selection")
        report["features"] = {
            "row_count": features.count(),
            "evidence": {k: v for k, v in evidence.items() if k != "calendar_mapping"},
        }
    from warehouse_bigdata.forecasting.acceptance import verify_ml_artifacts

    report.update(
        verify_ml_artifacts(
            spark,
            selector,
            through=through,
            client=client,
            config=config,
            scope=scope,
            features=features,
        )
    )
    if verification_run_id is not None:
        from warehouse_bigdata.common.small_evidence import completed_execution

        expected_event_path = None
        if event_log_dir is not None:
            expected_event_path = event_log_dir.removeprefix(config.uri).rstrip("/")
        report["execution"] = completed_execution(
            client,
            config,
            verification_run_id,
            expected_application_id=expected_application_id,
            expected_application_name=expected_application_name,
            expected_master=spark_master,
            expected_event_log_dir=expected_event_path,
        )
        report["verification_run_id"] = verification_run_id
    report.update(status="VALIDATED", verified_through=through, verified_stages=list(stages))
    return report


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selector-json", required=True)
    parser.add_argument("--through", choices=VERIFY_STAGES, default="test")
    parser.add_argument("--verification-run-id")
    parser.add_argument("--spark-master", default="spark://spark-master:7077")
    parser.add_argument("--event-log-dir")
    parser.add_argument("--expected-application-id")
    parser.add_argument("--expected-application-name")
    args = parser.parse_args()
    selector = json.loads(args.selector_json)
    validate_small_artifacts(selector, through=args.through)
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master(args.spark_master)
        .appName(
            "warehouse-small-readback"
            + ("-" + args.verification_run_id if args.verification_run_id else "")
        )
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
    )
    if args.event_log_dir is not None:
        builder = (
            builder.config("spark.eventLog.enabled", "true")
            .config("spark.eventLog.dir", args.event_log_dir)
            .config("spark.eventLog.compress", "false")
            .config("spark.eventLog.rolling.enabled", "false")
        )
    spark = builder.getOrCreate()
    try:
        print(
            "SMALL_READBACK="
            + json.dumps(
                verify(
                    spark,
                    selector,
                    through=args.through,
                    client=SparkHdfsCliClient(metadata_limit=SCOPED_METADATA_LIMIT),
                    config=load_hdfs_config(),
                    verification_run_id=args.verification_run_id,
                    spark_master=args.spark_master,
                    event_log_dir=args.event_log_dir,
                    expected_application_id=args.expected_application_id,
                    expected_application_name=args.expected_application_name,
                ),
                sort_keys=True,
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
