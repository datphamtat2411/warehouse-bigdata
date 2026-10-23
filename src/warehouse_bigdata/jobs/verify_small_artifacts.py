"""Read-only distributed M5 Small readback; never builds upstream artifacts."""

import argparse
import json
from datetime import date, datetime

from warehouse_bigdata.common.config import load_hdfs_config
from warehouse_bigdata.common.hdfs_paths import (
    render_curated_category_monthly_sales_path,
    render_curated_sales_daily_path,
    render_curated_sales_enriched_path,
    render_curated_store_daily_sales_path,
    render_evaluation_output_path,
    render_forecast_output_path,
    render_raw_m5_path,
)
from warehouse_bigdata.common.scope_evidence import SCOPED_METADATA_LIMIT, read_scope
from warehouse_bigdata.common.small_artifacts import STAGES, VERIFY_STAGES, validate_small_artifacts
from warehouse_bigdata.common.spark_hdfs_client import SparkHdfsCliClient
from warehouse_bigdata.contracts.dataset_scope import ScopeError


def verify(spark, selector, *, through="test", client, config):
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
    if "j05" in stages:
        from warehouse_bigdata.forecasting.model_artifacts import (
            load_validated_model,
            read_validated_model_metadata,
        )

        args = dict(
            model_run_id=selector["model_run_id"],
            source_snapshot_id=snapshot,
            feature_version=selector["feature_version"],
            hdfs_config=config,
            client=client,
        )
        metadata = read_validated_model_metadata(**args)
        if (
            metadata["input_binding"]["scope"] != scope
            or metadata["manifest"]["training_row_count"] > 10000
        ):
            raise ScopeError("model population or training cap differs")
        model = load_validated_model(spark, **args)
        if features is None:
            raise ScopeError("model readback requires selected J04 features")
        probes = (
            model.transform(features.where("feature_role = 'train'").limit(2))
            .select("raw_prediction")
            .collect()
        )
        import math

        if not probes or any(not math.isfinite(r.raw_prediction) for r in probes):
            raise ScopeError("reloaded model inference probes failed")
        report["model"] = dict(
            model_run_id=selector["model_run_id"], reloaded=True, probe_count=len(probes)
        )
    if "j06" in stages:
        from warehouse_bigdata.forecasting.forecast_artifacts import validate_forecast_evidence
        from warehouse_bigdata.forecasting.recursive_forecasting import (
            ForecastRunContext,
            validate_forecast_daily,
        )

        path = render_forecast_output_path(config.base_path, selector["forecast_run_id"])
        marker = json.loads(client.read_remote(path + "/scoring_manifest.json"))
        validate_forecast_evidence(
            client, config, marker, path, pipeline_run_id=selector["j06_pipeline_run_id"]
        )
        if read_scope(client, path) != scope:
            raise ScopeError("forecast selection differs")
        context = ForecastRunContext(
            selector["forecast_run_id"],
            selector["model_run_id"],
            snapshot,
            selector["feature_version"],
            date(2016, 5, 22),
            "forward_demo",
            datetime.fromisoformat(marker["created_at"].replace("Z", "+00:00")),
        )
        frame = spark.read.parquet(config.uri + path + "/forecast_daily")
        population = spark.createDataFrame(
            [(r["item_id"], r["store_id"]) for r in scope["selected_series"]],
            "item_id string, store_id string",
        )
        report["forecast"] = validate_forecast_daily(
            frame, run_context=context, series_scope=population
        )
    for phase in ("validation", "test"):
        if phase not in stages:
            continue
        from warehouse_bigdata.forecasting.evaluation import EvaluationContext, validate_metrics
        from warehouse_bigdata.forecasting.evaluation_artifacts import validate_evaluation_evidence

        path = render_evaluation_output_path(
            config.base_path, selector[phase + "_evaluation_run_id"]
        )
        marker = json.loads(client.read_remote(path + "/evaluation_manifest.json"))
        validate_evaluation_evidence(
            client, config, marker, path, pipeline_run_id=selector[phase + "_pipeline_run_id"]
        )
        if read_scope(client, path) != scope:
            raise ScopeError("evaluation selection differs")
        paired = spark.read.parquet(config.uri + path + "/paired")
        metrics = spark.read.parquet(config.uri + path + "/metrics")
        context = EvaluationContext(
            selector[phase + "_evaluation_run_id"],
            selector["model_run_id"],
            snapshot,
            selector["feature_version"],
            phase,
        )
        validate_metrics(metrics, paired, context)
        audit = spark.read.parquet(config.uri + path + "/alignment")
        if audit.count() != 2800:
            raise ScopeError("evaluation must audit 100 series × four origins × seven days")
        report[phase] = dict(
            metric_rows=metrics.count(), paired_rows=paired.count(), candidate_rows=2800
        )
    report.update(status="VALIDATED", verified_through=through, verified_stages=list(stages))
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selector-json", required=True)
    parser.add_argument("--through", choices=VERIFY_STAGES, default="test")
    args = parser.parse_args()
    selector = json.loads(args.selector_json)
    validate_small_artifacts(selector, through=args.through)
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("spark://spark-master:7077")
        .appName("warehouse-small-readback")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
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
                ),
                sort_keys=True,
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
