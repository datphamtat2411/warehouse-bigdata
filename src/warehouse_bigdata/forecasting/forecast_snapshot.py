"""J06 input binding, persisted forecast validation and candidate staging."""

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import time

from warehouse_bigdata.common.config import load_hdfs_config, load_spark_config
from warehouse_bigdata.common.hdfs_paths import (
    render_forecast_output_path, render_staging_path, validate_feature_version,
    validate_forecast_run_id, validate_model_run_id, validate_pipeline_run_id,
    validate_source_snapshot_id, validate_source_version,
)
from warehouse_bigdata.common.hdfs_publication import document_digest
from warehouse_bigdata.forecasting.feature_input import read_validated_feature_context
from warehouse_bigdata.forecasting.feature_snapshot import packaged_code_revision
from warehouse_bigdata.forecasting.forecast_artifacts import (
    forecast_file_integrity, make_forecast_manifest, stage_forecast_publication, validate_forecast_evidence,
)
from warehouse_bigdata.forecasting.forecast_cohort import (
    POLICY, TRAINING_END, select_forecast_cohort, validate_cohort_selection,
)
from warehouse_bigdata.forecasting.model_artifacts import artifact_uri, load_validated_model, read_validated_model_metadata
from warehouse_bigdata.forecasting.recursive_forecasting import ForecastError, ForecastRunContext, forecast_recursive, validate_forecast_daily
from warehouse_bigdata.forecasting.training_snapshot import _now, _write_json
from warehouse_bigdata.processing.enrichment_analytics_snapshot import read_validated_sales_enriched_snapshot


@dataclass(frozen=True)
class ForecastSnapshotResult:
    status: str
    pipeline_run_id: str
    forecast_run_id: str
    artifact_path: str
    staging_path: str
    scoring_manifest_path: str | None
    quality_report_path: str | None
    failure_report_path: str | None
    failure_details: tuple[str, ...]


def validate_forecast_parameters(*, source_version, source_snapshot_id, j03_pipeline_run_id,
        j04_pipeline_run_id, feature_version, model_run_id, forecast_run_id, pipeline_run_id,
        issue_date, run_type, seed=42):
    validate_source_version(source_version)
    validate_source_snapshot_id(source_snapshot_id)
    for value in (j03_pipeline_run_id, j04_pipeline_run_id, pipeline_run_id):
        validate_pipeline_run_id(value)
    validate_feature_version(feature_version)
    validate_model_run_id(model_run_id)
    validate_forecast_run_id(forecast_run_id)
    if type(issue_date) is not date or run_type not in {"validation", "test", "forward_demo"}:
        raise ForecastError("explicit issue date and supported forecast run_type required")
    if type(seed) is not int or not -(2**31) <= seed < 2**31:
        raise ForecastError("cohort seed must be a signed 32-bit integer")


def validate_issue_window(issue_date, run_type, manifest):
    if (manifest.get("forecast_strategy") != "recursive_7_day" or
        manifest.get("training_end") != TRAINING_END.isoformat() or issue_date < TRAINING_END):
        raise ForecastError("model/cohort cutoff or forecast strategy is incompatible with issue date")
    if run_type == "forward_demo":
        if issue_date != date(2016, 5, 22):
            raise ForecastError("forward_demo requires the d_1941 issue date")
    elif run_type in {"validation", "test"}:
        start, end = (date.fromisoformat(manifest[run_type + suffix]) for suffix in ("_start", "_end"))
        if not start <= issue_date + timedelta(days=1) <= issue_date + timedelta(days=7) <= end:
            raise ForecastError("seven target dates must remain inside the declared historical window")
    else:
        raise ForecastError("unsupported run_type")


def reconcile_forecast_inputs(model_metadata, feature_context, sales_evidence, *, j04_pipeline_run_id):
    binding = model_metadata["input_binding"]
    if binding.get("upstream_pipeline_run_id") != j04_pipeline_run_id:
        raise ForecastError("selected model J04 lineage binding conflicts")
    frozen = binding.get("upstream_evidence", {})
    for key in ("completion_digest", "dataset_manifest_digest", "input_binding_digest"):
        if not frozen.get(key) or frozen[key] != feature_context.get(key):
            raise ForecastError("selected model exact J04 input binding conflicts: " + key)
    if feature_context.get("upstream_evidence") != sales_evidence:
        raise ForecastError("selected J03 lineage conflicts with J04 frozen source/raw binding")


def _read_parquet(spark, uri):
    return (spark.read.option("mergeSchema", "true").option("ignoreCorruptFiles", "false")
            .option("ignoreMissingFiles", "false").parquet(uri))


def _equal(actual, expected, name):
    if (actual.schema.simpleString() != expected.schema.simpleString() or
        actual.exceptAll(expected).unionByName(expected.exceptAll(actual)).limit(1).count()):
        raise ForecastError("persisted " + name + " differs from staged data")


def run_forecast_snapshot(spark, *, source_version, source_snapshot_id, j03_pipeline_run_id,
        j04_pipeline_run_id, feature_version, model_run_id, forecast_run_id, pipeline_run_id,
        issue_date, run_type, hdfs_config, spark_master, seed=42, event_log_dir=None, client=None):
    """Stage one immutable forecast run; the external owner performs promotion."""
    from pyspark import StorageLevel
    from pyspark.sql import functions as F
    from warehouse_bigdata.common.hdfs_runtime import create_hdfs_cli_client
    from warehouse_bigdata.forecasting.scoring_features import prepare_as_of_history

    identities = dict(source_version=source_version, source_snapshot_id=source_snapshot_id,
        j03_pipeline_run_id=j03_pipeline_run_id, j04_pipeline_run_id=j04_pipeline_run_id,
        feature_version=feature_version, model_run_id=model_run_id, forecast_run_id=forecast_run_id,
        pipeline_run_id=pipeline_run_id, issue_date=issue_date, run_type=run_type)
    validate_forecast_parameters(**identities, seed=seed)
    load_hdfs_config({"WAREHOUSE_HDFS_URI": hdfs_config.uri, "WAREHOUSE_HDFS_BASE_PATH": hdfs_config.base_path})
    load_spark_config({"WAREHOUSE_SPARK_MASTER_URL": spark_master})
    if not spark_master.startswith("spark://") or spark.sparkContext.master != spark_master:
        raise ForecastError("J06 requires an explicit matching Spark Standalone master")
    if spark.conf.get("spark.sql.session.timeZone") not in {"UTC", "Etc/UTC"}:
        raise ForecastError("J06 requires UTC Spark timestamps")
    staging = render_staging_path(hdfs_config.base_path, pipeline_run_id)
    final = render_forecast_output_path(hdfs_config.base_path, forecast_run_id)
    candidate = staging + "/candidates/forecast"
    hdfs = client
    owns_attempt, result, retained = False, None, []
    created_at, started = _now(), time.perf_counter()
    try:
        hdfs = create_hdfs_cli_client(hdfs_config) if hdfs is None else hdfs
        if not hdfs.is_directory(staging):
            raise ForecastError("owner-prepared execution staging is required")
        if hdfs.exists(final):
            raise ForecastError("immutable forecast identity exists; use owner recovery")
        for name in ("input_binding.json", "quality_report.json", "failure_report.json", "publication", "candidates"):
            if hdfs.exists(staging + "/" + name):
                raise ForecastError("execution has frozen evidence; use owner recovery")
        hdfs.mkdir_exclusive(staging + "/candidates")
        owns_attempt = True
        hdfs.mkdir_exclusive(candidate)
        model_args = dict(model_run_id=model_run_id, source_snapshot_id=source_snapshot_id,
                          feature_version=feature_version, hdfs_config=hdfs_config, client=hdfs)
        metadata = read_validated_model_metadata(**model_args)
        validate_issue_window(issue_date, run_type, metadata["manifest"])
        context = read_validated_feature_context(spark, source_version=source_version, source_snapshot_id=source_snapshot_id,
            upstream_pipeline_run_id=j04_pipeline_run_id, feature_version=feature_version, hdfs_config=hdfs_config, client=hdfs)
        sales, sales_evidence = read_validated_sales_enriched_snapshot(spark, source_version=source_version,
            source_snapshot_id=source_snapshot_id, pipeline_run_id=j03_pipeline_run_id, hdfs_config=hdfs_config, client=hdfs)
        reconcile_forecast_inputs(metadata, context, sales_evidence, j04_pipeline_run_id=j04_pipeline_run_id)
        calendar = context["calendar_mapping"].where(F.col("target_date").between(issue_date, issue_date + timedelta(days=7)))
        if calendar.count() != 8:
            raise ForecastError("bound calendar requires issue date through T+7 coverage")
        configuration = dict(seed=seed, selection_policy=POLICY, training_end=TRAINING_END.isoformat(),
            horizon_days=7, lookback_days=28, sku_count=100, store_count=10, event_flag=False,
            price_policy="hold_last_known_at_issue", hdfs_uri=hdfs_config.uri, hdfs_base_path=hdfs_config.base_path,
            spark_master=spark_master, event_log_dir=event_log_dir, spark_version=spark.version,
            runtime={k: spark.sparkContext.getConf().get(k, None) for k in (
                "spark.driver.memory", "spark.executor.memory", "spark.executor.cores", "spark.cores.max",
                "spark.default.parallelism", "spark.dynamicAllocation.enabled", "spark.eventLog.dir")},
            sql={k: spark.conf.get(k) for k in ("spark.sql.shuffle.partitions", "spark.sql.adaptive.enabled",
                "spark.sql.session.timeZone", "spark.sql.caseSensitive", "spark.sql.ansi.enabled")})
        binding = {**identities, "issue_date": issue_date.isoformat(), "schema_version": "forecast-input-binding-v1",
            "effective_configuration": configuration, "configuration_digest": document_digest(configuration),
            "code_revision": packaged_code_revision(), "artifact_uri": hdfs_config.uri + final,
            "model_completion_digest": document_digest(metadata["manifest"]), "model_input_binding_digest": document_digest(metadata["input_binding"]),
            "j04_evidence": {k: v for k, v in context.items() if k not in {"calendar_mapping", "manifest", "sidecar"}},
            "j03_evidence": sales_evidence, "provenance": "predicted_unit_sales",
            "recursive_provenance": "temporary predicted entries retain the original issue_date; observed inputs remain unchanged"}
        _write_json(hdfs, staging + "/input_binding.json", binding)
        cohort = select_forecast_cohort(sales, training_end=TRAINING_END, seed=seed)
        evidence = cohort.evidence.persist(StorageLevel.DISK_ONLY)
        retained.append(evidence)
        evidence.count()
        evidence.write.mode("errorifexists").parquet(artifact_uri(hdfs_config, candidate + "/cohort_selection"))
        saved_selection = _read_parquet(spark, artifact_uri(hdfs_config, candidate + "/cohort_selection"))
        _equal(saved_selection, evidence, "cohort_selection")
        scope = saved_selection.where("status = 'SELECTED'").select("item_id", F.explode("stores").alias("store_id"))
        validate_cohort_selection(saved_selection, scope)
        prepared = prepare_as_of_history(sales, issue_date=issue_date, source_snapshot_id=source_snapshot_id,
                                         feature_version=feature_version, series_scope=scope)
        model = load_validated_model(spark, **model_args)
        run_context = ForecastRunContext(forecast_run_id, model_run_id, source_snapshot_id, feature_version,
                                        issue_date, run_type, datetime.fromisoformat(created_at.replace("Z", "+00:00")))
        result = forecast_recursive(model, prepared, known_calendar=calendar.select(F.col("target_date").alias("date")),
                                    run_context=run_context)
        saved = {}
        for name, frame in (("forecast_daily", result.daily), ("coverage", result.coverage), ("coverage_exceptions", result.exceptions)):
            uri = artifact_uri(hdfs_config, candidate + "/" + name)
            frame.write.mode("errorifexists").parquet(uri)
            saved[name] = _read_parquet(spark, uri)
            _equal(saved[name], frame, name)
        counts = validate_forecast_daily(saved["forecast_daily"], run_context=run_context, series_scope=scope)
        coverage = saved["coverage"].first().asDict()
        if (coverage["candidate_series"] != 1000 or counts["successful_series"] != coverage["successful_series"] or
            saved["coverage_exceptions"].select("item_id", "store_id").distinct().count() != coverage["excluded_series"]):
            raise ForecastError("persisted fixed-cohort coverage does not reconcile")
        totals = saved["forecast_daily"].groupBy("item_id", "store_id").agg(F.sum("predicted_units").alias("forecast_7d_units"))
        if totals.where(F.isnan("forecast_7d_units") | (F.col("forecast_7d_units") > 1.7976931348623157e308)).limit(1).count():
            raise ForecastError("nonfinite seven-day aggregate")
        total = totals.agg(F.sum("forecast_7d_units").alias("units")).first().units
        hdfs.mkdir_exclusive(candidate + "/evidence")
        _write_json(hdfs, candidate + "/evidence/scoring.json", binding)
        quality = dict(status="VALIDATED", input_binding=binding, persisted_parquet_validated=True,
            **coverage, schema_fingerprint=document_digest({"schema": saved["forecast_daily"].schema.simpleString()}),
            file_integrity=forecast_file_integrity(hdfs, candidate), seven_day_total_units=total or 0.0,
            elapsed_seconds=time.perf_counter() - started)
        _write_json(hdfs, staging + "/quality_report.json", quality)
        _write_json(hdfs, candidate + "/evidence/publication.json", quality)
        manifest = make_forecast_manifest(hdfs_config, binding=binding, quality=quality, created_at=created_at, completed_at=_now())
        _write_json(hdfs, candidate + "/scoring_manifest.json", manifest)
        validate_forecast_evidence(hdfs, hdfs_config, manifest, candidate, pipeline_run_id=pipeline_run_id)
        stage_forecast_publication(hdfs, hdfs_config, manifest, pipeline_run_id=pipeline_run_id)
        return ForecastSnapshotResult("STAGED", pipeline_run_id, forecast_run_id, final, staging,
            final + "/scoring_manifest.json", staging + "/quality_report.json", None, ())
    except Exception as error:
        detail = f"{type(error).__name__}: {str(error)[:1000]}"
        failure = None
        if owns_attempt:
            try:
                failure = staging + "/failure_report.json"
                _write_json(hdfs, failure, {**identities, "issue_date": issue_date.isoformat(), "status": "FAILED",
                    "failure_details": [detail], "created_at": created_at, "failed_at": _now()})
            except Exception as evidence_error:
                failure = None
                detail += "; failure evidence unavailable: " + str(evidence_error)[:300]
        return ForecastSnapshotResult("FAILED", pipeline_run_id, forecast_run_id, final, staging, None, None, failure, (detail,))
    finally:
        if result is not None:
            result.release()
        for frame in reversed(retained):
            frame.unpersist()
