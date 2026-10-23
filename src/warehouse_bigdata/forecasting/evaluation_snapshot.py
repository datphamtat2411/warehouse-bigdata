"""Four-origin evaluation execution, immutable candidates and persisted readback."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import reduce

from warehouse_bigdata.common.config import load_hdfs_config, load_spark_config
from warehouse_bigdata.common.hdfs_paths import (
    render_evaluation_output_path,
    render_staging_path,
    validate_evaluation_run_id,
)
from warehouse_bigdata.common.hdfs_publication import document_digest
from warehouse_bigdata.common.scope_evidence import authorize_upstream, read_scope, write_scope
from warehouse_bigdata.forecasting.evaluation import (
    ORIGINS,
    EvaluationContext,
    aggregate_metrics,
    align_origin,
    cohort_content_digest,
    coverage_from_audit,
    diagnostics,
    expected_keys,
    freeze_protocol,
    resolve_origins,
    seasonal_naive,
    validate_freeze,
    validate_metrics,
)
from warehouse_bigdata.forecasting.evaluation_artifacts import (
    evaluation_file_integrity,
    make_evaluation_manifest,
    read_validated_validation,
    stage_evaluation_publication,
    validate_evaluation_evidence,
)
from warehouse_bigdata.forecasting.feature_input import read_validated_feature_context
from warehouse_bigdata.forecasting.feature_snapshot import packaged_code_revision
from warehouse_bigdata.forecasting.forecast_cohort import (
    TRAINING_END,
    select_forecast_cohort,
    validate_cohort_selection,
)
from warehouse_bigdata.forecasting.forecast_snapshot import (
    _equal,
    _read_parquet,
    reconcile_forecast_inputs,
    validate_forecast_parameters,
    validate_issue_window,
)
from warehouse_bigdata.forecasting.model_artifacts import (
    artifact_uri,
    read_validated_model_metadata,
    validate_scoring_pipeline,
)
from warehouse_bigdata.forecasting.recursive_forecasting import (
    ForecastError,
    ForecastRunContext,
    forecast_recursive,
)
from warehouse_bigdata.forecasting.scoring_features import AsOfHistoryResult, prepare_as_of_history
from warehouse_bigdata.forecasting.training_snapshot import _now, _write_json
from warehouse_bigdata.processing.enrichment_analytics_snapshot import (
    read_validated_sales_enriched_snapshot,
)


@dataclass(frozen=True)
class EvaluationSnapshotResult:
    status: str
    pipeline_run_id: str
    evaluation_run_id: str
    artifact_path: str
    staging_path: str
    evaluation_manifest_path: str | None
    quality_report_path: str | None
    failure_report_path: str | None
    failure_details: tuple[str, ...]


def validate_evaluation_parameters(
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
    seed=42,
    validation_evaluation_run_id=None,
):
    ctx = EvaluationContext(
        evaluation_run_id, model_run_id, source_snapshot_id, feature_version, run_type
    )
    validate_forecast_parameters(
        source_version=source_version,
        source_snapshot_id=source_snapshot_id,
        j03_pipeline_run_id=j03_pipeline_run_id,
        j04_pipeline_run_id=j04_pipeline_run_id,
        feature_version=feature_version,
        model_run_id=model_run_id,
        forecast_run_id=ctx.forecast_id(ORIGINS[run_type][0]),
        pipeline_run_id=pipeline_run_id,
        issue_date=ctx.window[0],
        run_type=run_type,
        seed=seed,
    )
    if run_type == "test":
        validate_evaluation_run_id(validation_evaluation_run_id)
        if validation_evaluation_run_id == evaluation_run_id:
            raise ForecastError("validation and test require distinct evaluation identities")
    elif validation_evaluation_run_id is not None:
        raise ForecastError("validation cannot select a validation freeze")


def _load_frozen_model(spark, *, metadata, hdfs_config):
    """The metadata gate has already verified integrity; load the fitted object once."""
    from pyspark.ml import PipelineModel

    model = PipelineModel.load(
        artifact_uri(hdfs_config, metadata["artifact_path"] + "/spark_pipeline_model")
    )
    validate_scoring_pipeline(model)
    return model


def _union(frames):
    return reduce(lambda a, b: a.unionByName(b), frames)


def _validate_origin_exclusions(
    rf, rf_exceptions, baseline, baseline_exceptions, eligibility, scope
):
    """No absent method outcome may disappear without its attributable evidence."""
    from pyspark.sql import functions as F

    keys = ["item_id", "store_id"]
    success = rf.select(*keys).distinct()
    excluded = rf_exceptions.select(*keys).distinct()
    if (
        success.join(excluded, keys).limit(1).count()
        or success.unionByName(excluded).exceptAll(scope).limit(1).count()
        or scope.exceptAll(success.unionByName(excluded)).limit(1).count()
    ):
        raise ForecastError("RF complete-series exclusions do not reconcile")
    eligible = eligibility.where("status = 'ELIGIBLE'").select(*keys)
    expected = eligible.withColumn("horizon", F.explode(F.sequence(F.lit(1), F.lit(7))))
    outcomes = baseline.select(*keys, "horizon").unionByName(
        baseline_exceptions.select(*keys, "horizon")
    )
    if outcomes.exceptAll(expected).unionByName(expected.exceptAll(outcomes)).limit(1).count():
        raise ForecastError("baseline exclusions do not reconcile")


def run_forecast_evaluation(
    spark,
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
    hdfs_config,
    spark_master,
    seed=42,
    validation_evaluation_run_id=None,
    event_log_dir=None,
    client=None,
    dataset_scope=None,
    confirm_large_workload=False,
):
    """Stage a complete phase; the external owner seals and exclusively promotes it."""
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    from warehouse_bigdata.common.hdfs_runtime import create_hdfs_cli_client

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
    load_hdfs_config(
        {"WAREHOUSE_HDFS_URI": hdfs_config.uri, "WAREHOUSE_HDFS_BASE_PATH": hdfs_config.base_path}
    )
    load_spark_config({"WAREHOUSE_SPARK_MASTER_URL": spark_master})
    if not spark_master.startswith("spark://") or spark.sparkContext.master != spark_master:
        raise ForecastError("evaluation requires an explicit matching Spark Standalone master")
    if spark.conf.get("spark.sql.session.timeZone") not in {"UTC", "Etc/UTC"}:
        raise ForecastError("evaluation requires UTC Spark timestamps")
    ctx = EvaluationContext(
        evaluation_run_id, model_run_id, source_snapshot_id, feature_version, run_type
    )
    staging = render_staging_path(hdfs_config.base_path, pipeline_run_id)
    final = render_evaluation_output_path(hdfs_config.base_path, evaluation_run_id)
    candidate = staging + "/candidates/evaluation"
    hdfs, owns_attempt = client, False
    created_at = _now()

    def save(name, frame):
        uri = artifact_uri(hdfs_config, candidate + "/" + name)
        frame.write.mode("errorifexists").parquet(uri)
        saved = _read_parquet(spark, uri)
        _equal(saved, frame, name.split("/")[-1])
        return saved

    try:
        hdfs = create_hdfs_cli_client(hdfs_config) if hdfs is None else hdfs
        from warehouse_bigdata.common.hdfs_paths import render_curated_sales_enriched_path

        authorize_upstream(
            read_scope(
                hdfs, render_curated_sales_enriched_path(hdfs_config.base_path, source_snapshot_id)
            ),
            dataset_scope=dataset_scope,
            confirm_large_workload=confirm_large_workload,
        )
        if not hdfs.is_directory(staging):
            raise ForecastError("owner-prepared execution staging is required")
        if hdfs.exists(final):
            raise ForecastError("immutable evaluation identity exists; use owner recovery")
        for name in (
            "input_binding.json",
            "quality_report.json",
            "failure_report.json",
            "publication",
            "candidates",
        ):
            if hdfs.exists(staging + "/" + name):
                raise ForecastError("execution has frozen evidence; use owner recovery")
        hdfs.mkdir_exclusive(staging + "/candidates")
        owns_attempt = True
        hdfs.mkdir_exclusive(candidate)
        model_args = dict(
            model_run_id=model_run_id,
            source_snapshot_id=source_snapshot_id,
            feature_version=feature_version,
            hdfs_config=hdfs_config,
            client=hdfs,
        )
        metadata = read_validated_model_metadata(**model_args)
        feature = read_validated_feature_context(
            spark,
            source_version=source_version,
            source_snapshot_id=source_snapshot_id,
            upstream_pipeline_run_id=j04_pipeline_run_id,
            feature_version=feature_version,
            hdfs_config=hdfs_config,
            client=hdfs,
        )
        frozen_scope = authorize_upstream(
            feature["scope"],
            dataset_scope=dataset_scope,
            confirm_large_workload=confirm_large_workload,
        )
        candidate_series = 100 if frozen_scope["dataset_scope"] == "demo_sample" else 1000
        write_scope(hdfs, candidate, frozen_scope)
        sales, sales_evidence = read_validated_sales_enriched_snapshot(
            spark,
            source_version=source_version,
            source_snapshot_id=source_snapshot_id,
            pipeline_run_id=j03_pipeline_run_id,
            hdfs_config=hdfs_config,
            client=hdfs,
        )
        reconcile_forecast_inputs(
            metadata, feature, sales_evidence, j04_pipeline_run_id=j04_pipeline_run_id
        )
        origins = resolve_origins(feature["calendar_mapping"], run_type)
        for _, issue in origins:
            validate_issue_window(issue, run_type, metadata["manifest"])
        source_binding = {
            k: identities[k]
            for k in (
                "source_version",
                "source_snapshot_id",
                "j03_pipeline_run_id",
                "j04_pipeline_run_id",
                "feature_version",
                "model_run_id",
            )
        }
        source_binding.update(
            scope=frozen_scope,
            j03_evidence=sales_evidence,
            j04_evidence={
                k: v
                for k, v in feature.items()
                if k not in {"calendar_mapping", "manifest", "sidecar"}
            },
        )
        selected_validation = None
        if run_type == "validation":
            selection = select_forecast_cohort(
                sales, training_end=TRAINING_END, seed=seed, scope_evidence=frozen_scope
            ).evidence
        else:
            selected_validation = read_validated_validation(
                hdfs, hdfs_config, validation_evaluation_run_id
            )
            selection = _read_parquet(
                spark,
                artifact_uri(
                    hdfs_config, selected_validation["artifact_path"] + "/cohort_selection"
                ),
            )
        selection = save("cohort_selection", selection)
        scope = selection.where("status = 'SELECTED'").select(
            "item_id", F.explode("stores").alias("store_id")
        )
        validate_cohort_selection(selection, scope, scope_evidence=frozen_scope)
        code_revision = packaged_code_revision()
        frozen = freeze_protocol(
            model_metadata=metadata,
            cohort_digest=cohort_content_digest(selection),
            seed=seed,
            code_revision=code_revision,
            source_binding=source_binding,
        )
        if selected_validation:
            validate_freeze(selected_validation["binding"]["freeze"], frozen)
        configuration = dict(
            seed=seed,
            hdfs_uri=hdfs_config.uri,
            hdfs_base_path=hdfs_config.base_path,
            spark_master=spark_master,
            event_log_dir=event_log_dir,
            spark_version=spark.version,
            runtime={
                k: spark.sparkContext.getConf().get(k, None)
                for k in (
                    "spark.driver.memory",
                    "spark.executor.memory",
                    "spark.executor.cores",
                    "spark.cores.max",
                    "spark.default.parallelism",
                    "spark.dynamicAllocation.enabled",
                    "spark.eventLog.dir",
                )
            },
            sql={
                k: spark.conf.get(k)
                for k in (
                    "spark.sql.shuffle.partitions",
                    "spark.sql.adaptive.enabled",
                    "spark.sql.session.timeZone",
                    "spark.sql.caseSensitive",
                    "spark.sql.ansi.enabled",
                )
            },
        )
        binding = dict(
            **identities,
            schema_version="evaluation-input-binding-v1",
            holdout_id=ctx.holdout_id,
            scope=frozen_scope,
            code_revision=code_revision,
            configuration_digest=document_digest(configuration),
            effective_configuration=configuration,
            freeze=frozen,
            origin_forecasts=ctx.origin_forecasts,
            validation_manifest_digest=document_digest(selected_validation["manifest"])
            if selected_validation
            else None,
            provenance="forecast_quality_evaluation",
            prediction_provenance="predicted_unit_sales",
            label_provenance="observed_m5_unit_sales",
        )
        # Durable analytical lock exists before loading/scoring or accessing outcome labels.
        _write_json(hdfs, staging + "/input_binding.json", binding)
        hdfs.mkdir_exclusive(candidate + "/evidence")
        _write_json(hdfs, candidate + "/evidence/evaluation.json", binding)
        hdfs.mkdir_exclusive(candidate + "/origins")
        model = _load_frozen_model(spark, metadata=metadata, hdfs_config=hdfs_config)
        saved_origins = []
        for day, issue in origins:
            parent = f"origins/d_{day}"
            hdfs.mkdir_exclusive(candidate + "/" + parent)
            # Give preparation only observations available at this origin. Never
            # carry temporary recursive predictions into the next origin.
            prepared = prepare_as_of_history(
                sales.where((F.col("date") <= F.lit(issue)) | F.col("date").isNull()),
                issue_date=issue,
                source_snapshot_id=source_snapshot_id,
                feature_version=feature_version,
                series_scope=scope,
            )
            state = prepared.state.persist(StorageLevel.DISK_ONLY)
            eligibility = prepared.eligibility.persist(StorageLevel.DISK_ONLY)
            result = None
            try:
                state.count()
                eligibility.count()
                prepared = AsOfHistoryResult(state, eligibility, prepared.coverage)
                baseline = seasonal_naive(state, issue_date=issue)
                base = save(parent + "/baseline_predictions", baseline.predictions)
                base_exceptions = save(parent + "/baseline_exceptions", baseline.exceptions)
                saved_eligibility = save(parent + "/eligibility", eligibility)
                run = ForecastRunContext(
                    ctx.forecast_id(day),
                    model_run_id,
                    source_snapshot_id,
                    feature_version,
                    issue,
                    run_type,
                    datetime.fromisoformat(created_at.replace("Z", "+00:00")),
                )
                known = feature["calendar_mapping"].where(
                    F.col("target_date").between(issue, issue + timedelta(days=7))
                )
                result = forecast_recursive(
                    model,
                    prepared,
                    known_calendar=known.select(F.col("target_date").alias("date")),
                    run_context=run,
                )
                daily = save(parent + "/forecast_daily", result.daily)
                rf_exceptions = save(parent + "/rf_exceptions", result.exceptions)
                _validate_origin_exclusions(
                    daily, rf_exceptions, base, base_exceptions, saved_eligibility, scope
                )
                saved_origins.append(
                    (day, run, daily, base, rf_exceptions, base_exceptions, saved_eligibility)
                )
            finally:
                if result is not None:
                    result.release()
                eligibility.unpersist()
                state.unpersist()
        # Only persisted predictions cross this boundary into future observed labels.
        pairs, audits, exceptions = [], [], []
        for day, run, daily, base, rf_exceptions, base_exceptions, eligibility in saved_origins:
            aligned = align_origin(
                expected_keys(scope, [(day, run.issue_date)]),
                daily,
                base,
                sales,
                eligibility,
                run_context=run,
            )
            pairs.append(aligned.paired)
            audits.append(aligned.audit)
            exceptions.append(aligned.exceptions.withColumn("method_id", F.lit("paired")))
            exceptions.append(
                rf_exceptions.withColumn("issue_date", F.lit(run.issue_date))
                .withColumn(
                    "target_date",
                    F.when(F.col("horizon") > 0, F.date_add("issue_date", F.col("horizon"))),
                )
                .withColumn("method_id", F.lit("random_forest"))
            )
            exceptions.append(base_exceptions)
        paired = save("paired", _union(pairs))
        audit = save("alignment", _union(audits))
        if (
            paired.count() != audit.where("paired").count()
            or audit.count() != candidate_series * 4 * 7
        ):
            raise ForecastError("persisted candidate/pair coverage does not reconcile")
        coverage = save("coverage", coverage_from_audit(audit))
        save("exceptions", _union(exceptions))
        metrics, statistics = aggregate_metrics(paired, ctx)
        saved_metrics = save("metrics", metrics)
        save("metric_statistics", statistics)
        validate_metrics(saved_metrics, paired, ctx)
        dimensions, totals = diagnostics(paired, selection)
        save("diagnostics", dimensions)
        save("seven_day_errors", totals)
        quality = dict(
            status="VALIDATED",
            input_binding=binding,
            persisted_parquet_validated=True,
            row_count=48,
            schema_fingerprint=document_digest({"schema": saved_metrics.schema.simpleString()}),
            coverage=[
                r.asDict() | {"issue_date": r.issue_date.isoformat()}
                for r in coverage.orderBy("issue_date", "method_id").collect()
            ],
            file_integrity=evaluation_file_integrity(hdfs, candidate, run_type=run_type),
        )
        _write_json(hdfs, staging + "/quality_report.json", quality)
        _write_json(hdfs, candidate + "/evidence/publication.json", quality)
        manifest = make_evaluation_manifest(
            hdfs_config,
            binding=binding,
            quality=quality,
            created_at=created_at,
            completed_at=_now(),
        )
        _write_json(hdfs, candidate + "/evaluation_manifest.json", manifest)
        validate_evaluation_evidence(
            hdfs, hdfs_config, manifest, candidate, pipeline_run_id=pipeline_run_id
        )
        stage_evaluation_publication(hdfs, hdfs_config, manifest, pipeline_run_id=pipeline_run_id)
        return EvaluationSnapshotResult(
            "STAGED",
            pipeline_run_id,
            evaluation_run_id,
            final,
            staging,
            final + "/evaluation_manifest.json",
            staging + "/quality_report.json",
            None,
            (),
        )
    except Exception as error:
        detail = f"{type(error).__name__}: {str(error)[:1000]}"
        failure = None
        if owns_attempt:
            try:
                failure = staging + "/failure_report.json"
                _write_json(
                    hdfs,
                    failure,
                    {
                        **identities,
                        "status": "FAILED",
                        "failure_details": [detail],
                        "created_at": created_at,
                        "failed_at": _now(),
                    },
                )
            except Exception as evidence_error:
                failure = None
                detail += "; failure evidence unavailable: " + str(evidence_error)[:300]
        return EvaluationSnapshotResult(
            "FAILED",
            pipeline_run_id,
            evaluation_run_id,
            final,
            staging,
            None,
            None,
            failure,
            (detail,),
        )
