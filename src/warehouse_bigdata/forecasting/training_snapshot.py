"""J05 training workflow; driver stages a complete bundle for the owner publisher."""

from __future__ import annotations

import json
import math
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

from warehouse_bigdata.common.hdfs_paths import (
    render_model_artifact_path, render_staging_path, validate_feature_version,
    validate_model_run_id, validate_pipeline_run_id, validate_source_snapshot_id, validate_source_version,
)
from warehouse_bigdata.common.hdfs_publication import document_digest, stage_publication
from warehouse_bigdata.common.hdfs_runtime import create_hdfs_cli_client
from warehouse_bigdata.contracts.datasets import FORECAST_FEATURES
from warehouse_bigdata.forecasting.feature_input import read_validated_forecast_features_snapshot
from warehouse_bigdata.forecasting.feature_snapshot import packaged_code_revision
from warehouse_bigdata.forecasting.feature_validation import SCOPED_KEY, typed_feature_schema
from warehouse_bigdata.forecasting.model_artifacts import (
    artifact_uri, make_model_manifest, model_file_integrity, validate_model_evidence,
)
from warehouse_bigdata.forecasting.training import (
    NUMERIC_FEATURES, PREDICTORS, RF_PARAMETERS, SELECTION_POLICY, STRATA, TrainingConfig,
    TrainingError, fit_training_pipeline, select_training_rows, selection_coverage,
)


@dataclass(frozen=True, slots=True)
class ModelTrainingResult:
    status: str
    pipeline_run_id: str
    model_run_id: str
    artifact_path: str
    staging_path: str
    model_manifest_path: str | None
    quality_report_path: str | None
    failure_report_path: str | None
    timings: dict
    failure_details: tuple[str, ...]


def validate_training_parameters(*, source_version, source_snapshot_id, upstream_pipeline_run_id,
                                 feature_version, model_run_id, pipeline_run_id):
    validate_source_version(source_version)
    validate_source_snapshot_id(source_snapshot_id)
    validate_pipeline_run_id(upstream_pipeline_run_id)
    validate_feature_version(feature_version)
    validate_model_run_id(model_run_id)
    validate_pipeline_run_id(pipeline_run_id)


def _now():
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _write_json(client, path, document):
    payload = (json.dumps(document, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()
    client.upload_bytes(payload, path)
    if client.read_remote(path) != payload:
        raise TrainingError("model evidence independent reread differs: " + path)


def _configuration(spark, config, hdfs_config):
    conf = spark.sparkContext.getConf()
    return {"training": asdict(config), "selection_policy": SELECTION_POLICY,
            "rf_parameters": RF_PARAMETERS, "predictor_order": list(PREDICTORS),
            "hdfs_uri": hdfs_config.uri, "hdfs_base_path": hdfs_config.base_path,
            "spark_master": spark.sparkContext.master, "spark_version": spark.version,
            "python_version": __import__("platform").python_version(),
            "runtime": {key: conf.get(key, None) for key in (
                "spark.driver.memory", "spark.executor.memory", "spark.executor.cores", "spark.cores.max",
                "spark.default.parallelism", "spark.dynamicAllocation.enabled", "spark.eventLog.dir")},
            "sql": {key: spark.conf.get(key) for key in (
                "spark.sql.shuffle.partitions", "spark.sql.adaptive.enabled", "spark.sql.session.timeZone",
                "spark.sql.caseSensitive", "spark.sql.ansi.enabled")}}


def _reload_probes(spark, model, rows, path):
    """Bounded parity check only; these probes produce no forecast artifact."""
    from pyspark.ml import PipelineModel
    from pyspark.sql import functions as F

    base = rows.orderBy(*SCOPED_KEY).limit(1).drop("label")
    ordinary = base.withColumn("_probe", F.lit("ordinary"))
    missing = (base.withColumn("last_known_price", F.lit(None).cast("double"))
               .withColumn("price_missing", F.lit(True)).withColumn("_probe", F.lit("missing_price")))
    unseen = (missing.withColumn("store_id", F.lit("__j05_unseen_store__"))
              .withColumn("dept_id", F.lit("__j05_unseen_department__"))
              .withColumn("lag_1", F.lit(0.375)).withColumn("lag_7", F.lit(1.125))
              .withColumn("lag_28", F.lit(2.625)).withColumn("_probe", F.lit("fractional_unseen")))
    probes = ordinary.unionByName(missing).unionByName(unseen)
    def predictions(pipeline):
        return pipeline.transform(probes).select("_probe", "features", "raw_prediction").orderBy("_probe").collect()
    before = predictions(model)
    loaded = PipelineModel.load(path)
    after = predictions(loaded)
    if len(before) != 3 or before != after or any(not math.isfinite(r.raw_prediction) for r in after):
        raise TrainingError("persisted PipelineModel feature/prediction reload parity failed")
    return {"probe_count": 3, "cases": [r._probe for r in after],
            "features_equal": True, "predictions_equal": True}


def run_model_training(spark, *, source_version, source_snapshot_id, upstream_pipeline_run_id,
        feature_version, model_run_id, pipeline_run_id, config: TrainingConfig,
        hdfs_config, client=None, spark_master=None):
    """Fit once from persisted selected keys; retain failure evidence only for our claim."""
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    identities = dict(source_version=source_version, source_snapshot_id=source_snapshot_id,
        upstream_pipeline_run_id=upstream_pipeline_run_id, feature_version=feature_version,
        model_run_id=model_run_id, pipeline_run_id=pipeline_run_id)
    validate_training_parameters(**identities)
    if spark_master is not None and (not spark_master.startswith("spark://") or
                                    spark.sparkContext.master != spark_master):
        raise TrainingError("explicit Standalone Spark master differs from effective master")
    staging = render_staging_path(hdfs_config.base_path, pipeline_run_id)
    final = render_model_artifact_path(hdfs_config.base_path, model_run_id)
    candidate = staging + "/candidates/model"
    started, created_at = time.perf_counter(), _now()
    timings, retained, owns_attempt = {}, [], False
    hdfs = client
    failure_path = None
    spark.sparkContext.setJobGroup(pipeline_run_id + ":training", "J05 global RF training", True)
    try:
        hdfs = create_hdfs_cli_client(hdfs_config) if hdfs is None else hdfs
        if not hdfs.is_directory(staging):
            raise TrainingError("owner-prepared execution staging is required")
        if hdfs.exists(final):
            raise TrainingError("immutable model identity already exists; use owner recovery or a new model_run_id")
        for name in ("input_binding.json", "quality_report.json", "failure_report.json", "publication", "candidates"):
            if hdfs.exists(staging + "/" + name):
                raise TrainingError("execution has frozen evidence; use owner recovery or a new pipeline_run_id")
        hdfs.mkdir_exclusive(staging + "/candidates")
        owns_attempt = True
        hdfs.mkdir_exclusive(candidate)
        phase = time.perf_counter()
        features, evidence = read_validated_forecast_features_snapshot(spark, hdfs_config=hdfs_config,
            client=hdfs, **{k: v for k, v in identities.items() if k not in {"model_run_id", "pipeline_run_id"}})
        timings["input_validation_seconds"] = time.perf_counter() - phase
        configuration = _configuration(spark, config, hdfs_config)
        binding = {**identities, "schema_version": "model-training-binding-v1",
                   "upstream_evidence": {k: v for k, v in evidence.items() if k != "calendar_mapping"},
                   "code_revision": packaged_code_revision(), "effective_configuration": configuration,
                   "configuration_digest": document_digest(configuration), "artifact_uri": hdfs_config.uri + final}
        _write_json(hdfs, staging + "/input_binding.json", binding)
        phase = time.perf_counter()
        selection = select_training_rows(features, calendar_mapping=evidence["calendar_mapping"], config=config)
        keys = selection.keys.persist(StorageLevel.DISK_ONLY)
        retained.append(keys)
        selection_path = artifact_uri(hdfs_config, candidate + "/training_selection_manifest")
        keys.write.mode("errorifexists").option("compression", "snappy").parquet(selection_path)
        persisted = (spark.read.option("mergeSchema", "true").option("ignoreCorruptFiles", "false")
                     .option("ignoreMissingFiles", "false").parquet(selection_path))
        if (persisted.schema.simpleString() != keys.schema.simpleString() or
            persisted.exceptAll(keys).unionByName(keys.exceptAll(persisted)).limit(1).count() or
            persisted.count() != selection.summary["selected_count"] or
            persisted.groupBy(*SCOPED_KEY).count().where("count != 1").limit(1).count()):
            raise TrainingError("persisted training key reconciliation failed")
        actual_quotas = {tuple(row[k] for k in STRATA): row["count"]
                         for row in persisted.groupBy(*STRATA).count().limit(20001).collect()}
        expected_quotas = {tuple(row[k] for k in STRATA): row["quota"] for row in selection.summary["strata"]}
        if actual_quotas != expected_quotas:
            raise TrainingError("persisted selected stratum quotas differ")
        rows = (features.join(F.broadcast(evidence["calendar_mapping"]), "target_date")
                .where((F.col("feature_role") == "train") & F.col("day_index").between(29, 1885))
                .join(persisted.select(*SCOPED_KEY), list(SCOPED_KEY)).persist(StorageLevel.DISK_ONLY))
        retained.append(rows)
        if selection_coverage(rows) != {k: selection.summary[k] for k in selection_coverage_keys()}:
            raise TrainingError("persisted selected training coverage differs")
        timings["selection_seconds"] = time.perf_counter() - phase
        phase = time.perf_counter()
        model = fit_training_pipeline(rows, config)
        timings["fit_seconds"] = time.perf_counter() - phase
        model_path = artifact_uri(hdfs_config, candidate + "/spark_pipeline_model")
        phase = time.perf_counter()
        model.write().save(model_path)
        probes = _reload_probes(spark, model, rows, model_path)
        timings["persistence_reload_seconds"] = time.perf_counter() - phase
        transformed_schema = model.transform(rows.limit(1)).schema["features"].metadata
        schema = {"schema_version": "model-feature-schema-v1", "j04_schema": typed_feature_schema(),
                  "normalized_predictors": [{"name": n, "type": "double"} for n in NUMERIC_FEATURES],
                  "predictor_order": list(PREDICTORS), "vector_layout": transformed_schema,
                  "scoring_lag_types": {n: "double" for n in ("lag_1", "lag_7", "lag_28")},
                  "events": "disabled_without_verified_advance_availability",
                  "price_availability": "observed_date_close_strictly_prior_to_target"}
        _write_json(hdfs, candidate + "/feature_schema.json", schema)
        hdfs.mkdir_exclusive(candidate + "/evidence")
        parameters = {p.name: value for p, value in model.stages[-1].extractParamMap().items()}
        timings["total_seconds"] = time.perf_counter() - started
        training = {"input_binding": binding, "selection": selection.summary, "actual_estimator_parameters": parameters,
                    "category_mappings": {"store_id": list(model.stages[1].labels), "dept_id": list(model.stages[2].labels)},
                    "imputed_price_median": model.stages[4].surrogateDF.first()["last_known_price"],
                    "imputer_relative_error": 0.0, "provenance": FORECAST_FEATURES.provenance,
                    "label_provenance": "observed_unit_sales", "reload_probes": probes, "timings": dict(timings),
                    "spark_application_id": spark.sparkContext.applicationId}
        _write_json(hdfs, candidate + "/evidence/training.json", training)
        inventory = model_file_integrity(hdfs, candidate)
        quality = {"status": "VALIDATED", "input_binding": binding, "file_integrity": inventory,
                   "reload_verified": True, "schema_fingerprint": document_digest(schema),
                   "selection_summary": selection.summary, "timings": dict(timings)}
        _write_json(hdfs, staging + "/quality_report.json", quality)
        _write_json(hdfs, candidate + "/evidence/publication.json", quality)
        manifest = make_model_manifest(hdfs_config, binding=binding, quality=quality, summary=selection.summary,
                                       parameters=parameters, created_at=created_at, completed_at=_now())
        _write_json(hdfs, candidate + "/model_manifest.json", manifest)
        validate_model_evidence(hdfs, hdfs_config, manifest, candidate)
        stage_publication(hdfs, hdfs_config, manifest, result_status="VALIDATED")
        return ModelTrainingResult("STAGED", pipeline_run_id, model_run_id, final, staging,
                                   final + "/model_manifest.json", staging + "/quality_report.json", None, timings, ())
    except Exception as error:
        detail = f"{type(error).__name__}: {str(error)[:1000]}"
        timings["total_seconds"] = time.perf_counter() - started
        if owns_attempt:
            try:
                path = staging + "/failure_report.json"
                _write_json(hdfs, path, {**identities, "status": "FAILED", "failure_details": [detail],
                                        "created_at": created_at, "failed_at": _now(), "timings": timings})
                failure_path = path
            except Exception as evidence_error:
                detail += "; failure evidence unavailable: " + str(evidence_error)[:300]
        return ModelTrainingResult("FAILED", pipeline_run_id, model_run_id, final, staging,
                                   None, None, failure_path, timings, (detail,))
    finally:
        for frame in reversed(retained):
            frame.unpersist()


def selection_coverage_keys():
    return ("selected_count", "selected_series_count", "start", "end", "zero_count", "missing_price_count",
            "nonzero_count", "observed_price_count", "day_of_week_coverage", "month_coverage")
