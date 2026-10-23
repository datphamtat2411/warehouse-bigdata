"""J04 historical feature materialization; only the owner publisher promotes artifacts."""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from warehouse_bigdata.common.config import HDFSConfig
from warehouse_bigdata.common.hdfs_paths import (
    render_forecast_features_path,
    render_run_manifest_path,
    render_staging_path,
    validate_feature_version,
    validate_pipeline_run_id,
    validate_source_snapshot_id,
    validate_source_version,
)
from warehouse_bigdata.common.hdfs_publication import (
    document_digest,
    file_integrity,
    stage_publication,
    validate_feature_publication_evidence,
)
from warehouse_bigdata.common.hdfs_runtime import create_hdfs_cli_client
from warehouse_bigdata.contracts.datasets import FORECAST_FEATURES
from warehouse_bigdata.contracts.manifests import PIPELINE_RUN_MANIFEST, VALIDATED
from warehouse_bigdata.contracts.validation import validate_manifest
from warehouse_bigdata.forecasting.feature_validation import (
    SCOPED_KEY,
    typed_feature_schema,
    validate_persisted_feature_snapshot,
)
from warehouse_bigdata.forecasting.features import (
    LAG_DAYS,
    LOOKBACK_DAYS,
    MEAN_DAYS,
    TEST_DAYS,
    TRAIN_DAYS,
    VALIDATION_DAYS,
    build_historical_features,
)
from warehouse_bigdata.processing.enrichment_analytics_snapshot import (
    read_validated_sales_enriched_snapshot,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_PRODUCER = "warehouse-bigdata:feature-snapshot"
_BINDING_VERSION = "feature-input-binding-v1"
_QUALITY_VERSION = "feature-snapshot-quality-v1"
_SIDECAR_VERSION = "feature-dataset-manifest-v1"


class FeatureSnapshotError(ValueError):
    """One immutable feature execution cannot satisfy its input/publication contract."""


@dataclass(frozen=True, slots=True)
class FeatureSnapshotResult:
    status: str
    pipeline_run_id: str
    producing_pipeline_run_id: str | None
    source_version: str
    source_snapshot_id: str
    upstream_pipeline_run_id: str
    feature_version: str
    artifact_path: str
    staging_path: str
    dataset_manifest_path: str | None
    quality_report_path: str | None
    failure_report_path: str | None
    run_manifest_path: str | None
    metrics: dict | None
    timings: dict[str, float]
    failure_details: tuple[str, ...]


def validate_feature_snapshot_parameters(
    *,
    source_version,
    source_snapshot_id,
    upstream_pipeline_run_id,
    feature_version,
    pipeline_run_id,
    output_partitions=16,
    max_records_per_file=1_000_000,
) -> None:
    """Validate all caller identities and layout bounds before any HDFS access."""
    validate_source_version(source_version)
    validate_source_snapshot_id(source_snapshot_id)
    validate_pipeline_run_id(upstream_pipeline_run_id)
    validate_pipeline_run_id(pipeline_run_id)
    validate_feature_version(feature_version)
    for name, value, maximum in (
        ("output_partitions", output_partitions, 256),
        ("max_records_per_file", max_records_per_file, 10_000_000),
    ):
        if type(value) is not int or not 1 <= value <= maximum:
            raise FeatureSnapshotError(f"{name} must be an integer in [1, {maximum}]")


def packaged_code_revision() -> str:
    """Hash the actual runtime-distributed archive, never a constant or guessed Git revision."""
    archive = Path("/opt/warehouse-app/warehouse_bigdata.zip")
    digest = hashlib.sha256()
    with archive.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _utc_now():
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _uri(config, path):
    return config.uri + path


def _read_json(client, path):
    document = json.loads(client.read_remote(path))
    if not isinstance(document, dict):
        raise FeatureSnapshotError(f"feature evidence must be a JSON object: {path}")
    return document


def _write_json(client, path, document):
    """Exclusive metadata write with independent reread; no retry rewrites."""
    payload = (json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n").encode()
    client.upload_bytes(payload, path)
    if client.read_remote(path) != payload:
        raise FeatureSnapshotError(f"feature metadata independent reread differs: {path}")


def _read_parquet(spark, uri):
    return (
        spark.read.option("mergeSchema", "true")
        .option("ignoreCorruptFiles", "false")
        .option("ignoreMissingFiles", "false")
        .parquet(uri)
    )


def _write_candidate(features, exclusions, *, uri, output_partitions, max_records_per_file):
    def write(frame, path):
        (
            frame.repartition(output_partitions)
            .write.mode("errorifexists")
            .option("compression", "snappy")
            .option("maxRecordsPerFile", max_records_per_file)
            .parquet(path)
        )

    write(features, uri)
    write(exclusions, uri + "/_diagnostics/exclusions")


def _effective_configuration(spark, config, output_partitions, max_records_per_file):
    spark_conf = spark.sparkContext.getConf()
    return {
        "hdfs_uri": config.uri,
        "hdfs_base_path": config.base_path,
        "spark_master": spark.sparkContext.master,
        "spark_version": spark.version,
        "runtime": {
            key: spark_conf.get(key, None)
            for key in (
                "spark.driver.memory",
                "spark.executor.memory",
                "spark.executor.cores",
                "spark.cores.max",
                "spark.default.parallelism",
                "spark.dynamicAllocation.enabled",
            )
        },
        "sql": {
            key: spark.conf.get(key)
            for key in (
                "spark.sql.shuffle.partitions",
                "spark.sql.adaptive.enabled",
                "spark.sql.session.timeZone",
                "spark.sql.caseSensitive",
                "spark.sql.ansi.enabled",
            )
        },
        "output_partitions": output_partitions,
        "max_records_per_file": max_records_per_file,
        "compression": "snappy",
        "partition_columns": [],
        "semantics": {
            "contract": FORECAST_FEATURES.version,
            "lookback_days": LOOKBACK_DAYS,
            "lag_days": list(LAG_DAYS),
            "rolling_mean_days": list(MEAN_DAYS),
            "train_days": list(TRAIN_DAYS),
            "validation_days": list(VALIDATION_DAYS),
            "test_days": list(TEST_DAYS),
            "events": "disabled_without_verified_advance_availability",
            "price_availability": "observed_date_close_strictly_prior_to_target",
            "missing_observations": "not_imputed",
            "historical_roles": "one_step_observed_labels",
        },
    }


def _artifact_binding(binding):
    return {key: value for key, value in binding.items() if key != "pipeline_run_id"}


def _task_activity(spark, pipeline_run_id):
    tracker = spark.sparkContext.statusTracker()
    jobs = tracker.getJobIdsForGroup(pipeline_run_id + ":features")
    stages = set()
    for job_id in jobs:
        job = tracker.getJobInfo(job_id)
        if job is not None:
            stages.update(int(stage) for stage in job.stageIds)
    infos = [tracker.getStageInfo(stage) for stage in stages]
    return {
        "job_count": len(jobs),
        "stage_count": len(stages),
        "retained_task_count": sum(info.numTasks for info in infos if info is not None),
        "retained_completed_task_count": sum(
            info.numCompletedTasks for info in infos if info is not None
        ),
        "source": "SparkStatusTracker_retained_stages",
        "spill_bytes": None,
        "gc_time_ms": None,
        "io_bytes": None,
        "unavailable_metrics_source": "read_completed_execution_event_log",
    }


def _manifest(config, binding, sidecar, quality, metrics, created_at, completed_at):
    run = binding["pipeline_run_id"]
    staging = render_staging_path(config.base_path, run)
    coverage = {
        "feature_version": binding["feature_version"],
        "upstream_pipeline_run_id": binding["upstream_pipeline_run_id"],
        "producing_pipeline_run_id": sidecar["pipeline_run_id"],
        "dataset_manifest_uri": binding["artifact_uri"] + "/_dataset_manifest.json",
        "binding_digest": document_digest(binding),
        "quality_digest": document_digest(quality),
        "dataset_manifest_digest": document_digest(sidecar),
        "file_integrity": sidecar["file_integrity"],
        "metrics": metrics,
        "publication_complete": True,
        "persisted_parquet_validated": True,
    }
    manifest = {
        "manifest_schema_version": PIPELINE_RUN_MANIFEST.version,
        "contract_name": PIPELINE_RUN_MANIFEST.name,
        "contract_version": PIPELINE_RUN_MANIFEST.version,
        "manifest_kind": PIPELINE_RUN_MANIFEST.manifest_kind,
        "status": VALIDATED,
        "producer": _PRODUCER,
        "pipeline_run_id": run,
        "source_version": binding["source_version"],
        "source_snapshot_id": binding["source_snapshot_id"],
        "artifact_uri": config.uri + render_run_manifest_path(config.base_path, run),
        "artifact_checksum": None,
        "schema_fingerprint": sidecar["schema_fingerprint"],
        "grain": FORECAST_FEATURES.grain,
        "logical_unique_key": ",".join(SCOPED_KEY),
        "row_count": metrics["row_count"],
        "required_field_null_counts": json.dumps(
            metrics["required_field_null_counts"], sort_keys=True
        ),
        "code_revision": binding["code_revision"],
        "configuration_digest": binding["configuration_digest"],
        "created_at": created_at,
        "completed_at": completed_at,
        "coverage_and_exclusion_counts": json.dumps(
            coverage, sort_keys=True, separators=(",", ":")
        ),
        "output_dataset_names": '["forecast_features"]',
        "output_dataset_uris": json.dumps([binding["artifact_uri"]]),
        "quality_report_uri": config.uri + staging + "/quality_report.json",
    }
    validate_manifest(
        PIPELINE_RUN_MANIFEST, manifest, lineage_context={"required_status": VALIDATED}
    )
    return manifest


def run_feature_snapshot(
    spark: SparkSession,
    *,
    source_version: str,
    source_snapshot_id: str,
    upstream_pipeline_run_id: str,
    feature_version: str,
    pipeline_run_id: str,
    hdfs_config: HDFSConfig,
    spark_master: str | None = None,
    output_partitions: int = 16,
    max_records_per_file: int = 1_000_000,
    client: Any | None = None,
) -> FeatureSnapshotResult:
    """Read a validated J03 snapshot, call T01 unchanged, and stage a frozen owner request.

    An attempt's binding is exclusive and never used to authorize recomputation.
    Recovery of a frozen request belongs to the owner publisher before driver launch.
    New executions may independently validate and reuse an already published artifact.
    """
    validate_feature_snapshot_parameters(
        source_version=source_version,
        source_snapshot_id=source_snapshot_id,
        upstream_pipeline_run_id=upstream_pipeline_run_id,
        feature_version=feature_version,
        pipeline_run_id=pipeline_run_id,
        output_partitions=output_partitions,
        max_records_per_file=max_records_per_file,
    )
    if spark_master is not None and (
        not spark_master.startswith("spark://") or spark.sparkContext.master != spark_master
    ):
        raise FeatureSnapshotError("explicit Spark Standalone master differs from effective master")
    spark.sparkContext.setJobGroup(
        pipeline_run_id + ":features", "J04 historical feature snapshot", True
    )
    staging = render_staging_path(hdfs_config.base_path, pipeline_run_id)
    artifact = render_forecast_features_path(
        hdfs_config.base_path, source_snapshot_id, feature_version
    )
    candidate = staging + "/candidates/forecast_features"
    binding_path, quality_path = staging + "/input_binding.json", staging + "/quality_report.json"
    failure_path = staging + "/failure_report.json"
    started = time.perf_counter()
    created_at = _utc_now()
    timings: dict[str, float] = {}
    retained = []
    hdfs = client
    owns_attempt = False
    producer = None
    metrics = None
    try:
        hdfs = create_hdfs_cli_client(hdfs_config) if hdfs is None else hdfs
        if not hdfs.is_directory(staging):
            raise FeatureSnapshotError("owner-prepared execution staging directory is required")
        for path in (
            binding_path,
            failure_path,
            quality_path,
            staging + "/publication/request.json",
            staging + "/candidates",
            candidate,
            render_run_manifest_path(hdfs_config.base_path, pipeline_run_id),
        ):
            if hdfs.exists(path):
                raise FeatureSnapshotError(
                    "execution already has evidence; use owner recovery or a new pipeline_run_id"
                )
        # This planned candidate parent is also the exclusive execution claim.
        # A losing simultaneous invocation cannot append failure evidence to its winner.
        hdfs.mkdir_exclusive(staging + "/candidates")
        owns_attempt = True
        phase = time.perf_counter()
        source, upstream_evidence = read_validated_sales_enriched_snapshot(
            spark,
            source_version=source_version,
            source_snapshot_id=source_snapshot_id,
            pipeline_run_id=upstream_pipeline_run_id,
            hdfs_config=hdfs_config,
            client=hdfs,
        )
        timings["upstream_validation_seconds"] = time.perf_counter() - phase
        configuration = _effective_configuration(
            spark, hdfs_config, output_partitions, max_records_per_file
        )
        binding = {
            "schema_version": _BINDING_VERSION,
            "source_version": source_version,
            "source_snapshot_id": source_snapshot_id,
            "pipeline_run_id": pipeline_run_id,
            "upstream_pipeline_run_id": upstream_pipeline_run_id,
            "feature_version": feature_version,
            "upstream_evidence": upstream_evidence,
            "code_revision": packaged_code_revision(),
            "configuration_digest": document_digest(configuration),
            "effective_configuration": configuration,
            "artifact_uri": hdfs_config.uri + artifact,
        }
        _write_json(hdfs, binding_path, binding)
        phase = time.perf_counter()
        expected = build_historical_features(
            source, source_snapshot_id=source_snapshot_id, feature_version=feature_version
        )
        from pyspark import StorageLevel

        for frame in (expected.features, expected.eligibility, expected.coverage):
            frame.persist(StorageLevel.DISK_ONLY)
            retained.append(frame)
        timings["transformation_seconds"] = time.perf_counter() - phase
        phase = time.perf_counter()
        reuse = hdfs.exists(artifact)
        if reuse:
            sidecar = _read_json(hdfs, artifact + "/_dataset_manifest.json")
            producer = validate_pipeline_run_id(sidecar.get("pipeline_run_id"))
            original = _read_json(hdfs, render_run_manifest_path(hdfs_config.base_path, producer))
            validate_feature_publication_evidence(hdfs, hdfs_config, original, artifact)
            if (
                sidecar.get("schema_version") != _SIDECAR_VERSION
                or _artifact_binding(sidecar.get("input_binding", {})) != _artifact_binding(binding)
                or sidecar.get("schema") != typed_feature_schema()
                or sidecar.get("provenance") != FORECAST_FEATURES.provenance
            ):
                raise FeatureSnapshotError(
                    "published artifact conflicts with source/code/configuration"
                )
            target = artifact
        else:
            producer = pipeline_run_id
            _write_candidate(
                expected.features,
                expected.eligibility.where("status != 'ELIGIBLE'"),
                uri=_uri(hdfs_config, candidate),
                output_partitions=output_partitions,
                max_records_per_file=max_records_per_file,
            )
            target = candidate
        timings["materialization_seconds"] = time.perf_counter() - phase
        phase = time.perf_counter()
        persisted = _read_parquet(spark, _uri(hdfs_config, target))
        exclusions = _read_parquet(spark, _uri(hdfs_config, target + "/_diagnostics/exclusions"))
        metrics = validate_persisted_feature_snapshot(
            persisted,
            expected=expected,
            source=source,
            exclusions=exclusions,
            source_snapshot_id=source_snapshot_id,
            feature_version=feature_version,
        )
        inventory = file_integrity(hdfs, target)
        timings["persisted_validation_seconds"] = time.perf_counter() - phase
        completed_at = _utc_now()
        schema = typed_feature_schema()
        fingerprint = document_digest({"schema": schema})
        if reuse:
            if (
                sidecar.get("metrics") != metrics
                or sidecar.get("file_integrity") != inventory
                or sidecar.get("schema_fingerprint") != fingerprint
            ):
                raise FeatureSnapshotError("reused artifact content/schema/coverage differs")
        else:
            sidecar = {
                **binding,
                "schema_version": _SIDECAR_VERSION,
                "input_binding": binding,
                "status": VALIDATED,
                "dataset_name": FORECAST_FEATURES.name,
                "dataset_version": FORECAST_FEATURES.version,
                "provenance": FORECAST_FEATURES.provenance,
                "schema": schema,
                "schema_fingerprint": fingerprint,
                "schema_fingerprint_definition": "sha256 of canonical JSON {schema: ordered typed logical fields}",
                "logical_unique_key": list(SCOPED_KEY),
                "metrics": metrics,
                "row_count": metrics["row_count"],
                "file_integrity": inventory,
                "artifact_checksum": None,
                "persisted_parquet_validated": True,
                "quality_report_uri": hdfs_config.uri + quality_path,
                "created_at": created_at,
                "completed_at": completed_at,
            }
            _write_json(hdfs, candidate + "/_dataset_manifest.json", sidecar)
        timings["total_seconds"] = time.perf_counter() - started
        quality = {
            **binding,
            "quality_schema_version": _QUALITY_VERSION,
            "status": VALIDATED,
            "producing_pipeline_run_id": producer,
            "metrics": metrics,
            "schema": schema,
            "schema_fingerprint": fingerprint,
            "file_integrity": inventory,
            "persisted_parquet_validated": True,
            "created_at": created_at,
            "completed_at": completed_at,
            "spark_application_id": spark.sparkContext.applicationId,
            "timings": dict(timings),
            "file_count": len(inventory),
            "file_bytes": sum(f["size_bytes"] for f in inventory),
            "task_metrics": _task_activity(spark, pipeline_run_id),
            "event_log_dir": spark.sparkContext.getConf().get("spark.eventLog.dir", None),
        }
        _write_json(hdfs, quality_path, quality)
        manifest = _manifest(
            hdfs_config, binding, sidecar, quality, metrics, created_at, completed_at
        )
        stage_publication(
            hdfs, hdfs_config, manifest, result_status="REUSED" if reuse else VALIDATED
        )
        return FeatureSnapshotResult(
            "REUSED" if reuse else "STAGED",
            pipeline_run_id,
            producer,
            source_version,
            source_snapshot_id,
            upstream_pipeline_run_id,
            feature_version,
            artifact,
            staging,
            artifact + "/_dataset_manifest.json",
            quality_path,
            None,
            None,
            metrics,
            timings,
            (),
        )
    except Exception as error:
        detail = f"{type(error).__name__}: {str(error)[:1000]}"
        failure = None
        timings["total_seconds"] = time.perf_counter() - started
        if hdfs is not None and owns_attempt:
            try:
                _write_json(
                    hdfs,
                    failure_path,
                    {
                        "status": "FAILED",
                        "pipeline_run_id": pipeline_run_id,
                        "source_version": source_version,
                        "source_snapshot_id": source_snapshot_id,
                        "upstream_pipeline_run_id": upstream_pipeline_run_id,
                        "feature_version": feature_version,
                        "artifact_path": artifact,
                        "created_at": created_at,
                        "failed_at": _utc_now(),
                        "timings": timings,
                        "failure_details": [detail],
                    },
                )
                failure = failure_path
            except Exception as evidence_error:
                detail += f"; failure evidence unavailable: {str(evidence_error)[:300]}"
        return FeatureSnapshotResult(
            "FAILED",
            pipeline_run_id,
            producer,
            source_version,
            source_snapshot_id,
            upstream_pipeline_run_id,
            feature_version,
            artifact,
            staging,
            None,
            None,
            failure,
            None,
            metrics,
            timings,
            (detail,),
        )
    finally:
        for frame in retained:
            frame.unpersist()
