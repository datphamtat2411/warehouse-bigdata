"""Bounded, retry-safe materialization of the Contract v1 sales snapshot."""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

from warehouse_bigdata.common.config import HDFSConfig
from warehouse_bigdata.common.hdfs_paths import (
    HdfsPathError,
    render_curated_sales_daily_path,
    render_normalization_candidate_path,
    render_normalization_chunk_manifest_path,
    render_normalization_chunk_path,
    render_normalization_chunks_path,
    render_normalization_failure_path,
    render_normalization_quality_path,
    render_run_manifest_path,
    render_staging_path,
    validate_pipeline_run_id,
    validate_source_snapshot_id,
    validate_source_version,
)
from warehouse_bigdata.common.hdfs_runtime import create_hdfs_cli_client
from warehouse_bigdata.contracts.datasets import SALES_DAILY
from warehouse_bigdata.contracts.manifests import FAILED, PIPELINE_RUN_MANIFEST, VALIDATED
from warehouse_bigdata.contracts.validation import ContractValidationError, validate_manifest
from warehouse_bigdata.ingestion.hdfs_raw import (
    HdfsRawIngestionError,
    ValidatedHdfsRawSnapshot,
    validate_hdfs_raw_snapshot,
)
from warehouse_bigdata.ingestion.m5_source import (
    REFERENCE_EVALUATION_DAYS,
    REFERENCE_EVALUATION_SERIES,
)
from warehouse_bigdata.processing.sales_normalization import (
    NormalizationMetrics,
    SalesNormalizationConfig,
    measure_sales_daily,
    normalize_sales_daily,
    validate_sales_daily,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


_MAX_FAILURE_DETAILS = 8
_MAX_FAILURE_LENGTH = 1_000
_SALES_SOURCE_FILES = frozenset({"calendar.csv", "sales_train_evaluation.csv"})


class NormalizationSnapshotError(RuntimeError):
    """Raised when a normalized snapshot cannot pass an immutable quality gate."""


@dataclass(frozen=True, slots=True)
class NormalizationSnapshotConfig:
    """Bounded full-history configuration and measured cardinality expectations."""

    days_per_chunk: int = 128
    expected_series_count: int = REFERENCE_EVALUATION_SERIES
    expected_day_count: int = REFERENCE_EVALUATION_DAYS
    expected_row_count: int | None = None

    def __post_init__(self) -> None:
        values = {
            "days_per_chunk": self.days_per_chunk,
            "expected_series_count": self.expected_series_count,
            "expected_day_count": self.expected_day_count,
        }
        if any(type(value) is not int for value in values.values()):
            raise ValueError("normalization snapshot cardinality values must be integers")
        if not 1 <= self.days_per_chunk <= REFERENCE_EVALUATION_DAYS:
            raise ValueError("days_per_chunk must be from 1 through 1941")
        if self.expected_series_count < 1:
            raise ValueError("expected_series_count must be positive")
        if not 1 <= self.expected_day_count <= REFERENCE_EVALUATION_DAYS:
            raise ValueError("expected_day_count must be from 1 through 1941")
        if self.expected_row_count is not None:
            if type(self.expected_row_count) is not int or self.expected_row_count < 1:
                raise ValueError("expected_row_count must be a positive integer or null")
            if self.expected_row_count != self.expected_series_count * self.expected_day_count:
                raise ValueError(
                    "expected_row_count must equal expected_series_count * expected_day_count"
                )

    @property
    def row_count(self) -> int:
        """Return the exact expected expanded-row count."""
        return self.expected_series_count * self.expected_day_count


@dataclass(frozen=True, slots=True)
class NormalizationChunk:
    """One contiguous day-range staging unit."""

    index: int
    day_start: int
    day_end: int
    parquet_path: str
    manifest_path: str

    @property
    def day_count(self) -> int:
        """Return the number of day keys in this chunk."""
        return self.day_end - self.day_start + 1

    @property
    def day_keys(self) -> tuple[str, ...]:
        """Return canonical M5 day keys without collecting any Spark rows."""
        return tuple(f"d_{day}" for day in range(self.day_start, self.day_end + 1))


@dataclass(frozen=True, slots=True)
class NormalizationSnapshotResult:
    """Bounded outcome and immutable HDFS evidence for one snapshot attempt."""

    status: str
    pipeline_run_id: str
    source_version: str
    source_snapshot_id: str
    raw_path: str | None
    staging_path: str
    curated_path: str
    quality_report_path: str | None
    run_manifest_path: str | None
    metrics: NormalizationMetrics | None
    chunks: tuple[NormalizationChunk, ...]
    timings: Mapping[str, float]
    failure_details: tuple[str, ...]


def validate_normalization_identifiers(
    source_version: str,
    source_snapshot_id: str,
    pipeline_run_id: str,
) -> None:
    """Require all lineage identities explicitly and reject path-unsafe values."""
    try:
        validate_source_version(source_version)
        validate_source_snapshot_id(source_snapshot_id)
        validate_pipeline_run_id(pipeline_run_id)
    except HdfsPathError as error:
        raise NormalizationSnapshotError(str(error)) from error


def build_chunk_plan(
    config: NormalizationSnapshotConfig,
    staging_path: str,
) -> tuple[NormalizationChunk, ...]:
    """Build a deterministic contiguous plan over the configured full history."""
    chunks: list[NormalizationChunk] = []
    day_start = 1
    index = 0
    while day_start <= config.expected_day_count:
        day_end = min(day_start + config.days_per_chunk - 1, config.expected_day_count)
        chunks.append(
            NormalizationChunk(
                index=index,
                day_start=day_start,
                day_end=day_end,
                parquet_path=render_normalization_chunk_path(staging_path, day_start, day_end),
                manifest_path=render_normalization_chunk_manifest_path(
                    staging_path,
                    day_start,
                    day_end,
                ),
            )
        )
        index += 1
        day_start = day_end + 1
    result = tuple(chunks)
    validate_chunk_coverage(result, day_start=1, day_end=config.expected_day_count)
    return result


def validate_chunk_coverage(
    chunks: Sequence[NormalizationChunk],
    *,
    day_start: int,
    day_end: int,
) -> None:
    """Require unique, ordered, gap-free coverage of the selected day range."""
    if not chunks or type(day_start) is not int or type(day_end) is not int:
        raise NormalizationSnapshotError("normalization chunk coverage is empty or invalid")
    ordered = tuple(sorted(chunks, key=lambda chunk: (chunk.day_start, chunk.day_end)))
    if ordered[0].day_start != day_start or ordered[-1].day_end != day_end:
        raise NormalizationSnapshotError("normalization chunk coverage does not span the full range")
    expected_start = day_start
    seen_ranges: set[tuple[int, int]] = set()
    for chunk in ordered:
        current = (chunk.day_start, chunk.day_end)
        if current in seen_ranges or chunk.day_start != expected_start or chunk.day_start > chunk.day_end:
            raise NormalizationSnapshotError("normalization chunk coverage contains a gap or overlap")
        seen_ranges.add(current)
        expected_start = chunk.day_end + 1
    if expected_start != day_end + 1:
        raise NormalizationSnapshotError("normalization chunk coverage is incomplete")


def run_normalization_snapshot(
    spark: SparkSession,
    *,
    source_version: str,
    source_snapshot_id: str,
    pipeline_run_id: str,
    hdfs_config: HDFSConfig,
    spark_master: str | None = None,
    client: Any | None = None,
    config: NormalizationSnapshotConfig | None = None,
    now: Callable[[], str] | None = None,
) -> NormalizationSnapshotResult:
    """Normalize validated HDFS M5 sources into one immutable curated snapshot."""
    validate_normalization_identifiers(source_version, source_snapshot_id, pipeline_run_id)
    if spark_master is not None:
        if not isinstance(spark_master, str) or not spark_master.startswith("spark://"):
            raise NormalizationSnapshotError(
                "normalization snapshot requires an explicit Spark Standalone master"
            )
        actual_master = spark.sparkContext.master
        if actual_master != spark_master:
            raise NormalizationSnapshotError(
                f"Spark master mismatch: expected {spark_master!r}, received {actual_master!r}"
            )
    selected_config = config or NormalizationSnapshotConfig()
    timestamp = _utc_now if now is None else now
    started_at = time.perf_counter()
    timings: dict[str, float] = {}
    staging_path = render_staging_path(hdfs_config.base_path, pipeline_run_id)
    curated_path = render_curated_sales_daily_path(
        hdfs_config.base_path,
        source_snapshot_id,
    )
    chunks = build_chunk_plan(selected_config, staging_path)
    quality_path = render_normalization_quality_path(staging_path)
    hdfs = client
    raw_path: str | None = None

    try:
        hdfs = create_hdfs_cli_client(hdfs_config) if hdfs is None else hdfs
        phase_started = time.perf_counter()
        raw_snapshot = validate_hdfs_raw_snapshot(
            source_version,
            client=hdfs,
            config=hdfs_config,
        )
        raw_path = raw_snapshot.raw_path
        _validate_raw_inputs(raw_snapshot)
        timings["raw_validation_seconds"] = time.perf_counter() - phase_started
        _ensure_staging_root(hdfs, staging_path)
        hdfs.mkdir_parents(render_normalization_chunks_path(staging_path))
        hdfs.mkdir_parents(str(PurePosixPath(staging_path) / "chunk_manifests"))

        phase_started = time.perf_counter()
        sales_wide, calendar = _read_validated_sources(spark, hdfs_config, raw_snapshot)
        measured_series_count = sales_wide.count()
        timings["source_read_seconds"] = time.perf_counter() - phase_started
        if measured_series_count != selected_config.expected_series_count:
            raise NormalizationSnapshotError(
                "series coverage mismatch: "
                f"expected {selected_config.expected_series_count}, received {measured_series_count}"
            )

        chunk_metrics_list: list[NormalizationMetrics] = []
        for chunk in chunks:
            phase_started = time.perf_counter()
            chunk_metrics_list.append(
                _stage_or_verify_chunk(
                    spark,
                    hdfs,
                    hdfs_config,
                    sales_wide,
                    calendar,
                    chunk,
                    expected_series_count=measured_series_count,
                    source_version=source_version,
                    source_snapshot_id=source_snapshot_id,
                    pipeline_run_id=pipeline_run_id,
                )
            )
            timings[
                f"chunk_{chunk.day_start:04d}_{chunk.day_end:04d}_seconds"
            ] = time.perf_counter() - phase_started
        chunk_metrics = tuple(chunk_metrics_list)
        _validate_staged_chunk_listing(hdfs, staging_path, chunks)
        validate_chunk_coverage(
            chunks,
            day_start=1,
            day_end=selected_config.expected_day_count,
        )
        expected_metrics = _reconcile_chunk_metrics(
            chunk_metrics,
            expected_series_count=measured_series_count,
            expected_day_count=selected_config.expected_day_count,
            expected_row_count=selected_config.row_count,
        )

        compaction_started = time.perf_counter()
        candidate_uri = _hdfs_uri(
            hdfs_config.uri,
            render_normalization_candidate_path(staging_path),
        )
        curated_uri = _hdfs_uri(hdfs_config.uri, curated_path)
        curated_exists = _exists(hdfs, curated_path)
        if curated_exists:
            if not hdfs.is_directory(curated_path):
                raise NormalizationSnapshotError("existing curated path is not a directory")
            persisted = _read_parquet(spark, curated_uri)
            metrics = _validate_persisted_output(
                persisted,
                expected_metrics=expected_metrics,
                selected_day_keys=_day_keys(selected_config.expected_day_count),
            )
            _validate_dataset_manifest(
                hdfs,
                curated_path,
                hdfs_config,
                source_version=source_version,
                source_snapshot_id=source_snapshot_id,
                expected_metrics=metrics,
                expected_row_count=selected_config.row_count,
            )
            status = "REUSED"
        else:
            candidate_exists = _exists(hdfs, render_normalization_candidate_path(staging_path))
            if candidate_exists:
                if not hdfs.is_directory(render_normalization_candidate_path(staging_path)):
                    raise NormalizationSnapshotError("existing candidate path is not a directory")
                persisted = _read_parquet(spark, candidate_uri)
                metrics = _validate_persisted_output(
                    persisted,
                    expected_metrics=expected_metrics,
                    selected_day_keys=_day_keys(selected_config.expected_day_count),
                )
                _validate_dataset_manifest(
                    hdfs,
                    render_normalization_candidate_path(staging_path),
                    hdfs_config,
                    source_version=source_version,
                    source_snapshot_id=source_snapshot_id,
                    expected_metrics=metrics,
                    expected_row_count=selected_config.row_count,
                )
            else:
                compacted = _read_parquet(
                    spark,
                    *(_hdfs_uri(hdfs_config.uri, chunk.parquet_path) for chunk in chunks),
                )
                _validate_persisted_output(
                    compacted,
                    expected_metrics=expected_metrics,
                    selected_day_keys=_day_keys(selected_config.expected_day_count),
                )
                _write_parquet_once(spark, compacted, candidate_uri)
                persisted = _read_parquet(spark, candidate_uri)
                metrics = _validate_persisted_output(
                    persisted,
                    expected_metrics=expected_metrics,
                    selected_day_keys=_day_keys(selected_config.expected_day_count),
                )
                _write_dataset_manifest(
                    hdfs,
                    render_normalization_candidate_path(staging_path),
                    hdfs_config,
                    source_version=source_version,
                    source_snapshot_id=source_snapshot_id,
                    pipeline_run_id=pipeline_run_id,
                    curated_path=curated_path,
                    metrics=metrics,
                    chunks=chunks,
                    created_at=timestamp(),
                )

            _write_quality_report(
                hdfs,
                quality_path,
                _quality_document(
                    status=VALIDATED,
                    source_version=source_version,
                    source_snapshot_id=source_snapshot_id,
                    pipeline_run_id=pipeline_run_id,
                    raw_path=raw_snapshot.raw_path,
                    curated_path=curated_path,
                    metrics=metrics,
                    chunks=chunks,
                    expected_series_count=selected_config.expected_series_count,
                    expected_day_count=selected_config.expected_day_count,
                    expected_row_count=selected_config.row_count,
                    failure_details=(),
                ),
            )
            hdfs.mkdir_parents(str(PurePosixPath(curated_path).parent))
            try:
                hdfs.rename(render_normalization_candidate_path(staging_path), curated_path)
                status = "VALIDATED"
            except Exception as error:
                if not _exists(hdfs, curated_path):
                    raise NormalizationSnapshotError(
                        f"curated snapshot promotion failed: {error}"
                    ) from error
                if not hdfs.is_directory(curated_path):
                    raise NormalizationSnapshotError(
                        "curated path appeared during promotion but is not a directory"
                    ) from error
                persisted = _read_parquet(spark, curated_uri)
                metrics = _validate_persisted_output(
                    persisted,
                    expected_metrics=expected_metrics,
                    selected_day_keys=_day_keys(selected_config.expected_day_count),
                )
                _validate_dataset_manifest(
                    hdfs,
                    curated_path,
                    hdfs_config,
                    source_version=source_version,
                    source_snapshot_id=source_snapshot_id,
                    expected_metrics=metrics,
                    expected_row_count=selected_config.row_count,
                )
                status = "REUSED"

        timings["compaction_seconds"] = time.perf_counter() - compaction_started
        _write_quality_report(
            hdfs,
            quality_path,
            _quality_document(
                status=VALIDATED,
                source_version=source_version,
                source_snapshot_id=source_snapshot_id,
                pipeline_run_id=pipeline_run_id,
                raw_path=raw_snapshot.raw_path,
                curated_path=curated_path,
                metrics=metrics,
                chunks=chunks,
                expected_series_count=selected_config.expected_series_count,
                expected_day_count=selected_config.expected_day_count,
                expected_row_count=selected_config.row_count,
                failure_details=(),
            ),
        )
        manifest = _build_pipeline_manifest(
            hdfs_config,
            source_version=source_version,
            source_snapshot_id=source_snapshot_id,
            pipeline_run_id=pipeline_run_id,
            curated_path=curated_path,
            quality_path=quality_path,
            metrics=metrics,
            chunks=chunks,
            created_at=timestamp(),
            completed_at=timestamp(),
            status=VALIDATED,
        )
        manifest_path = _publish_or_reuse_run_manifest(
            hdfs,
            hdfs_config,
            manifest,
            integrity=raw_snapshot.integrity,
        )
        timings["total_seconds"] = time.perf_counter() - started_at
        return NormalizationSnapshotResult(
            status=status,
            pipeline_run_id=pipeline_run_id,
            source_version=source_version,
            source_snapshot_id=source_snapshot_id,
            raw_path=raw_path,
            staging_path=staging_path,
            curated_path=curated_path,
            quality_report_path=quality_path,
            run_manifest_path=manifest_path,
            metrics=metrics,
            chunks=chunks,
            timings=dict(timings),
            failure_details=(),
        )
    except Exception as error:
        timings["total_seconds"] = time.perf_counter() - started_at
        detail = _failure_detail(error)
        failure_path = None
        if hdfs is not None:
            try:
                _ensure_staging_root(hdfs, staging_path)
                failure_path = render_normalization_failure_path(staging_path)
                _write_quality_report(
                    hdfs,
                    failure_path,
                    _quality_document(
                        status=FAILED,
                        source_version=source_version,
                        source_snapshot_id=source_snapshot_id,
                        pipeline_run_id=pipeline_run_id,
                        raw_path=raw_path,
                        curated_path=curated_path,
                        metrics=None,
                        chunks=chunks,
                        expected_series_count=selected_config.expected_series_count,
                        expected_day_count=selected_config.expected_day_count,
                        expected_row_count=selected_config.row_count,
                        failure_details=(detail,),
                    ),
                )
            except Exception as evidence_error:
                detail = f"{detail}; failure evidence unavailable: {_failure_detail(evidence_error)}"
        return NormalizationSnapshotResult(
            status=FAILED,
            pipeline_run_id=pipeline_run_id,
            source_version=source_version,
            source_snapshot_id=source_snapshot_id,
            raw_path=raw_path,
            staging_path=staging_path,
            curated_path=curated_path,
            quality_report_path=failure_path,
            run_manifest_path=None,
            metrics=None,
            chunks=chunks,
            timings=dict(timings),
            failure_details=(detail,),
        )


def _read_validated_sources(
    spark: SparkSession,
    hdfs_config: HDFSConfig,
    raw_snapshot: ValidatedHdfsRawSnapshot,
) -> tuple[DataFrame, DataFrame]:
    names = {manifest.get("source_file") for manifest in raw_snapshot.manifests}
    if not _SALES_SOURCE_FILES <= names:
        raise NormalizationSnapshotError("validated raw snapshot lacks required sales/calendar sources")
    raw_uri = _hdfs_uri(hdfs_config.uri, raw_snapshot.raw_path)
    reader = spark.read.option("header", True).option("inferSchema", False)
    return (
        reader.csv(f"{raw_uri}/sales_train_evaluation.csv"),
        reader.csv(f"{raw_uri}/calendar.csv"),
    )


def _stage_or_verify_chunk(
    spark: SparkSession,
    hdfs: Any,
    hdfs_config: HDFSConfig,
    sales_wide: DataFrame,
    calendar: DataFrame,
    chunk: NormalizationChunk,
    *,
    expected_series_count: int,
    source_version: str,
    source_snapshot_id: str,
    pipeline_run_id: str,
) -> NormalizationMetrics:
    normalized = normalize_sales_daily(
        sales_wide,
        calendar,
        config=SalesNormalizationConfig(
            day_start=chunk.day_start,
            day_end=chunk.day_end,
            days_per_chunk=chunk.day_count,
        ),
    )
    source_metrics = measure_sales_daily(normalized)
    data_path = chunk.parquet_path
    data_uri = _hdfs_uri(hdfs_config.uri, data_path)
    if _exists(hdfs, data_path):
        if not hdfs.is_directory(data_path):
            raise NormalizationSnapshotError(f"chunk path is not a directory: {data_path}")
        manifest = _read_json(hdfs, chunk.manifest_path, "chunk manifest")
        _validate_chunk_manifest(
            manifest,
            chunk,
            expected_series_count=expected_series_count,
            expected_metrics=source_metrics,
            hdfs_config=hdfs_config,
            source_version=source_version,
            source_snapshot_id=source_snapshot_id,
            pipeline_run_id=pipeline_run_id,
        )
    else:
        _write_parquet_once(spark, normalized, data_uri)
        persisted = _read_parquet(spark, data_uri)
        persisted_metrics = _validate_persisted_output(
            persisted,
            expected_metrics=source_metrics,
            selected_day_keys=chunk.day_keys,
        )
        if persisted_metrics != source_metrics:
            raise NormalizationSnapshotError(f"persisted chunk metrics differ: {data_path}")
        _write_quality_report(
            hdfs,
            chunk.manifest_path,
            _chunk_document(
                chunk,
                hdfs_config=hdfs_config,
                expected_series_count=expected_series_count,
                metrics=source_metrics,
                source_version=source_version,
                source_snapshot_id=source_snapshot_id,
                pipeline_run_id=pipeline_run_id,
            ),
        )
    persisted = _read_parquet(spark, data_uri)
    persisted_metrics = _validate_persisted_output(
        persisted,
        expected_metrics=source_metrics,
        selected_day_keys=chunk.day_keys,
    )
    if persisted_metrics != source_metrics:
        raise NormalizationSnapshotError(f"reused chunk metrics differ: {data_path}")
    return persisted_metrics


def _validate_persisted_output(
    persisted: DataFrame,
    *,
    expected_metrics: NormalizationMetrics,
    selected_day_keys: Sequence[str],
) -> NormalizationMetrics:
    return validate_sales_daily(
        persisted,
        expected_series_count=expected_metrics.series_count,
        selected_day_keys=selected_day_keys,
        expected_unit_sales_total=expected_metrics.unit_sales_total,
        expected_zero_sales_count=expected_metrics.zero_sales_count,
    )


def _reconcile_chunk_metrics(
    metrics: Sequence[NormalizationMetrics],
    *,
    expected_series_count: int,
    expected_day_count: int,
    expected_row_count: int,
) -> NormalizationMetrics:
    if not metrics:
        raise NormalizationSnapshotError("normalization produced no chunk metrics")
    row_count = sum(item.row_count for item in metrics)
    day_count = sum(item.day_count for item in metrics)
    unit_sales_total = sum(item.unit_sales_total for item in metrics)
    zero_sales_count = sum(item.zero_sales_count for item in metrics)
    if row_count != expected_row_count or day_count != expected_day_count:
        raise NormalizationSnapshotError(
            "exact normalization reconciliation failed: "
            f"rows={row_count}/{expected_row_count}, days={day_count}/{expected_day_count}"
        )
    if any(item.series_count != expected_series_count for item in metrics):
        raise NormalizationSnapshotError("normalization chunk series reconciliation failed")
    return NormalizationMetrics(
        row_count=row_count,
        series_count=expected_series_count,
        day_count=day_count,
        unit_sales_total=unit_sales_total,
        zero_sales_count=zero_sales_count,
    )


def _validate_staged_chunk_listing(
    hdfs: Any,
    staging_path: str,
    chunks: Sequence[NormalizationChunk],
) -> None:
    expected_data = {chunk.parquet_path for chunk in chunks}
    expected_manifests = {chunk.manifest_path for chunk in chunks}
    actual_data = set(hdfs.list_children(render_normalization_chunks_path(staging_path)))
    actual_manifests = set(
        hdfs.list_children(str(PurePosixPath(staging_path) / "chunk_manifests"))
    )
    if actual_data != expected_data or actual_manifests != expected_manifests:
        raise NormalizationSnapshotError(
            "staged chunk coverage is incomplete or contains unexpected artifacts"
        )


def _write_parquet_once(spark: SparkSession, dataframe: DataFrame, uri: str) -> None:
    dataframe.write.mode("errorifexists").parquet(uri)


def _read_parquet(spark: SparkSession, *uris: str) -> DataFrame:
    if not uris:
        raise NormalizationSnapshotError("cannot read Parquet without a path")
    return spark.read.option("pathGlobFilter", "*.parquet").parquet(*uris)


def _day_keys(day_count: int) -> tuple[str, ...]:
    return tuple(f"d_{day}" for day in range(1, day_count + 1))


def _ensure_staging_root(hdfs: Any, path: str) -> None:
    if _exists(hdfs, path):
        if not hdfs.is_directory(path):
            raise NormalizationSnapshotError(f"staging path is not a directory: {path}")
        return
    hdfs.mkdir_parents(str(PurePosixPath(path).parent))
    try:
        hdfs.mkdir_exclusive(path)
    except Exception as error:
        if not _exists(hdfs, path):
            raise NormalizationSnapshotError(
                f"staging path was not exclusively created: {path}"
            ) from error
        if not hdfs.is_directory(path):
            raise NormalizationSnapshotError(f"staging path is not a directory: {path}") from error


def _write_quality_report(hdfs: Any, path: str, document: Mapping[str, object]) -> None:
    _write_json_once(hdfs, path, document)


def _write_json_once(hdfs: Any, path: str, document: Mapping[str, object]) -> None:
    payload = _json_bytes(document)
    if _exists(hdfs, path):
        if hdfs.read_remote(path) != payload:
            raise NormalizationSnapshotError(f"refusing to overwrite immutable evidence: {path}")
        return
    hdfs.upload_bytes(payload, path)
    if hdfs.read_remote(path) != payload:
        raise NormalizationSnapshotError(f"persisted evidence differs from candidate: {path}")


def _validate_chunk_manifest(
    manifest: Mapping[str, object],
    chunk: NormalizationChunk,
    *,
    expected_series_count: int,
    expected_metrics: NormalizationMetrics,
    hdfs_config: HDFSConfig,
    source_version: str,
    source_snapshot_id: str,
    pipeline_run_id: str,
) -> None:
    expected = _chunk_document(
        chunk,
        hdfs_config=hdfs_config,
        expected_series_count=expected_series_count,
        metrics=expected_metrics,
        source_version=source_version,
        source_snapshot_id=source_snapshot_id,
        pipeline_run_id=pipeline_run_id,
    )
    if dict(manifest) != expected:
        raise NormalizationSnapshotError(
            f"chunk manifest does not match the validated source chunk: {chunk.parquet_path}"
        )


def _chunk_document(
    chunk: NormalizationChunk,
    *,
    hdfs_config: HDFSConfig,
    expected_series_count: int,
    metrics: NormalizationMetrics,
    source_version: str,
    source_snapshot_id: str,
    pipeline_run_id: str,
) -> dict[str, object]:
    return {
        "schema_version": "normalization-chunk-v1",
        "status": VALIDATED,
        "source_version": source_version,
        "source_snapshot_id": source_snapshot_id,
        "pipeline_run_id": pipeline_run_id,
        "day_start": chunk.day_start,
        "day_end": chunk.day_end,
        "day_count": chunk.day_count,
        "expected_series_count": expected_series_count,
        "row_count": metrics.row_count,
        "series_count": metrics.series_count,
        "unit_sales_total": metrics.unit_sales_total,
        "zero_sales_count": metrics.zero_sales_count,
        "parquet_uri": _hdfs_uri(hdfs_config.uri, chunk.parquet_path),
    }


def _write_dataset_manifest(
    hdfs: Any,
    candidate_path: str,
    hdfs_config: HDFSConfig,
    *,
    source_version: str,
    source_snapshot_id: str,
    pipeline_run_id: str,
    curated_path: str,
    metrics: NormalizationMetrics,
    chunks: Sequence[NormalizationChunk],
    created_at: str,
) -> None:
    document = {
        "schema_version": "sales-daily-snapshot-v1",
        "status": VALIDATED,
        "dataset_name": SALES_DAILY.name,
        "dataset_version": SALES_DAILY.version,
        "provenance": SALES_DAILY.provenance,
        "source_version": source_version,
        "source_snapshot_id": source_snapshot_id,
        "pipeline_run_id": pipeline_run_id,
        "artifact_uri": _hdfs_uri(hdfs_config.uri, curated_path),
        "schema": list(SALES_DAILY.field_names),
        "logical_unique_key": list(SALES_DAILY.unique_key),
        "row_count": metrics.row_count,
        "series_count": metrics.series_count,
        "day_count": metrics.day_count,
        "unit_sales_total": metrics.unit_sales_total,
        "zero_sales_count": metrics.zero_sales_count,
        "chunk_count": len(chunks),
        "persisted_parquet_validated": True,
        "created_at": created_at,
    }
    _write_json_once(hdfs, str(PurePosixPath(candidate_path) / "_snapshot_manifest.json"), document)


def _validate_dataset_manifest(
    hdfs: Any,
    dataset_path: str,
    hdfs_config: HDFSConfig,
    *,
    source_version: str,
    source_snapshot_id: str,
    expected_metrics: NormalizationMetrics,
    expected_row_count: int,
) -> None:
    path = str(PurePosixPath(dataset_path) / "_snapshot_manifest.json")
    manifest = _read_json(hdfs, path, "sales daily snapshot manifest")
    expected_schema = list(SALES_DAILY.field_names)
    if (
        manifest.get("schema_version") != "sales-daily-snapshot-v1"
        or manifest.get("status") != VALIDATED
        or manifest.get("dataset_name") != SALES_DAILY.name
        or manifest.get("dataset_version") != SALES_DAILY.version
        or manifest.get("provenance") != SALES_DAILY.provenance
        or manifest.get("source_version") != source_version
        or manifest.get("source_snapshot_id") != source_snapshot_id
        or manifest.get("artifact_uri") != _hdfs_uri(hdfs_config.uri, dataset_path)
        or manifest.get("schema") != expected_schema
        or manifest.get("logical_unique_key") != list(SALES_DAILY.unique_key)
        or manifest.get("row_count") != expected_row_count
        or manifest.get("series_count") != expected_metrics.series_count
        or manifest.get("day_count") != expected_metrics.day_count
        or manifest.get("unit_sales_total") != expected_metrics.unit_sales_total
        or manifest.get("zero_sales_count") != expected_metrics.zero_sales_count
        or manifest.get("persisted_parquet_validated") is not True
    ):
        raise NormalizationSnapshotError("curated SALES_DAILY manifest does not match validation")


def _quality_document(
    *,
    status: str,
    source_version: str,
    source_snapshot_id: str,
    pipeline_run_id: str,
    raw_path: str | None,
    curated_path: str,
    metrics: NormalizationMetrics | None,
    chunks: Sequence[NormalizationChunk],
    expected_series_count: int,
    expected_day_count: int,
    expected_row_count: int,
    failure_details: Sequence[str],
) -> dict[str, object]:
    return {
        "quality_schema_version": "sales-daily-normalization-quality-v1",
        "status": status,
        "source_version": source_version,
        "source_snapshot_id": source_snapshot_id,
        "pipeline_run_id": pipeline_run_id,
        "raw_path": raw_path,
        "curated_path": curated_path,
        "expected_series_count": expected_series_count,
        "expected_day_count": expected_day_count,
        "expected_row_count": expected_row_count,
        "measured": None
        if metrics is None
        else {
            "row_count": metrics.row_count,
            "series_count": metrics.series_count,
            "day_count": metrics.day_count,
            "unit_sales_total": metrics.unit_sales_total,
            "zero_sales_count": metrics.zero_sales_count,
        },
        "chunk_coverage": [
            {
                "index": chunk.index,
                "day_start": chunk.day_start,
                "day_end": chunk.day_end,
                "parquet_path": chunk.parquet_path,
            }
            for chunk in chunks
        ],
        "failure_details": list(failure_details[:_MAX_FAILURE_DETAILS]),
    }


def _build_pipeline_manifest(
    hdfs_config: HDFSConfig,
    *,
    source_version: str,
    source_snapshot_id: str,
    pipeline_run_id: str,
    curated_path: str,
    quality_path: str,
    metrics: NormalizationMetrics,
    chunks: Sequence[NormalizationChunk],
    created_at: str,
    completed_at: str,
    status: str,
) -> dict[str, object]:
    artifact_path = render_run_manifest_path(hdfs_config.base_path, pipeline_run_id)
    manifest = {
        "manifest_schema_version": PIPELINE_RUN_MANIFEST.version,
        "contract_name": PIPELINE_RUN_MANIFEST.name,
        "contract_version": PIPELINE_RUN_MANIFEST.version,
        "manifest_kind": PIPELINE_RUN_MANIFEST.manifest_kind,
        "status": status,
        "producer": "warehouse-bigdata:normalization-snapshot",
        "artifact_uri": _hdfs_uri(hdfs_config.uri, artifact_path),
        "artifact_checksum": None,
        "schema_fingerprint": _schema_fingerprint(),
        "grain": SALES_DAILY.grain,
        "logical_unique_key": ",".join(SALES_DAILY.unique_key),
        "row_count": metrics.row_count,
        "required_field_null_counts": json.dumps(
            {field: 0 for field in SALES_DAILY.field_names},
            sort_keys=True,
            separators=(",", ":"),
        ),
        "code_revision": "warehouse-bigdata:normalization-snapshot-v1",
        "configuration_digest": _configuration_digest(chunks),
        "created_at": created_at,
        "completed_at": completed_at,
        "coverage_and_exclusion_counts": json.dumps(
            {
                "series_count": metrics.series_count,
                "day_count": metrics.day_count,
                "row_count": metrics.row_count,
                "unit_sales_total": metrics.unit_sales_total,
                "zero_sales_count": metrics.zero_sales_count,
                "chunk_count": len(chunks),
                "persisted_parquet_validated": True,
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
        "pipeline_run_id": pipeline_run_id,
        "source_version": source_version,
        "output_dataset_names": json.dumps([SALES_DAILY.name], separators=(",", ":")),
        "output_dataset_uris": json.dumps(
            [_hdfs_uri(hdfs_config.uri, curated_path)],
            separators=(",", ":"),
        ),
        "quality_report_uri": _hdfs_uri(hdfs_config.uri, quality_path),
        "source_snapshot_id": source_snapshot_id,
    }
    try:
        validate_manifest(
            PIPELINE_RUN_MANIFEST,
            manifest,
            lineage_context={
                "required_status": status,
                "source_version": source_version,
                "pipeline_run_id": pipeline_run_id,
                "source_snapshot_id": source_snapshot_id,
            },
        )
    except ContractValidationError as error:
        raise NormalizationSnapshotError(f"generated invalid pipeline manifest: {error}") from error
    return manifest


def _publish_or_reuse_run_manifest(
    hdfs: Any,
    hdfs_config: HDFSConfig,
    manifest: Mapping[str, object],
    *,
    integrity,
) -> str:
    path = render_run_manifest_path(hdfs_config.base_path, str(manifest["pipeline_run_id"]))
    if _exists(hdfs, path):
        existing = _read_json(hdfs, path, "pipeline run manifest")
        try:
            validate_manifest(
                PIPELINE_RUN_MANIFEST,
                existing,
                lineage_context={
                    "required_status": VALIDATED,
                    "source_version": manifest["source_version"],
                    "pipeline_run_id": manifest["pipeline_run_id"],
                    "source_snapshot_id": manifest["source_snapshot_id"],
                },
            )
        except ContractValidationError as error:
            raise NormalizationSnapshotError(
                f"existing pipeline run manifest is not a matching VALIDATED attempt: {error}"
            ) from error
        identity_fields = (
            "status",
            "source_version",
            "source_snapshot_id",
            "pipeline_run_id",
            "row_count",
            "output_dataset_names",
            "output_dataset_uris",
            "quality_report_uri",
        )
        if any(existing.get(field) != manifest.get(field) for field in identity_fields):
            raise NormalizationSnapshotError("existing pipeline run manifest conflicts with retry")
        return path
    from warehouse_bigdata.ingestion.hdfs_raw import publish_run_manifest

    try:
        return publish_run_manifest(
            manifest,
            integrity=integrity,
            client=hdfs,
            config=hdfs_config,
        )
    except HdfsRawIngestionError as error:
        raise NormalizationSnapshotError(f"pipeline run manifest publication failed: {error}") from error


def _read_json(hdfs: Any, path: str, label: str) -> dict[str, object]:
    try:
        value = json.loads(hdfs.read_remote(path).decode("utf-8"))
    except (AttributeError, UnicodeError, json.JSONDecodeError, OSError, KeyError) as error:
        raise NormalizationSnapshotError(f"{label} is not valid JSON: {path}") from error
    if not isinstance(value, dict):
        raise NormalizationSnapshotError(f"{label} must be a JSON object: {path}")
    return value


def _validate_raw_inputs(raw_snapshot: ValidatedHdfsRawSnapshot) -> None:
    if raw_snapshot.status != VALIDATED:
        raise NormalizationSnapshotError("raw snapshot did not reach VALIDATED status")


def _exists(hdfs: Any, path: str) -> bool:
    try:
        return bool(hdfs.exists(path))
    except Exception as error:
        raise NormalizationSnapshotError(f"cannot inspect HDFS path {path}: {error}") from error


def _hdfs_uri(uri: str, path: str) -> str:
    parsed = urlsplit(uri)
    if parsed.scheme.lower() != "hdfs" or not parsed.netloc or not path.startswith("/"):
        raise NormalizationSnapshotError("normalization inputs and outputs must use configured HDFS")
    return uri.rstrip("/") + path


def _json_bytes(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")


def _schema_fingerprint() -> str:
    return "sha256:" + hashlib.sha256(
        json.dumps(SALES_DAILY.field_names, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _configuration_digest(chunks: Sequence[NormalizationChunk]) -> str:
    payload = json.dumps(
        [(chunk.day_start, chunk.day_end) for chunk in chunks],
        separators=(",", ":"),
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _failure_detail(error: BaseException) -> str:
    message = str(error).strip() or error.__class__.__name__
    return f"{error.__class__.__name__}: {message[:_MAX_FAILURE_LENGTH]}"


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
