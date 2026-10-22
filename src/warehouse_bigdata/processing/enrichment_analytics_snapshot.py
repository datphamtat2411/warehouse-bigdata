"""Immutable Spark enrichment and analytics artifacts for one sales snapshot."""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

from warehouse_bigdata.common.config import HDFSConfig
from warehouse_bigdata.common.hdfs_paths import (
    HdfsPathError,
    render_curated_sales_daily_path,
    render_curated_sales_enriched_path,
    render_enrichment_candidate_path,
    render_enrichment_failure_path,
    render_enrichment_output_quality_path,
    render_enrichment_quality_path,
    render_enrichment_staging_binding_path,
    render_raw_m5_path,
    render_run_manifest_path,
    render_staging_path,
    validate_pipeline_run_id,
    validate_source_snapshot_id,
    validate_source_version,
)
from warehouse_bigdata.common.hdfs_runtime import create_hdfs_cli_client
from warehouse_bigdata.contracts.datasets import (
    SALES_DAILY,
    SALES_ENRICHED,
    STORE_DAILY_SALES,
)
from warehouse_bigdata.contracts.manifests import FAILED, PIPELINE_RUN_MANIFEST, VALIDATED
from warehouse_bigdata.contracts.validation import ContractValidationError, validate_manifest
from warehouse_bigdata.ingestion.hdfs_raw import (
    HdfsRawIngestionError,
    ValidatedHdfsRawSnapshot,
    publish_run_manifest,
    validate_hdfs_raw_snapshot,
)
from warehouse_bigdata.processing.sales_aggregation import (
    SalesAggregationError,
    aggregate_sales,
    validate_category_monthly_sales,
    validate_store_daily_sales,
)
from warehouse_bigdata.processing.sales_enrichment import (
    SalesEnrichmentError,
    SalesEnrichmentMetrics,
    enrich_sales_daily,
    validate_sales_enriched,
)
from warehouse_bigdata.processing.sales_normalization import (
    NormalizationError,
    NormalizationMetrics,
    validate_sales_daily,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


_MAX_FAILURE_LENGTH = 1_000
_RAW_LOOKUP_FILES = frozenset({"calendar.csv", "sell_prices.csv"})
_OUTPUT_NAMES = ("sales_enriched", "store_daily_sales", "category_monthly_sales")
_CODE_REVISION = "warehouse-bigdata:enrichment-analytics-snapshot-v1"
_QUALITY_SCHEMA_VERSION = "enrichment-analytics-quality-v1"
_BINDING_SCHEMA_VERSION = "enrichment-analytics-input-binding-v1"
_ENRICHED_SNAPSHOT_SCHEMA_VERSION = "sales-enriched-snapshot-v1"
_SALES_SCHEMA_FINGERPRINT = "sha256:" + hashlib.sha256(
    json.dumps(SALES_DAILY.field_names, separators=(",", ":")).encode("utf-8")
).hexdigest()


class EnrichmentAnalyticsSnapshotError(RuntimeError):
    """Raised when one enrichment and analytics attempt cannot pass its quality gates."""


@dataclass(frozen=True, slots=True)
class EnrichmentAnalyticsMetrics:
    """Measured scalar quality values for one validated enrichment attempt."""

    row_count: int
    unit_sales_total: int
    zero_sales_count: int
    matched_price_count: int
    missing_price_count: int
    price_match_rate: float
    store_daily_row_count: int
    category_monthly_row_count: int


@dataclass(frozen=True, slots=True)
class EnrichmentAnalyticsSnapshotResult:
    """Immutable evidence and status for one J03 execution attempt."""

    status: str
    pipeline_run_id: str
    source_version: str
    source_snapshot_id: str
    raw_path: str | None
    sales_daily_path: str
    staging_path: str
    curated_path: str
    output_paths: Mapping[str, str]
    quality_report_path: str | None
    output_quality_paths: Mapping[str, str]
    failure_report_path: str | None
    run_manifest_path: str | None
    metrics: EnrichmentAnalyticsMetrics | None
    timings: Mapping[str, float]
    failure_details: tuple[str, ...]


def validate_enrichment_analytics_identifiers(
    source_version: str,
    source_snapshot_id: str,
    pipeline_run_id: str,
) -> None:
    """Require explicit, path-safe lineage identities before any HDFS work."""
    try:
        validate_source_version(source_version)
        validate_source_snapshot_id(source_snapshot_id)
        validate_pipeline_run_id(pipeline_run_id)
    except HdfsPathError as error:
        raise EnrichmentAnalyticsSnapshotError(str(error)) from error


def run_enrichment_analytics_snapshot(
    spark: SparkSession,
    *,
    source_version: str,
    source_snapshot_id: str,
    pipeline_run_id: str,
    hdfs_config: HDFSConfig,
    spark_master: str | None = None,
    client: Any | None = None,
    now: Callable[[], str] | None = None,
) -> EnrichmentAnalyticsSnapshotResult:
    """Enrich one validated SALES_DAILY snapshot and stage immutable analytics candidates."""
    validate_enrichment_analytics_identifiers(
        source_version,
        source_snapshot_id,
        pipeline_run_id,
    )
    if spark_master is not None:
        if not isinstance(spark_master, str) or not spark_master.startswith("spark://"):
            raise EnrichmentAnalyticsSnapshotError(
                "enrichment analytics requires an explicit Spark Standalone master"
            )
        if spark.sparkContext.master != spark_master:
            raise EnrichmentAnalyticsSnapshotError(
                f"Spark master mismatch: expected {spark_master!r}, "
                f"received {spark.sparkContext.master!r}"
            )

    staging_path = render_staging_path(hdfs_config.base_path, pipeline_run_id)
    sales_daily_path = render_curated_sales_daily_path(
        hdfs_config.base_path,
        source_snapshot_id,
    )
    curated_path = render_curated_sales_enriched_path(
        hdfs_config.base_path,
        source_snapshot_id,
    )
    output_paths = {
        name: render_enrichment_candidate_path(staging_path, name) for name in _OUTPUT_NAMES
    }
    output_quality_paths = {
        name: render_enrichment_output_quality_path(staging_path, name)
        for name in _OUTPUT_NAMES
    }
    quality_path = render_enrichment_quality_path(staging_path)
    failure_path = render_enrichment_failure_path(staging_path)
    timestamp = _utc_now if now is None else now
    started_at = time.perf_counter()
    timings: dict[str, float] = {}
    hdfs = client
    raw_path: str | None = None
    result_status = "VALIDATED"

    try:
        hdfs = create_hdfs_cli_client(hdfs_config) if hdfs is None else hdfs
        _ensure_staging_root(hdfs, staging_path)
        result_status = (
            "REUSED"
            if _exists(hdfs, render_run_manifest_path(hdfs_config.base_path, pipeline_run_id))
            else "VALIDATED"
        )
        if _exists(hdfs, failure_path):
            raise EnrichmentAnalyticsSnapshotError(
                "staging attempt already has FAILED evidence; use a new pipeline_run_id"
            )
        _write_or_validate_input_binding(
            hdfs,
            staging_path,
            source_version=source_version,
            source_snapshot_id=source_snapshot_id,
            pipeline_run_id=pipeline_run_id,
            sales_daily_path=sales_daily_path,
            raw_path=render_raw_m5_path(hdfs_config.base_path, source_version),
        )

        raw_snapshot = validate_hdfs_raw_snapshot(
            source_version,
            client=hdfs,
            config=hdfs_config,
        )
        _validate_raw_snapshot_binding(raw_snapshot, source_version, hdfs_config)
        raw_path = raw_snapshot.raw_path

        phase_started = time.perf_counter()
        sales_daily, input_manifest, input_metrics = _read_validated_sales_daily(
            spark,
            hdfs,
            hdfs_config,
            source_version=source_version,
            source_snapshot_id=source_snapshot_id,
        )
        calendar, sell_prices = _read_raw_lookup_sources(
            spark,
            hdfs_config,
            raw_snapshot,
            source_version=source_version,
        )
        timings["input_read_seconds"] = time.perf_counter() - phase_started

        phase_started = time.perf_counter()
        enriched = enrich_sales_daily(sales_daily, calendar, sell_prices)
        timings["enrichment_seconds"] = time.perf_counter() - phase_started

        phase_started = time.perf_counter()
        aggregation = aggregate_sales(
            sales_daily,
            source_snapshot_id=source_snapshot_id,
        )
        timings["aggregation_seconds"] = time.perf_counter() - phase_started

        phase_started = time.perf_counter()
        enriched_metrics = _materialize_sales_enriched(
            spark,
            hdfs,
            hdfs_config,
            enriched,
            candidate_path=output_paths["sales_enriched"],
            curated_path=curated_path,
            source_version=source_version,
            source_snapshot_id=source_snapshot_id,
            pipeline_run_id=pipeline_run_id,
            expected_row_count=input_metrics.row_count,
            expected_unit_sales_total=input_metrics.unit_sales_total,
            created_at=timestamp(),
            promote=False,
        )
        timings["sales_enriched_materialization_seconds"] = time.perf_counter() - phase_started

        phase_started = time.perf_counter()
        store_daily = _materialize_aggregate_candidate(
            spark,
            hdfs,
            hdfs_config,
            aggregation.store_daily_sales,
            output_paths["store_daily_sales"],
            lambda persisted: validate_store_daily_sales(
                persisted,
                sales_daily,
                source_snapshot_id=source_snapshot_id,
            ),
        )
        timings["store_aggregation_seconds"] = time.perf_counter() - phase_started

        phase_started = time.perf_counter()
        category_monthly = _materialize_aggregate_candidate(
            spark,
            hdfs,
            hdfs_config,
            aggregation.category_monthly_sales,
            output_paths["category_monthly_sales"],
            lambda persisted: validate_category_monthly_sales(
                persisted,
                sales_daily,
                source_snapshot_id=source_snapshot_id,
            ),
        )
        timings["category_aggregation_seconds"] = time.perf_counter() - phase_started

        phase_started = time.perf_counter()
        enriched_metrics = _materialize_sales_enriched(
            spark,
            hdfs,
            hdfs_config,
            enriched,
            candidate_path=output_paths["sales_enriched"],
            curated_path=curated_path,
            source_version=source_version,
            source_snapshot_id=source_snapshot_id,
            pipeline_run_id=pipeline_run_id,
            expected_row_count=input_metrics.row_count,
            expected_unit_sales_total=input_metrics.unit_sales_total,
            created_at=timestamp(),
            promote=True,
        )
        timings["sales_enriched_promotion_seconds"] = time.perf_counter() - phase_started

        persisted_enriched = _read_parquet(
            spark,
            _hdfs_uri(hdfs_config.uri, curated_path),
        ).select(*SALES_ENRICHED.field_names)
        enriched_quality = _basic_quality_values(
            persisted_enriched,
            total_field="unit_sales",
        )
        enriched_quality["price_metrics"] = {
            "matched_price_count": enriched_metrics.matched_price_count,
            "missing_price_count": enriched_metrics.missing_price_count,
            "price_match_rate": enriched_metrics.price_match_rate,
        }

        output_documents = {
            "sales_enriched": _output_quality_document(
                enriched_quality,
                hdfs_config=hdfs_config,
                artifact_path=curated_path,
                contract=SALES_ENRICHED,
                candidate_only=False,
                quality_path=output_quality_paths["sales_enriched"],
            ),
            "store_daily_sales": _output_quality_document(
                store_daily,
                hdfs_config=hdfs_config,
                artifact_path=output_paths["store_daily_sales"],
                contract=STORE_DAILY_SALES,
                candidate_only=True,
                quality_path=output_quality_paths["store_daily_sales"],
            ),
            "category_monthly_sales": _output_quality_document(
                category_monthly,
                hdfs_config=hdfs_config,
                artifact_path=output_paths["category_monthly_sales"],
                contract=None,
                dataset_name="category_monthly_sales",
                candidate_only=True,
                quality_path=output_quality_paths["category_monthly_sales"],
            ),
        }
        for document in output_documents.values():
            document.update(
                {
                    "source_version": source_version,
                    "source_snapshot_id": source_snapshot_id,
                    "pipeline_run_id": pipeline_run_id,
                }
            )
        for name, document in output_documents.items():
            _write_json_once(hdfs, output_quality_paths[name], document)

        summary = _quality_summary(
            source_version=source_version,
            source_snapshot_id=source_snapshot_id,
            pipeline_run_id=pipeline_run_id,
            raw_path=raw_path,
            sales_daily_path=sales_daily_path,
            curated_path=curated_path,
            output_paths=output_paths,
            output_quality_paths=output_quality_paths,
            input_manifest=input_manifest,
            input_metrics=input_metrics,
            output_documents=output_documents,
        )
        _write_json_once(hdfs, quality_path, summary)

        metrics = EnrichmentAnalyticsMetrics(
            row_count=enriched_metrics.row_count,
            unit_sales_total=enriched_metrics.unit_sales_total,
            zero_sales_count=enriched_metrics.zero_sales_count,
            matched_price_count=enriched_metrics.matched_price_count,
            missing_price_count=enriched_metrics.missing_price_count,
            price_match_rate=enriched_metrics.price_match_rate,
            store_daily_row_count=int(store_daily["row_count"]),
            category_monthly_row_count=int(category_monthly["row_count"]),
        )
        manifest = _build_pipeline_manifest(
            hdfs_config,
            source_version=source_version,
            source_snapshot_id=source_snapshot_id,
            pipeline_run_id=pipeline_run_id,
            curated_path=curated_path,
            output_paths=output_paths,
            quality_path=quality_path,
            output_documents=output_documents,
            input_metrics=input_metrics,
            enriched_metrics=enriched_metrics,
            metrics=metrics,
            created_at=timestamp(),
            completed_at=timestamp(),
            spark_master=spark_master or spark.sparkContext.master,
        )
        manifest_path = _publish_or_reuse_run_manifest(
            hdfs,
            hdfs_config,
            manifest,
            integrity=raw_snapshot.integrity,
        )
        timings["total_seconds"] = time.perf_counter() - started_at
        return EnrichmentAnalyticsSnapshotResult(
            status=result_status,
            pipeline_run_id=pipeline_run_id,
            source_version=source_version,
            source_snapshot_id=source_snapshot_id,
            raw_path=raw_path,
            sales_daily_path=sales_daily_path,
            staging_path=staging_path,
            curated_path=curated_path,
            output_paths={**output_paths, "sales_enriched": curated_path},
            quality_report_path=quality_path,
            output_quality_paths=output_quality_paths,
            failure_report_path=None,
            run_manifest_path=manifest_path,
            metrics=metrics,
            timings=dict(timings),
            failure_details=(),
        )
    except Exception as error:
        timings["total_seconds"] = time.perf_counter() - started_at
        detail = _failure_detail(error)
        stored_failure_path: str | None = None
        if hdfs is not None:
            try:
                _ensure_staging_root(hdfs, staging_path)
                _write_json_once(
                    hdfs,
                    failure_path,
                    _failure_document(
                        source_version=source_version,
                        source_snapshot_id=source_snapshot_id,
                        pipeline_run_id=pipeline_run_id,
                        raw_path=raw_path,
                        sales_daily_path=sales_daily_path,
                        staging_path=staging_path,
                        curated_path=curated_path,
                        output_paths=output_paths,
                        failure_details=(detail,),
                    ),
                )
                stored_failure_path = failure_path
            except Exception as evidence_error:
                detail = f"{detail}; failure evidence unavailable: {_failure_detail(evidence_error)}"
                if _safe_exists(hdfs, failure_path):
                    stored_failure_path = failure_path
        return EnrichmentAnalyticsSnapshotResult(
            status=FAILED,
            pipeline_run_id=pipeline_run_id,
            source_version=source_version,
            source_snapshot_id=source_snapshot_id,
            raw_path=raw_path,
            sales_daily_path=sales_daily_path,
            staging_path=staging_path,
            curated_path=curated_path,
            output_paths=output_paths,
            quality_report_path=None,
            output_quality_paths={},
            failure_report_path=stored_failure_path,
            run_manifest_path=None,
            metrics=None,
            timings=dict(timings),
            failure_details=(detail,),
        )


def _read_validated_sales_daily(
    spark: SparkSession,
    hdfs: Any,
    hdfs_config: HDFSConfig,
    *,
    source_version: str,
    source_snapshot_id: str,
) -> tuple[DataFrame, dict[str, object], NormalizationMetrics]:
    from warehouse_bigdata.common.hdfs_paths import validate_pipeline_run_id

    sales_daily_path = render_curated_sales_daily_path(
        hdfs_config.base_path,
        source_snapshot_id,
    )
    if not _exists(hdfs, sales_daily_path) or not hdfs.is_directory(sales_daily_path):
        raise EnrichmentAnalyticsSnapshotError(
            f"validated SALES_DAILY path is not a directory: {sales_daily_path}"
        )
    snapshot_manifest_path = str(PurePosixPath(sales_daily_path) / "_snapshot_manifest.json")
    snapshot_manifest = _read_json(hdfs, snapshot_manifest_path, "SALES_DAILY snapshot manifest")
    _validate_sales_daily_manifest(
        snapshot_manifest,
        hdfs_config,
        source_version=source_version,
        source_snapshot_id=source_snapshot_id,
        sales_daily_path=sales_daily_path,
    )

    upstream_run_id = snapshot_manifest.get("pipeline_run_id")
    try:
        validate_pipeline_run_id(upstream_run_id)
    except (HdfsPathError, TypeError) as error:
        raise EnrichmentAnalyticsSnapshotError(
            "SALES_DAILY snapshot manifest has an unsafe pipeline_run_id"
        ) from error
    upstream_manifest_path = render_run_manifest_path(hdfs_config.base_path, upstream_run_id)
    upstream_manifest = _read_json(hdfs, upstream_manifest_path, "SALES_DAILY pipeline manifest")
    try:
        validate_manifest(
            PIPELINE_RUN_MANIFEST,
            upstream_manifest,
            lineage_context={
                "required_status": VALIDATED,
                "source_version": source_version,
                "pipeline_run_id": upstream_run_id,
                "source_snapshot_id": source_snapshot_id,
            },
        )
    except ContractValidationError as error:
        raise EnrichmentAnalyticsSnapshotError(
            f"SALES_DAILY pipeline manifest is not VALIDATED: {error}"
        ) from error

    if upstream_manifest.get("artifact_uri") != _hdfs_uri(hdfs_config.uri, upstream_manifest_path):
        raise EnrichmentAnalyticsSnapshotError("SALES_DAILY pipeline manifest URI is inconsistent")
    if upstream_manifest.get("schema_fingerprint") != _SALES_SCHEMA_FINGERPRINT:
        raise EnrichmentAnalyticsSnapshotError("SALES_DAILY schema fingerprint is inconsistent")
    if upstream_manifest.get("row_count") != snapshot_manifest.get("row_count"):
        raise EnrichmentAnalyticsSnapshotError("SALES_DAILY manifest row counts disagree")
    names = _json_string_list(upstream_manifest.get("output_dataset_names"), "output dataset names")
    uris = _json_string_list(upstream_manifest.get("output_dataset_uris"), "output dataset URIs")
    expected_uri = _hdfs_uri(hdfs_config.uri, sales_daily_path)
    if "sales_daily" not in names or expected_uri not in uris:
        raise EnrichmentAnalyticsSnapshotError(
            "SALES_DAILY pipeline manifest does not identify the requested snapshot"
        )

    data_uri = _hdfs_uri(hdfs_config.uri, sales_daily_path)
    try:
        sales_daily = _read_parquet(spark, data_uri)
        expected_metrics = NormalizationMetrics(
            row_count=_manifest_int(snapshot_manifest, "row_count"),
            series_count=_manifest_int(snapshot_manifest, "series_count"),
            day_count=_manifest_int(snapshot_manifest, "day_count"),
            unit_sales_total=_manifest_int(snapshot_manifest, "unit_sales_total"),
            zero_sales_count=_manifest_int(snapshot_manifest, "zero_sales_count"),
        )
        metrics = validate_sales_daily(
            sales_daily,
            expected_series_count=expected_metrics.series_count,
            selected_day_keys=tuple(
                f"d_{day}" for day in range(1, expected_metrics.day_count + 1)
            ),
            expected_unit_sales_total=expected_metrics.unit_sales_total,
            expected_zero_sales_count=expected_metrics.zero_sales_count,
        )
    except (NormalizationError, EnrichmentAnalyticsSnapshotError, ValueError, TypeError) as error:
        raise EnrichmentAnalyticsSnapshotError(
            f"persisted SALES_DAILY failed validation: {error}"
        ) from error
    return sales_daily, snapshot_manifest, metrics


def _read_raw_lookup_sources(
    spark: SparkSession,
    hdfs_config: HDFSConfig,
    raw_snapshot: ValidatedHdfsRawSnapshot,
    *,
    source_version: str,
) -> tuple[DataFrame, DataFrame]:
    raw_uri = _hdfs_uri(hdfs_config.uri, raw_snapshot.raw_path)
    reader = spark.read.option("header", True).option("inferSchema", False)
    try:
        return (
            reader.csv(f"{raw_uri}/calendar.csv"),
            reader.csv(f"{raw_uri}/sell_prices.csv"),
        )
    except Exception as error:
        raise EnrichmentAnalyticsSnapshotError(
            f"matching raw calendar and price sources could not be read: {error}"
        ) from error


def _validate_raw_snapshot_binding(
    raw_snapshot: ValidatedHdfsRawSnapshot,
    source_version: str,
    hdfs_config: HDFSConfig,
) -> None:
    if raw_snapshot.status != VALIDATED:
        raise EnrichmentAnalyticsSnapshotError("raw HDFS snapshot is not VALIDATED")
    if raw_snapshot.source_version != source_version:
        raise EnrichmentAnalyticsSnapshotError("raw HDFS source_version does not match the run")
    expected_path = render_raw_m5_path(hdfs_config.base_path, source_version)
    if raw_snapshot.raw_path != expected_path:
        raise EnrichmentAnalyticsSnapshotError("raw HDFS path does not match source_version")
    manifests = {}
    for manifest in raw_snapshot.manifests:
        filename = manifest.get("source_file")
        if not isinstance(filename, str) or filename in manifests:
            raise EnrichmentAnalyticsSnapshotError("raw HDFS source manifests are duplicated")
        if manifest.get("status") != VALIDATED:
            raise EnrichmentAnalyticsSnapshotError("raw HDFS source manifest is not VALIDATED")
        if manifest.get("source_version") != source_version:
            raise EnrichmentAnalyticsSnapshotError("raw HDFS source manifest is cross-snapshot")
        manifests[filename] = manifest
    if not _RAW_LOOKUP_FILES <= set(manifests):
        raise EnrichmentAnalyticsSnapshotError(
            "validated raw HDFS snapshot lacks calendar.csv or sell_prices.csv"
        )


def _validate_sales_daily_manifest(
    manifest: Mapping[str, object],
    hdfs_config: HDFSConfig,
    *,
    source_version: str,
    source_snapshot_id: str,
    sales_daily_path: str,
) -> None:
    expected_schema = list(SALES_DAILY.field_names)
    if (
        manifest.get("schema_version") != "sales-daily-snapshot-v1"
        or manifest.get("status") != VALIDATED
        or manifest.get("dataset_name") != SALES_DAILY.name
        or manifest.get("dataset_version") != SALES_DAILY.version
        or manifest.get("provenance") != SALES_DAILY.provenance
        or manifest.get("source_version") != source_version
        or manifest.get("source_snapshot_id") != source_snapshot_id
        or manifest.get("artifact_uri") != _hdfs_uri(hdfs_config.uri, sales_daily_path)
        or manifest.get("schema") != expected_schema
        or manifest.get("logical_unique_key") != list(SALES_DAILY.unique_key)
        or manifest.get("persisted_parquet_validated") is not True
    ):
        raise EnrichmentAnalyticsSnapshotError(
            "SALES_DAILY snapshot manifest does not match Contract v1 or requested lineage"
        )
    fingerprint = manifest.get("schema_fingerprint")
    if fingerprint is not None and fingerprint != _SALES_SCHEMA_FINGERPRINT:
        raise EnrichmentAnalyticsSnapshotError("SALES_DAILY snapshot schema fingerprint mismatch")
    for field in (
        "row_count",
        "series_count",
        "day_count",
        "unit_sales_total",
        "zero_sales_count",
    ):
        _manifest_int(manifest, field)


def _materialize_sales_enriched(
    spark: SparkSession,
    hdfs: Any,
    hdfs_config: HDFSConfig,
    enriched: DataFrame,
    *,
    candidate_path: str,
    curated_path: str,
    source_version: str,
    source_snapshot_id: str,
    pipeline_run_id: str,
    expected_row_count: int,
    expected_unit_sales_total: int,
    created_at: str,
    promote: bool,
) -> SalesEnrichmentMetrics:
    curated_uri = _hdfs_uri(hdfs_config.uri, curated_path)
    if _exists(hdfs, curated_path):
        if not hdfs.is_directory(curated_path):
            raise EnrichmentAnalyticsSnapshotError("existing SALES_ENRICHED path is not a directory")
        metrics = _validate_persisted_enriched(
            spark,
            curated_uri,
            expected_row_count=expected_row_count,
            expected_unit_sales_total=expected_unit_sales_total,
        )
        _validate_enriched_manifest(
            hdfs,
            curated_path,
            hdfs_config,
            source_version=source_version,
            source_snapshot_id=source_snapshot_id,
            expected_metrics=metrics,
            artifact_path=curated_path,
        )
        return metrics

    candidate_uri = _hdfs_uri(hdfs_config.uri, candidate_path)
    if _exists(hdfs, candidate_path):
        if not hdfs.is_directory(candidate_path):
            raise EnrichmentAnalyticsSnapshotError("existing SALES_ENRICHED candidate is not a directory")
    else:
        _write_parquet_once(spark, enriched, candidate_uri, partition_by=("year",))

    metrics = _validate_persisted_enriched(
        spark,
        candidate_uri,
        expected_row_count=expected_row_count,
        expected_unit_sales_total=expected_unit_sales_total,
    )
    manifest_path = str(PurePosixPath(candidate_path) / "_snapshot_manifest.json")
    if _exists(hdfs, manifest_path):
        _validate_enriched_manifest(
            hdfs,
            candidate_path,
            hdfs_config,
            source_version=source_version,
            source_snapshot_id=source_snapshot_id,
            expected_metrics=metrics,
            artifact_path=curated_path,
        )
    else:
        _write_json_once(
            hdfs,
            manifest_path,
            _enriched_manifest(
                hdfs_config,
                source_version=source_version,
                source_snapshot_id=source_snapshot_id,
                pipeline_run_id=pipeline_run_id,
                artifact_path=curated_path,
                metrics=metrics,
                created_at=created_at,
            ),
        )

    if not promote:
        return metrics

    hdfs.mkdir_parents(str(PurePosixPath(curated_path).parent))
    try:
        hdfs.rename(candidate_path, curated_path)
    except Exception as error:
        if not _exists(hdfs, curated_path):
            raise EnrichmentAnalyticsSnapshotError(
                f"SALES_ENRICHED promotion failed: {error}"
            ) from error
        if not hdfs.is_directory(curated_path):
            raise EnrichmentAnalyticsSnapshotError(
                "SALES_ENRICHED destination appeared as a non-directory"
            ) from error

    promoted_metrics = _validate_persisted_enriched(
        spark,
        curated_uri,
        expected_row_count=expected_row_count,
        expected_unit_sales_total=expected_unit_sales_total,
    )
    _validate_enriched_manifest(
        hdfs,
        curated_path,
        hdfs_config,
        source_version=source_version,
        source_snapshot_id=source_snapshot_id,
        expected_metrics=promoted_metrics,
        artifact_path=curated_path,
    )
    return promoted_metrics


def _materialize_aggregate_candidate(
    spark: SparkSession,
    hdfs: Any,
    hdfs_config: HDFSConfig,
    dataframe: DataFrame,
    candidate_path: str,
    validator: Callable[[DataFrame], None],
) -> dict[str, object]:
    candidate_uri = _hdfs_uri(hdfs_config.uri, candidate_path)
    if _exists(hdfs, candidate_path) and not hdfs.is_directory(candidate_path):
        raise EnrichmentAnalyticsSnapshotError(
            f"existing aggregate candidate is not a directory: {candidate_path}"
        )
    if not _exists(hdfs, candidate_path):
        _write_parquet_once(spark, dataframe, candidate_uri)
    persisted = _read_parquet(spark, candidate_uri)
    try:
        validator(persisted)
    except (SalesAggregationError, ValueError, TypeError) as error:
        raise EnrichmentAnalyticsSnapshotError(
            f"persisted aggregate candidate failed validation: {error}"
        ) from error
    return _basic_quality_values(persisted, total_field="total_units")


def _validate_persisted_enriched(
    spark: SparkSession,
    uri: str,
    *,
    expected_row_count: int,
    expected_unit_sales_total: int,
) -> SalesEnrichmentMetrics:
    persisted = _read_parquet(spark, uri)
    if set(persisted.columns) != set(SALES_ENRICHED.field_names):
        raise EnrichmentAnalyticsSnapshotError("persisted SALES_ENRICHED schema fields differ")
    persisted = persisted.select(*SALES_ENRICHED.field_names)
    try:
        return validate_sales_enriched(
            persisted,
            expected_row_count,
            expected_unit_sales_total,
        )
    except (SalesEnrichmentError, ValueError, TypeError) as error:
        raise EnrichmentAnalyticsSnapshotError(
            f"persisted SALES_ENRICHED failed validation: {error}"
        ) from error


def _validate_enriched_manifest(
    hdfs: Any,
    dataset_path: str,
    hdfs_config: HDFSConfig,
    *,
    source_version: str,
    source_snapshot_id: str,
    expected_metrics: SalesEnrichmentMetrics,
    artifact_path: str,
) -> None:
    path = str(PurePosixPath(dataset_path) / "_snapshot_manifest.json")
    manifest = _read_json(hdfs, path, "SALES_ENRICHED snapshot manifest")
    if (
        manifest.get("schema_version") != _ENRICHED_SNAPSHOT_SCHEMA_VERSION
        or manifest.get("status") != VALIDATED
        or manifest.get("dataset_name") != SALES_ENRICHED.name
        or manifest.get("dataset_version") != SALES_ENRICHED.version
        or manifest.get("provenance") != SALES_ENRICHED.provenance
        or manifest.get("source_version") != source_version
        or manifest.get("source_snapshot_id") != source_snapshot_id
        or manifest.get("artifact_uri") != _hdfs_uri(hdfs_config.uri, artifact_path)
        or manifest.get("schema") != list(SALES_ENRICHED.field_names)
        or manifest.get("logical_unique_key") != list(SALES_ENRICHED.unique_key)
        or manifest.get("row_count") != expected_metrics.row_count
        or manifest.get("unit_sales_total") != expected_metrics.unit_sales_total
        or manifest.get("zero_sales_count") != expected_metrics.zero_sales_count
        or manifest.get("matched_price_count") != expected_metrics.matched_price_count
        or manifest.get("missing_price_count") != expected_metrics.missing_price_count
        or manifest.get("persisted_parquet_validated") is not True
    ):
        raise EnrichmentAnalyticsSnapshotError(
            "SALES_ENRICHED snapshot manifest does not match persisted validation"
        )
    if manifest.get("schema_fingerprint") != _schema_fingerprint(SALES_ENRICHED.field_names):
        raise EnrichmentAnalyticsSnapshotError("SALES_ENRICHED schema fingerprint mismatch")


def _enriched_manifest(
    hdfs_config: HDFSConfig,
    *,
    source_version: str,
    source_snapshot_id: str,
    pipeline_run_id: str,
    artifact_path: str,
    metrics: SalesEnrichmentMetrics,
    created_at: str,
) -> dict[str, object]:
    return {
        "schema_version": _ENRICHED_SNAPSHOT_SCHEMA_VERSION,
        "status": VALIDATED,
        "dataset_name": SALES_ENRICHED.name,
        "dataset_version": SALES_ENRICHED.version,
        "provenance": SALES_ENRICHED.provenance,
        "source_version": source_version,
        "source_snapshot_id": source_snapshot_id,
        "pipeline_run_id": pipeline_run_id,
        "artifact_uri": _hdfs_uri(hdfs_config.uri, artifact_path),
        "schema": list(SALES_ENRICHED.field_names),
        "schema_fingerprint": _schema_fingerprint(SALES_ENRICHED.field_names),
        "logical_unique_key": list(SALES_ENRICHED.unique_key),
        "row_count": metrics.row_count,
        "unit_sales_total": metrics.unit_sales_total,
        "zero_sales_count": metrics.zero_sales_count,
        "matched_price_count": metrics.matched_price_count,
        "missing_price_count": metrics.missing_price_count,
        "price_match_rate": metrics.price_match_rate,
        "persisted_parquet_validated": True,
        "created_at": created_at,
    }


def _output_quality_document(
    dataframe_or_metrics: DataFrame | SalesEnrichmentMetrics | Mapping[str, object],
    *,
    hdfs_config: HDFSConfig,
    artifact_path: str,
    contract,
    candidate_only: bool,
    quality_path: str,
    price_metrics: SalesEnrichmentMetrics | None = None,
    dataset_name: str | None = None,
) -> dict[str, object]:
    if isinstance(dataframe_or_metrics, SalesEnrichmentMetrics):
        values = {
            "row_count": dataframe_or_metrics.row_count,
            "unit_sales_total": dataframe_or_metrics.unit_sales_total,
            "zero_sales_count": dataframe_or_metrics.zero_sales_count,
            "matched_price_count": dataframe_or_metrics.matched_price_count,
            "missing_price_count": dataframe_or_metrics.missing_price_count,
            "price_match_rate": dataframe_or_metrics.price_match_rate,
            "null_counts": {field: 0 for field in SALES_ENRICHED.field_names},
            "coverage": {},
        }
        fields = SALES_ENRICHED.field_names
    else:
        values = dict(dataframe_or_metrics)
        fields = tuple(values.pop("fields", ()))
    required_fields = () if contract is None else contract.required_field_names
    required_nulls = {
        field: int(values.get("null_counts", {}).get(field, 0)) for field in required_fields
    }
    document = {
        "quality_schema_version": _QUALITY_SCHEMA_VERSION,
        "status": VALIDATED,
        "dataset_name": (
            dataset_name if dataset_name is not None else (None if contract is None else contract.name)
        ),
        "contract_name": None if contract is None else contract.name,
        "contract_version": None if contract is None else contract.version,
        "contract_approval": "PENDING_HUMAN_APPROVAL" if contract is None else "APPROVED",
        "candidate_only": candidate_only,
        "artifact_uri": _hdfs_uri(hdfs_config.uri, artifact_path),
        "quality_report_uri": _hdfs_uri(hdfs_config.uri, quality_path),
        "schema": list(fields),
        "schema_fingerprint": _schema_fingerprint(fields),
        "row_count": int(values["row_count"]),
        "unit_sales_total": int(values["unit_sales_total"]),
        "zero_sales_count": int(values.get("zero_sales_count", 0)),
        "null_counts": values.get("null_counts", {}),
        "required_field_null_counts": required_nulls,
        "coverage": values.get("coverage", {}),
        "persisted_parquet_validated": True,
    }
    if price_metrics is not None:
        document["price_metrics"] = {
            "matched_price_count": price_metrics.matched_price_count,
            "missing_price_count": price_metrics.missing_price_count,
            "price_match_rate": price_metrics.price_match_rate,
        }
    elif "price_metrics" in values:
        document["price_metrics"] = values["price_metrics"]
    return document


def _basic_quality_values(dataframe: DataFrame, *, total_field: str) -> dict[str, object]:
    from pyspark.sql import functions as F
    from pyspark.sql.types import DecimalType, LongType

    fields = tuple(dataframe.columns)
    null_row = dataframe.agg(
        *[
            F.sum(F.when(F.col(field).isNull(), F.lit(1)).otherwise(F.lit(0)))
            .cast(LongType())
            .alias(field)
            for field in fields
        ],
        F.count(F.lit(1)).alias("row_count"),
        F.sum(F.col(total_field).cast(DecimalType(38, 0))).alias("unit_sales_total"),
    ).first()
    if null_row is None or null_row["unit_sales_total"] is None:
        raise EnrichmentAnalyticsSnapshotError("cannot measure an empty aggregate candidate")
    zero_field = "is_zero_sale" if "is_zero_sale" in fields else total_field
    zero_count = dataframe.where(F.col(zero_field) == F.lit(0) if zero_field == total_field else F.col(zero_field)).count()
    null_counts = {field: int(null_row[field] or 0) for field in fields}
    coverage: dict[str, object] = {}
    for field in ("series_id", "store_id", "cat_id", "date", "month_start"):
        if field in fields:
            coverage[f"distinct_{field}_count"] = dataframe.select(field).distinct().count()
    if "date" in fields:
        bounds = dataframe.agg(F.min("date").alias("min_date"), F.max("date").alias("max_date")).first()
        coverage["min_date"] = _date_text(bounds["min_date"])
        coverage["max_date"] = _date_text(bounds["max_date"])
    if "month_start" in fields:
        bounds = dataframe.agg(
            F.min("month_start").alias("min_month_start"),
            F.max("month_start").alias("max_month_start"),
        ).first()
        coverage["min_month_start"] = _date_text(bounds["min_month_start"])
        coverage["max_month_start"] = _date_text(bounds["max_month_start"])
    return {
        "fields": fields,
        "row_count": int(null_row["row_count"]),
        "unit_sales_total": int(null_row["unit_sales_total"]),
        "zero_sales_count": int(zero_count),
        "null_counts": null_counts,
        "coverage": coverage,
    }


def _quality_summary(
    *,
    source_version: str,
    source_snapshot_id: str,
    pipeline_run_id: str,
    raw_path: str,
    sales_daily_path: str,
    curated_path: str,
    output_paths: Mapping[str, str],
    output_quality_paths: Mapping[str, str],
    input_manifest: Mapping[str, object],
    input_metrics: NormalizationMetrics,
    output_documents: Mapping[str, Mapping[str, object]],
) -> dict[str, object]:
    return {
        "quality_schema_version": _QUALITY_SCHEMA_VERSION,
        "status": VALIDATED,
        "source_version": source_version,
        "source_snapshot_id": source_snapshot_id,
        "pipeline_run_id": pipeline_run_id,
        "raw_path": raw_path,
        "sales_daily_path": sales_daily_path,
        "curated_sales_enriched_path": curated_path,
        "input": {
            "row_count": input_metrics.row_count,
            "series_count": input_metrics.series_count,
            "day_count": input_metrics.day_count,
            "unit_sales_total": input_metrics.unit_sales_total,
            "zero_sales_count": input_metrics.zero_sales_count,
            "schema_fingerprint": _SALES_SCHEMA_FINGERPRINT,
            "manifest_pipeline_run_id": input_manifest.get("pipeline_run_id"),
        },
        "outputs": {name: dict(document) for name, document in output_documents.items()},
        "output_candidate_paths": dict(output_paths),
        "output_quality_report_paths": dict(output_quality_paths),
        "candidate_only_outputs": ["store_daily_sales", "category_monthly_sales"],
        "category_monthly_contract": {
            "status": "PENDING_HUMAN_APPROVAL",
            "contract_name": None,
            "contract_version": None,
            "physical_path": None,
        },
        "failure_details": [],
    }


def _build_pipeline_manifest(
    hdfs_config: HDFSConfig,
    *,
    source_version: str,
    source_snapshot_id: str,
    pipeline_run_id: str,
    curated_path: str,
    output_paths: Mapping[str, str],
    quality_path: str,
    output_documents: Mapping[str, Mapping[str, object]],
    input_metrics: NormalizationMetrics,
    enriched_metrics: SalesEnrichmentMetrics,
    metrics: EnrichmentAnalyticsMetrics,
    created_at: str,
    completed_at: str,
    spark_master: str,
) -> dict[str, object]:
    output_uris = [
        _hdfs_uri(hdfs_config.uri, curated_path),
        _hdfs_uri(hdfs_config.uri, output_paths["store_daily_sales"]),
        _hdfs_uri(hdfs_config.uri, output_paths["category_monthly_sales"]),
    ]
    schema_fingerprint = _schema_fingerprint_map(
        {
            name: tuple(document["schema"])
            for name, document in output_documents.items()
        }
    )
    manifest = {
        "manifest_schema_version": PIPELINE_RUN_MANIFEST.version,
        "contract_name": PIPELINE_RUN_MANIFEST.name,
        "contract_version": PIPELINE_RUN_MANIFEST.version,
        "manifest_kind": PIPELINE_RUN_MANIFEST.manifest_kind,
        "status": VALIDATED,
        "producer": _CODE_REVISION,
        "artifact_uri": _hdfs_uri(
            hdfs_config.uri,
            render_run_manifest_path(hdfs_config.base_path, pipeline_run_id),
        ),
        "artifact_checksum": None,
        "schema_fingerprint": schema_fingerprint,
        "grain": SALES_ENRICHED.grain,
        "logical_unique_key": ",".join(SALES_ENRICHED.unique_key),
        "row_count": metrics.row_count,
        "required_field_null_counts": json.dumps(
            output_documents["sales_enriched"]["required_field_null_counts"],
            sort_keys=True,
            separators=(",", ":"),
        ),
        "code_revision": _CODE_REVISION,
        "configuration_digest": _configuration_digest(
            hdfs_config,
            spark_master=spark_master,
        ),
        "created_at": created_at,
        "completed_at": completed_at,
        "coverage_and_exclusion_counts": json.dumps(
            {
                "input_row_count": input_metrics.row_count,
                "input_unit_sales_total": input_metrics.unit_sales_total,
                "input_zero_sales_count": input_metrics.zero_sales_count,
                "row_count": metrics.row_count,
                "unit_sales_total": metrics.unit_sales_total,
                "zero_sales_count": metrics.zero_sales_count,
                "matched_price_count": enriched_metrics.matched_price_count,
                "missing_price_count": enriched_metrics.missing_price_count,
                "price_match_rate": enriched_metrics.price_match_rate,
                "store_daily_row_count": metrics.store_daily_row_count,
                "category_monthly_row_count": metrics.category_monthly_row_count,
                "schema_fingerprint": schema_fingerprint,
                "persisted_parquet_validated": True,
                "candidate_only_outputs": ["store_daily_sales", "category_monthly_sales"],
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
        "pipeline_run_id": pipeline_run_id,
        "source_version": source_version,
        "output_dataset_names": json.dumps(list(_OUTPUT_NAMES), separators=(",", ":")),
        "output_dataset_uris": json.dumps(output_uris, separators=(",", ":")),
        "quality_report_uri": _hdfs_uri(hdfs_config.uri, quality_path),
        "source_snapshot_id": source_snapshot_id,
    }
    try:
        validate_manifest(
            PIPELINE_RUN_MANIFEST,
            manifest,
            lineage_context={
                "required_status": VALIDATED,
                "source_version": source_version,
                "pipeline_run_id": pipeline_run_id,
                "source_snapshot_id": source_snapshot_id,
            },
        )
    except ContractValidationError as error:
        raise EnrichmentAnalyticsSnapshotError(
            f"generated invalid enrichment pipeline manifest: {error}"
        ) from error
    return manifest


def _publish_or_reuse_run_manifest(
    hdfs: Any,
    hdfs_config: HDFSConfig,
    manifest: Mapping[str, object],
    *,
    integrity: Any,
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
            raise EnrichmentAnalyticsSnapshotError(
                f"existing pipeline run manifest is not a matching VALIDATED attempt: {error}"
            ) from error
        identity_fields = (
            "status",
            "source_version",
            "source_snapshot_id",
            "pipeline_run_id",
            "row_count",
            "schema_fingerprint",
            "configuration_digest",
            "output_dataset_names",
            "output_dataset_uris",
            "quality_report_uri",
        )
        if any(existing.get(field) != manifest.get(field) for field in identity_fields):
            raise EnrichmentAnalyticsSnapshotError("existing pipeline run manifest conflicts with retry")
        return path
    try:
        return publish_run_manifest(
            manifest,
            integrity=integrity,
            client=hdfs,
            config=hdfs_config,
        )
    except HdfsRawIngestionError as error:
        raise EnrichmentAnalyticsSnapshotError(
            f"pipeline run manifest publication failed: {error}"
        ) from error


def _write_or_validate_input_binding(
    hdfs: Any,
    staging_path: str,
    *,
    source_version: str,
    source_snapshot_id: str,
    pipeline_run_id: str,
    sales_daily_path: str,
    raw_path: str,
) -> None:
    expected = {
        "schema_version": _BINDING_SCHEMA_VERSION,
        "source_version": source_version,
        "source_snapshot_id": source_snapshot_id,
        "pipeline_run_id": pipeline_run_id,
        "sales_daily_path": sales_daily_path,
        "raw_path": raw_path,
        "code_revision": _CODE_REVISION,
    }
    path = render_enrichment_staging_binding_path(staging_path)
    if _exists(hdfs, path):
        if _read_json(hdfs, path, "enrichment input binding") != expected:
            raise EnrichmentAnalyticsSnapshotError(
                "existing enrichment staging binding conflicts with this snapshot"
            )
        return
    _write_json_once(hdfs, path, expected)


def _failure_document(
    *,
    source_version: str,
    source_snapshot_id: str,
    pipeline_run_id: str,
    raw_path: str | None,
    sales_daily_path: str,
    staging_path: str,
    curated_path: str,
    output_paths: Mapping[str, str],
    failure_details: Sequence[str],
) -> dict[str, object]:
    return {
        "quality_schema_version": _QUALITY_SCHEMA_VERSION,
        "status": FAILED,
        "source_version": source_version,
        "source_snapshot_id": source_snapshot_id,
        "pipeline_run_id": pipeline_run_id,
        "raw_path": raw_path,
        "sales_daily_path": sales_daily_path,
        "staging_path": staging_path,
        "curated_sales_enriched_path": curated_path,
        "output_candidate_paths": dict(output_paths),
        "failure_details": list(failure_details[:8]),
    }


def _write_parquet_once(
    spark: SparkSession,
    dataframe: DataFrame,
    uri: str,
    *,
    partition_by: Sequence[str] = (),
) -> None:
    writer = dataframe.write.mode("errorifexists").option("compression", "snappy")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(uri)


def _read_parquet(spark: SparkSession, uri: str) -> DataFrame:
    try:
        return spark.read.option("pathGlobFilter", "*.parquet").parquet(uri)
    except Exception as error:
        raise EnrichmentAnalyticsSnapshotError(f"cannot read persisted Parquet: {uri}") from error


def _ensure_staging_root(hdfs: Any, path: str) -> None:
    if _exists(hdfs, path):
        if not hdfs.is_directory(path):
            raise EnrichmentAnalyticsSnapshotError(f"staging path is not a directory: {path}")
        return
    hdfs.mkdir_parents(str(PurePosixPath(path).parent))
    try:
        hdfs.mkdir_exclusive(path)
    except Exception as error:
        if not _exists(hdfs, path) or not hdfs.is_directory(path):
            raise EnrichmentAnalyticsSnapshotError(
                f"staging path was not exclusively created: {path}"
            ) from error


def _write_json_once(hdfs: Any, path: str, document: Mapping[str, object]) -> None:
    payload = _json_bytes(document)
    if _exists(hdfs, path):
        if hdfs.read_remote(path) != payload:
            raise EnrichmentAnalyticsSnapshotError(f"refusing to overwrite immutable evidence: {path}")
        return
    hdfs.upload_bytes(payload, path)
    if hdfs.read_remote(path) != payload:
        raise EnrichmentAnalyticsSnapshotError(f"persisted evidence differs from candidate: {path}")


def _read_json(hdfs: Any, path: str, label: str) -> dict[str, object]:
    try:
        value = json.loads(hdfs.read_remote(path).decode("utf-8"))
    except (AttributeError, UnicodeError, json.JSONDecodeError, OSError, KeyError) as error:
        raise EnrichmentAnalyticsSnapshotError(f"{label} is not valid JSON: {path}") from error
    if not isinstance(value, dict):
        raise EnrichmentAnalyticsSnapshotError(f"{label} must be a JSON object: {path}")
    return value


def _json_string_list(value: object, label: str) -> list[str]:
    if not isinstance(value, str):
        raise EnrichmentAnalyticsSnapshotError(f"{label} must be a JSON string array")
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as error:
        raise EnrichmentAnalyticsSnapshotError(f"{label} is malformed JSON") from error
    if not isinstance(parsed, list) or not all(isinstance(item, str) for item in parsed):
        raise EnrichmentAnalyticsSnapshotError(f"{label} must be a JSON string array")
    return parsed


def _manifest_int(manifest: Mapping[str, object], field: str) -> int:
    value = manifest.get(field)
    if type(value) is not int or value < 0:
        raise EnrichmentAnalyticsSnapshotError(f"SALES_DAILY manifest field {field} is invalid")
    return value


def _schema_fingerprint(fields: Sequence[str]) -> str:
    return "sha256:" + hashlib.sha256(
        json.dumps(tuple(fields), separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _schema_fingerprint_map(fields: Mapping[str, Sequence[str]]) -> str:
    payload = json.dumps(
        {name: list(value) for name, value in fields.items()},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _configuration_digest(hdfs_config: HDFSConfig, *, spark_master: str) -> str:
    payload = json.dumps(
        {
            "hdfs_uri": hdfs_config.uri,
            "hdfs_base_path": hdfs_config.base_path,
            "spark_master": spark_master,
            "compression": "snappy",
            "sales_enriched_partitioning": ["year"],
            "category_monthly_contract": "PENDING_HUMAN_APPROVAL",
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _hdfs_uri(uri: str, path: str) -> str:
    parsed = urlsplit(uri)
    if parsed.scheme.lower() != "hdfs" or not parsed.netloc or not path.startswith("/"):
        raise EnrichmentAnalyticsSnapshotError(
            "enrichment inputs and outputs must use configured HDFS"
        )
    return uri.rstrip("/") + path


def _date_text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def _failure_detail(error: BaseException) -> str:
    message = str(error).strip() or error.__class__.__name__
    return f"{error.__class__.__name__}: {message[:_MAX_FAILURE_LENGTH]}"


def _json_bytes(value: object) -> bytes:
    return (json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":")) + "\n").encode(
        "utf-8"
    )


def _exists(hdfs: Any, path: str) -> bool:
    try:
        return bool(hdfs.exists(path))
    except Exception as error:
        raise EnrichmentAnalyticsSnapshotError(f"cannot inspect HDFS path {path}: {error}") from error


def _safe_exists(hdfs: Any, path: str) -> bool:
    try:
        return bool(hdfs.exists(path))
    except Exception:
        return False


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


__all__ = [
    "EnrichmentAnalyticsMetrics",
    "EnrichmentAnalyticsSnapshotError",
    "EnrichmentAnalyticsSnapshotResult",
    "run_enrichment_analytics_snapshot",
    "validate_enrichment_analytics_identifiers",
]
