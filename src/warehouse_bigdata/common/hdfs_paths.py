"""Contract v1 HDFS paths used by immutable batch artifacts."""

from __future__ import annotations

import re
from pathlib import PurePosixPath

from warehouse_bigdata.contracts.manifests import HDFS_ARTIFACT_PATHS

_SOURCE_VERSION = re.compile(r"\Asha256:[0-9a-f]{64}\Z")
_SAFE_IDENTIFIER = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")


class HdfsPathError(ValueError):
    """Raised when an HDFS path identity is missing or unsafe."""


def validate_source_version(source_version: str) -> str:
    """Validate and return one content-derived source identity."""
    if not isinstance(source_version, str) or not _SOURCE_VERSION.fullmatch(source_version):
        raise HdfsPathError("source_version must be a content-derived SHA-256 identity")
    return source_version


def validate_pipeline_run_id(pipeline_run_id: str) -> str:
    """Validate and return one bounded execution-attempt identity."""
    if not isinstance(pipeline_run_id, str) or not _SAFE_IDENTIFIER.fullmatch(pipeline_run_id):
        raise HdfsPathError("pipeline_run_id must be a bounded safe identifier")
    return pipeline_run_id


def validate_source_snapshot_id(source_snapshot_id: str) -> str:
    """Validate and return one explicit immutable curated-snapshot identity."""
    if not isinstance(source_snapshot_id, str) or not _SAFE_IDENTIFIER.fullmatch(
        source_snapshot_id
    ):
        raise HdfsPathError("source_snapshot_id must be a bounded safe identifier")
    return source_snapshot_id


def render_hdfs_artifact_path(base_path: str, artifact_name: str, **values: str) -> str:
    """Render a declared Contract v1 artifact path without creating it."""
    base = PurePosixPath(base_path) if isinstance(base_path, str) else PurePosixPath(".")
    if not base.is_absolute() or any(part in {".", ".."} for part in base.parts[1:]):
        raise HdfsPathError("HDFS base path must be absolute")
    contract = next((item for item in HDFS_ARTIFACT_PATHS if item.name == artifact_name), None)
    if contract is None:
        raise HdfsPathError(f"missing HDFS artifact path contract: {artifact_name}")
    missing = [field for field in contract.identity_fields if field not in values]
    if missing:
        raise HdfsPathError(f"missing identity values for {artifact_name}: {missing}")
    if any(
        not isinstance(value, str) or any(part in {"", ".", ".."} for part in value.split("/"))
        for value in values.values()
    ):
        raise HdfsPathError("HDFS artifact identity values must be safe path components")
    path_values = {name: value.replace(":", "-") for name, value in values.items()}
    suffix = contract.template.removeprefix("/data/warehouse").format(**path_values)
    return str(PurePosixPath(base_path.rstrip("/") + suffix))


def render_raw_m5_path(base_path: str, source_version: str) -> str:
    """Render the immutable raw M5 directory for one source version."""
    validate_source_version(source_version)
    return render_hdfs_artifact_path(
        base_path,
        "raw_m5",
        source_version=source_version,
    )


def render_staging_path(base_path: str, pipeline_run_id: str) -> str:
    """Render one execution-owned staging directory."""
    validate_pipeline_run_id(pipeline_run_id)
    return render_hdfs_artifact_path(
        base_path,
        "staging",
        pipeline_run_id=pipeline_run_id,
    )


def render_curated_sales_daily_path(base_path: str, source_snapshot_id: str) -> str:
    """Render the immutable curated SALES_DAILY directory."""
    validate_source_snapshot_id(source_snapshot_id)
    return render_hdfs_artifact_path(
        base_path,
        "curated_sales_daily",
        source_snapshot_id=source_snapshot_id,
    )


def render_curated_sales_enriched_path(base_path: str, source_snapshot_id: str) -> str:
    """Render the immutable curated SALES_ENRICHED directory."""
    validate_source_snapshot_id(source_snapshot_id)
    return render_hdfs_artifact_path(
        base_path,
        "curated_sales_enriched",
        source_snapshot_id=source_snapshot_id,
    )


def render_curated_store_daily_sales_path(base_path: str, source_snapshot_id: str) -> str:
    """Render the immutable curated STORE_DAILY_SALES directory."""
    validate_source_snapshot_id(source_snapshot_id)
    return render_hdfs_artifact_path(
        base_path,
        "curated_store_daily_sales",
        source_snapshot_id=source_snapshot_id,
    )


def render_curated_category_monthly_sales_path(base_path: str, source_snapshot_id: str) -> str:
    """Render the immutable curated CATEGORY_MONTHLY_SALES directory."""
    validate_source_snapshot_id(source_snapshot_id)
    return render_hdfs_artifact_path(
        base_path,
        "curated_category_monthly_sales",
        source_snapshot_id=source_snapshot_id,
    )


def render_run_manifest_path(base_path: str, pipeline_run_id: str) -> str:
    """Render one immutable Contract v1 run-manifest path."""
    validate_pipeline_run_id(pipeline_run_id)
    return render_hdfs_artifact_path(
        base_path,
        "run_manifest",
        pipeline_run_id=pipeline_run_id,
    )


def validate_feature_version(feature_version: str) -> str:
    """Require an explicit bounded feature-semantic identity safe for canonical paths."""
    if not isinstance(feature_version, str) or not _SAFE_IDENTIFIER.fullmatch(feature_version):
        raise HdfsPathError("feature_version must be a bounded safe identifier")
    return feature_version


def render_forecast_features_path(
    base_path: str,
    source_snapshot_id: str,
    feature_version: str,
) -> str:
    """Render the frozen v1 feature artifact without creating publication ancestors."""
    validate_source_snapshot_id(source_snapshot_id)
    validate_feature_version(feature_version)
    return render_hdfs_artifact_path(
        base_path,
        "forecast_features",
        source_snapshot_id=source_snapshot_id,
        feature_version=feature_version,
    )


def render_normalization_chunks_path(staging_path: str) -> str:
    """Render the bounded normalized chunk parent under one staging attempt."""
    return _join_staging(staging_path, "chunks")


def validate_model_run_id(model_run_id: str) -> str:
    """Require an explicit immutable model identity safe for its Contract v1 path."""
    if not isinstance(model_run_id, str) or not _SAFE_IDENTIFIER.fullmatch(model_run_id):
        raise HdfsPathError("model_run_id must be a bounded safe identifier")
    return model_run_id


def render_model_artifact_path(base_path: str, model_run_id: str) -> str:
    validate_model_run_id(model_run_id)
    return render_hdfs_artifact_path(base_path, "model_artifact", model_run_id=model_run_id)


def validate_forecast_run_id(forecast_run_id: str) -> str:
    if not isinstance(forecast_run_id, str) or not _SAFE_IDENTIFIER.fullmatch(forecast_run_id):
        raise HdfsPathError("forecast_run_id must be a bounded safe identifier")
    return forecast_run_id


def render_forecast_output_path(base_path: str, forecast_run_id: str) -> str:
    validate_forecast_run_id(forecast_run_id)
    return render_hdfs_artifact_path(base_path, "forecast_output", forecast_run_id=forecast_run_id)


def validate_evaluation_run_id(evaluation_run_id: str) -> str:
    if not isinstance(evaluation_run_id, str) or not _SAFE_IDENTIFIER.fullmatch(evaluation_run_id):
        raise HdfsPathError("evaluation_run_id must be a bounded safe identifier")
    return evaluation_run_id


def render_evaluation_output_path(base_path: str, evaluation_run_id: str) -> str:
    validate_evaluation_run_id(evaluation_run_id)
    return render_hdfs_artifact_path(
        base_path, "evaluation_output", evaluation_run_id=evaluation_run_id
    )


def render_normalization_chunk_path(staging_path: str, day_start: int, day_end: int) -> str:
    """Render one deterministic day-range Parquet chunk path."""
    _validate_day_range(day_start, day_end)
    return _join_staging(
        staging_path,
        "chunks",
        f"chunk_{day_start:04d}_{day_end:04d}",
    )


def render_normalization_chunk_manifest_path(
    staging_path: str,
    day_start: int,
    day_end: int,
) -> str:
    """Render one immutable chunk-quality sidecar path."""
    _validate_day_range(day_start, day_end)
    return _join_staging(
        staging_path,
        "chunk_manifests",
        f"chunk_{day_start:04d}_{day_end:04d}.json",
    )


def render_normalization_candidate_path(staging_path: str) -> str:
    """Render the compacted candidate Parquet directory."""
    return _join_staging(staging_path, "candidate")


def render_normalization_quality_path(staging_path: str) -> str:
    """Render the immutable quality report path for one staging attempt."""
    return _join_staging(staging_path, "quality_report.json")


def render_normalization_failure_path(staging_path: str) -> str:
    """Render a separate failure report without replacing prior quality evidence."""
    return _join_staging(staging_path, "failure_report.json")


def render_enrichment_candidate_path(staging_path: str, output_name: str) -> str:
    """Render one run-owned enrichment or analytics candidate directory."""
    _validate_enrichment_output_name(output_name)
    return _join_staging(staging_path, "candidates", output_name)


def render_enrichment_quality_path(staging_path: str) -> str:
    """Render the run-level enrichment quality report."""
    return _join_staging(staging_path, "quality_report.json")


def render_enrichment_output_quality_path(staging_path: str, output_name: str) -> str:
    """Render one immutable per-output enrichment quality report."""
    _validate_enrichment_output_name(output_name)
    return _join_staging(staging_path, "quality_reports", f"{output_name}.json")


def render_enrichment_failure_path(staging_path: str) -> str:
    """Render the immutable failed-run evidence for one enrichment attempt."""
    return _join_staging(staging_path, "failure_report.json")


def render_enrichment_staging_binding_path(staging_path: str) -> str:
    """Render the immutable input binding for one enrichment attempt."""
    return _join_staging(staging_path, "input_binding.json")


def render_enrichment_dataset_manifest_path(dataset_path: str) -> str:
    """Render the immutable manifest stored beside one J03 dataset."""
    path = PurePosixPath(dataset_path)
    if not path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts[1:]):
        raise HdfsPathError("dataset path must be an absolute safe HDFS path")
    return str(path / "_dataset_manifest.json")


def _join_staging(staging_path: str, *parts: str) -> str:
    path = PurePosixPath(staging_path)
    if not path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts[1:]):
        raise HdfsPathError("staging path must be an absolute safe HDFS path")
    if any(not part or part in {".", ".."} or "/" in part for part in parts):
        raise HdfsPathError("normalization path components must be safe")
    return str(path.joinpath(*parts))


def _validate_enrichment_output_name(output_name: str) -> None:
    if output_name not in {
        "sales_enriched",
        "store_daily_sales",
        "category_monthly_sales",
    }:
        raise HdfsPathError(f"unsupported enrichment output name: {output_name!r}")


def _validate_day_range(day_start: int, day_end: int) -> None:
    if type(day_start) is not int or type(day_end) is not int or not 1 <= day_start <= day_end:
        raise HdfsPathError("normalization day range must be positive and ordered")
