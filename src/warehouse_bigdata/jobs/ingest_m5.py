"""CLI entry point for the combined local-profiled M5 ingestion workflow."""

from __future__ import annotations

from pathlib import Path

from warehouse_bigdata.common.config import load_paths_config
from warehouse_bigdata.ingestion.orchestration import run_m5_ingestion


def run_ingest_m5(
    source_dir: Path,
    source_url: str,
    downloaded_at_utc: str,
    pipeline_run_id: str | None = None,
) -> int:
    """Run one M5 profiling, HDFS-ingestion, and run-manifest workflow."""
    result = run_m5_ingestion(
        Path(source_dir),
        source_url,
        downloaded_at_utc,
        artifact_root=load_paths_config().artifacts_dir,
        pipeline_run_id=pipeline_run_id,
    )
    print(f"status={result.status}")
    print(f"pipeline_run_id={result.pipeline_run_id}")
    print(f"source_version={result.source_version}")
    if result.raw_path is not None:
        print(f"raw_path={result.raw_path}")
    for name, path in sorted(result.artifact_paths.items()):
        print(f"{name}={path}")
    for detail in result.failure_details:
        print(f"failure={detail}")
    return 0 if result.status == "VALIDATED" else 1
