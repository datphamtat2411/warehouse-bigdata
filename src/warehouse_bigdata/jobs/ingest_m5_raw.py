"""CLI orchestration for immutable M5 raw ingestion into HDFS."""

from __future__ import annotations

from pathlib import Path

from warehouse_bigdata.ingestion.hdfs_raw import ingest_m5_raw


def run_ingest_m5_raw(
    source_dir: Path,
    profile_path: Path,
    manifest_path: Path,
    pipeline_run_id: str,
) -> int:
    """Run one explicitly identified raw-ingestion attempt and print its outcome."""
    result = ingest_m5_raw(source_dir, profile_path, manifest_path, pipeline_run_id)
    print(f"status={result.status}")
    print(f"source_version={result.source_version}")
    print(f"raw_path={result.raw_path}")
    if result.staging_path is not None:
        print(f"staging_path={result.staging_path}")
    return 0
