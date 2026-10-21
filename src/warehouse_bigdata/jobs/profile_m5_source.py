"""CLI orchestration for local M5 source profiling."""

from __future__ import annotations

from pathlib import Path

from warehouse_bigdata.common.config import load_paths_config
from warehouse_bigdata.ingestion.m5_source import (
    SourceProvenance,
    profile_m5_source,
    write_source_artifacts,
)


def run_profile_m5_source(
    source_dir: Path,
    source_url: str,
    downloaded_at_utc: str,
) -> int:
    """Profile a local source package and print its generated artifact paths."""
    provenance = SourceProvenance(
        source_url=source_url,
        downloaded_at_utc=downloaded_at_utc,
    )
    result = profile_m5_source(Path(source_dir), provenance)
    artifacts = write_source_artifacts(result, load_paths_config().artifacts_dir)
    print(f"status={result.status}")
    print(f"source_profile={artifacts['source_profile']}")
    print(f"source_manifests={artifacts['source_manifests']}")
    return 0 if result.status == "VALIDATED" else 1
