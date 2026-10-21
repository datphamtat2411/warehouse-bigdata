"""Non-disruptive live HDFS raw-ingestion acceptance."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from tests.integration.hdfs_support import HDFSVerification
from warehouse_bigdata.common.hdfs_client import HdfsCliClient
from warehouse_bigdata.contracts.manifests import SOURCE_MANIFEST, VALIDATED
from warehouse_bigdata.ingestion.hdfs_raw import ingest_m5_raw
from warehouse_bigdata.ingestion.m5_source import RECOGNIZED_M5_FILES, compute_source_version

pytestmark = pytest.mark.integration


def test_live_raw_ingestion_replication_manifests_and_matching_rerun(
    tmp_path: Path, hdfs: HDFSVerification
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    payloads = {
        "calendar.csv": b"calendar,fixture\n1,deterministic\n",
        "sales_train_evaluation.csv": b"sales,fixture\n1,deterministic\n",
        "sell_prices.csv": b"prices,fixture\n1,deterministic\n",
    }
    for filename, payload in payloads.items():
        (source / filename).write_bytes(payload)

    hashes = {
        filename: "sha256:" + hashlib.sha256(payload).hexdigest()
        for filename, payload in payloads.items()
    }
    source_version = compute_source_version(hashes)
    timestamp = "2026-10-21T12:34:56Z"
    profile = {
        "status": VALIDATED,
        "source_version": source_version,
        "source_name": "m5-forecasting-accuracy",
        "source_url": "https://www.kaggle.com/competitions/m5-forecasting-accuracy/data",
        "downloaded_at_utc": timestamp,
        "source_schema_version": "m5-source-v1",
        "validation_scope": {"hdfs_validated": False},
        "files": {
            filename: {
                "source_file": filename,
                "required": filename not in {"sales_train_validation.csv", "sample_submission.csv"},
                "present": filename in payloads,
                "row_count": 1 if filename in payloads else 0,
                "size_bytes": len(payloads[filename]) if filename in payloads else None,
                "sha256": hashes.get(filename),
                "schema_fingerprint": "sha256:" + "0" * 64 if filename in payloads else None,
                "validation_status": VALIDATED,
            }
            for filename in RECOGNIZED_M5_FILES
        },
    }
    manifests = []
    for filename, payload in payloads.items():
        manifests.append(
            {
                "manifest_schema_version": SOURCE_MANIFEST.version,
                "contract_name": SOURCE_MANIFEST.name,
                "contract_version": SOURCE_MANIFEST.version,
                "manifest_kind": SOURCE_MANIFEST.manifest_kind,
                "status": VALIDATED,
                "producer": "integration-fixture",
                "artifact_uri": (source / filename).resolve().as_uri(),
                "artifact_checksum": hashes[filename],
                "schema_fingerprint": "sha256:" + "0" * 64,
                "grain": "deterministic fixture row",
                "logical_unique_key": "fixture_id",
                "row_count": 1,
                "required_field_null_counts": "{}",
                "code_revision": "integration-fixture",
                "configuration_digest": "sha256:" + "1" * 64,
                "created_at": timestamp,
                "completed_at": timestamp,
                "coverage_and_exclusion_counts": "{}",
                "source_version": source_version,
                "source_name": "m5-forecasting-accuracy",
                "source_url": profile["source_url"],
                "downloaded_at_utc": timestamp,
                "source_file": filename,
                "size_bytes": len(payload),
                "source_schema_version": "m5-source-v1",
                "source_file_row_count": 1,
            }
        )
    profile_path = tmp_path / "source_profile.json"
    manifest_path = tmp_path / "source_manifests.json"
    profile_path.write_text(json.dumps(profile), encoding="utf-8")
    manifest_path.write_text(json.dumps(manifests), encoding="utf-8")

    client = HdfsCliClient(
        command_prefix=(
            "docker",
            "exec",
            "-i",
            hdfs.client,
            "timeout",
            "--kill-after=5s",
            "120s",
        )
    )
    promoted = None
    try:
        first = ingest_m5_raw(
            source,
            profile_path,
            manifest_path,
            "integration-first",
            client=client,
        )
        promoted = first.raw_path
        assert first.status == "PROMOTED"
        assert len(first.manifests) == len(payloads)
        assert all(manifest["status"] == VALIDATED for manifest in first.manifests)

        second = ingest_m5_raw(
            source,
            profile_path,
            manifest_path,
            "integration-rerun",
            client=client,
        )
        assert second.status == "REUSED"
        assert second.raw_path == first.raw_path
    finally:
        if promoted is not None:
            client.remove(promoted)
