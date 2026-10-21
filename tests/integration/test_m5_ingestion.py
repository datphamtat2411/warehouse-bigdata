"""Opt-in M5 raw-ingestion and Spark Standalone read acceptance."""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from pathlib import Path

import pytest

from tests.integration.hdfs_support import HDFSVerification, ROOT, require
from warehouse_bigdata.common.config import load_hdfs_config, load_spark_config
from warehouse_bigdata.common.hdfs_client import HdfsCliClient
from warehouse_bigdata.contracts.manifests import PIPELINE_RUN_MANIFEST, VALIDATED
from warehouse_bigdata.contracts.validation import validate_manifest
from warehouse_bigdata.ingestion.hdfs_raw import render_run_manifest_path
from warehouse_bigdata.ingestion.orchestration import run_m5_ingestion

pytestmark = pytest.mark.integration

SOURCE_DIR = os.environ.get("WAREHOUSE_M5_SOURCE_DIR")
SOURCE_URL = os.environ.get("WAREHOUSE_M5_SOURCE_URL")
DOWNLOADED_AT_UTC = os.environ.get("WAREHOUSE_M5_DOWNLOADED_AT_UTC")
if not all((SOURCE_DIR, SOURCE_URL, DOWNLOADED_AT_UTC)):
    pytestmark = [
        pytest.mark.integration,
        pytest.mark.skip(
            reason=(
                "Set WAREHOUSE_M5_SOURCE_DIR, WAREHOUSE_M5_SOURCE_URL, and "
                "WAREHOUSE_M5_DOWNLOADED_AT_UTC for the live M5 acceptance."
            )
        ),
    ]


def _client(hdfs: HDFSVerification) -> HdfsCliClient:
    return HdfsCliClient(
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


def _raw_uri(config, raw_path: str) -> str:
    return config.uri.rstrip("/") + raw_path


def _assert_remote_source_evidence(client: HdfsCliClient, source_dir: Path, raw_path: str) -> None:
    nodes = client.report()
    addresses = {node.address for node in nodes.values()}
    local_files = {
        path.name: path
        for path in source_dir.iterdir()
        if path.is_file() and path.suffix.lower() == ".csv"
    }
    for filename, local_path in local_files.items():
        remote_path = f"{raw_path}/{filename}"
        expected = local_path.read_bytes()
        stat = client.stat(remote_path)
        remote_hash = client.hash_remote(remote_path)
        blocks = client.fsck(remote_path, addresses, expected_replication=2)
        assert stat.size_bytes == len(expected)
        assert stat.replication == 2
        assert remote_hash.size_bytes == len(expected)
        assert remote_hash.sha256 == "sha256:" + hashlib.sha256(expected).hexdigest()
        assert blocks and sum(block.length for block in blocks) == len(expected)
        assert all(len(block.locations) == 2 and set(block.locations) == addresses for block in blocks)


def _submit_spark_read(hdfs: HDFSVerification, raw_uri: str, source_version: str) -> dict:
    command = [
        "timeout",
        "--foreground",
        "--signal=TERM",
        "--kill-after=45s",
        "300s",
        "bash",
        str(ROOT / "scripts/spark-submit.sh"),
        "--",
        "warehouse_bigdata/jobs/verify_m5_raw_hdfs.py",
        "--raw-uri",
        raw_uri,
        "--source-version",
        source_version,
    ]
    completed = hdfs.runner.run(command, timeout=360, check=False)
    require(completed.returncode == 0, f"M5 Spark verification failed: {completed.stderr[-2000:]!r}")
    records = [
        json.loads(line.removeprefix("WAREHOUSE_M5_RAW "))
        for line in completed.stdout.decode().splitlines()
        if line.startswith("WAREHOUSE_M5_RAW ")
    ]
    require(len(records) == 1, "M5 Spark verification must emit one result record.")
    return records[0]


def test_real_m5_ingestion_manifest_integrity_and_spark_hdfs_read(
    tmp_path: Path, hdfs: HDFSVerification
) -> None:
    assert SOURCE_DIR is not None and SOURCE_URL is not None and DOWNLOADED_AT_UTC is not None
    source_dir = Path(SOURCE_DIR).resolve(strict=True)
    hdfs_config = load_hdfs_config()
    spark_config = load_spark_config()
    assert spark_config.master_url.startswith("spark://")
    client = _client(hdfs)
    pipeline_run_id = "integration-m5-" + uuid.uuid4().hex

    result = run_m5_ingestion(
        source_dir,
        SOURCE_URL,
        DOWNLOADED_AT_UTC,
        artifact_root=tmp_path,
        hdfs_config=hdfs_config,
        client=client,
        pipeline_run_id=pipeline_run_id,
    )

    assert result.status == VALIDATED
    assert result.source_snapshot_id is None
    assert result.source_version is not None
    assert result.raw_path is not None
    raw_uri = _raw_uri(hdfs_config, result.raw_path)
    local_manifests = json.loads(
        result.artifact_paths["source_manifests"].read_text(encoding="utf-8")
    )
    remote_manifests = json.loads(
        client.read_remote(result.raw_path + "/source_manifests.json").decode("utf-8")
    )
    assert {
        (item["source_file"], item["source_version"], item["artifact_checksum"], item["size_bytes"])
        for item in remote_manifests
    } == {
        (item["source_file"], item["source_version"], item["artifact_checksum"], item["size_bytes"])
        for item in local_manifests
    }
    assert all(item["status"] == VALIDATED for item in remote_manifests)
    _assert_remote_source_evidence(client, source_dir, result.raw_path)

    run_manifest_path = render_run_manifest_path(hdfs_config.base_path, pipeline_run_id)
    run_manifest = json.loads(client.read_remote(run_manifest_path).decode("utf-8"))
    validate_manifest(
        PIPELINE_RUN_MANIFEST,
        run_manifest,
        lineage_context={
            "required_status": VALIDATED,
            "pipeline_run_id": pipeline_run_id,
            "source_version": result.source_version,
        },
    )
    assert run_manifest["source_version"] == result.source_version
    assert "source_snapshot_id" not in run_manifest

    spark_result = _submit_spark_read(hdfs, raw_uri, result.source_version)
    assert spark_result["status"] == VALIDATED
    assert spark_result["raw_uri"] == raw_uri
    assert spark_result["source_version"] == result.source_version
    assert spark_result["hdfs_configuration"]["fs.defaultFS"] == hdfs_config.uri
    assert spark_result["sales_header"].startswith("id,")

    rerun_id = pipeline_run_id + "-rerun"
    rerun = run_m5_ingestion(
        source_dir,
        SOURCE_URL,
        DOWNLOADED_AT_UTC,
        artifact_root=tmp_path,
        hdfs_config=hdfs_config,
        client=client,
        pipeline_run_id=rerun_id,
    )
    assert rerun.status == VALIDATED
    assert rerun.raw_path == result.raw_path
    assert rerun.artifact_paths["pipeline_run_manifest"] != result.artifact_paths[
        "pipeline_run_manifest"
    ]
