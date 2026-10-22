"""Opt-in driver-image CLI regression against a small, run-owned HDFS fixture."""

import json
import uuid
from textwrap import dedent

import pytest

from tests.integration.hdfs_support import ROOT, HDFSVerification
from tests.integration.m04_support import _create_event_directory, hdfs_client
from warehouse_bigdata.common.config import HDFSConfig
from warehouse_bigdata.common.hdfs_paths import render_staging_path

pytestmark = pytest.mark.integration


def test_driver_hdfs_cli_supports_m04_preflight_and_integrity_checks(
    hdfs: HDFSVerification,
) -> None:
    config = HDFSConfig(hdfs.config.uri, hdfs.fixture.path)
    run_id = "cli-" + uuid.uuid4().hex
    staging = render_staging_path(config.base_path, run_id)
    events = staging + "/events"
    _create_event_directory(hdfs_client(hdfs), config, events, expected_pipeline_run_id=run_id)
    probe = dedent("""\
        import hashlib
        import json
        import os
        import subprocess
        import sys

        from warehouse_bigdata.common.config import HDFSConfig
        from warehouse_bigdata.common.hdfs_permissions import prepare_spark_run_permissions
        from warehouse_bigdata.common.spark_hdfs_client import SparkHdfsCliClient

        uri, base, run_id, staging, events = sys.argv[1:]
        client = SparkHdfsCliClient()
        assert os.getuid() == os.getgid() == 185
        assert client.whoami() == "spark"
        version_result = subprocess.run(
            ["hdfs", "version"], capture_output=True, text=True, check=False, timeout=20
        )
        assert version_result.returncode == 0, (version_result.stdout, version_result.stderr)
        version = version_result.stdout.splitlines()[0]
        assert version == "Hadoop 3.4.3", version
        for key, expected in {
            "fs.defaultFS": uri,
            "dfs.replication": "2",
            "dfs.namenode.acls.enabled": "true",
            "dfs.client.use.datanode.hostname": "true",
            "dfs.datanode.use.datanode.hostname": "true",
        }.items():
            assert client.getconf(key) == expected, key
        assert client.safe_mode() == "Safe mode is OFF"
        nodes = client.report()
        paths = prepare_spark_run_permissions(
            client, HDFSConfig(uri, base), pipeline_run_id=run_id, event_log_dir=uri + events
        )
        assert paths == (staging, events)
        assert client.list_children(events) == ()

        child = staging + "/chunks"
        client.mkdir_exclusive(child)
        assert "default:user:spark:rwx" in client.get_acl(child)
        path = child + "/payload with spaces"
        payload = bytes(range(256)) * 256
        client.upload_bytes(payload, path)
        stat = client.stat(path)
        assert stat.size_bytes == len(payload) and stat.replication == 2
        assert client.read_remote(path) == payload
        digest = client.hash_remote(path)
        assert digest.size_bytes == len(payload)
        assert digest.sha256 == "sha256:" + hashlib.sha256(payload).hexdigest()
        assert client.checksum(path)
        blocks = client.fsck(path, {node.address for node in nodes.values()})
        assert blocks and sum(block.length for block in blocks) == len(payload)
        renamed = child + "/renamed"
        client.rename(path, renamed)
        assert not client.exists(path) and client.exists(renamed)
        assert client.read_remote(renamed) == payload
        client.remove(child)
        assert client.list_children(staging) == (events,)
        assert "pyspark" not in sys.modules
        print(json.dumps({"version": version, "bytes": len(payload), "blocks": len(blocks)}))
        """)
    name = "warehouse-hdfs-cli-test-" + uuid.uuid4().hex
    command = [
        "docker",
        "compose",
        "-f",
        str(ROOT / "infrastructure/compose.yaml"),
        "--profile",
        "spark-submit",
        "run",
        "--rm",
        "--no-deps",
        "-T",
        "--pull",
        "never",
        "--name",
        name,
        "--entrypoint",
        "/opt/warehouse-app/.venv/bin/python",
        "spark-submit",
        "-c",
        probe,
        config.uri,
        config.base_path,
        run_id,
        staging,
        events,
    ]
    try:
        result = hdfs.runner.run(command, timeout=240, check=False)
    finally:
        hdfs.runner.run(["docker", "rm", "--force", name], timeout=15, check=False)
    assert result.returncode == 0, (
        f"Driver HDFS CLI probe failed (exit {result.returncode})\n"
        f"stdout:\n{result.stdout.decode(errors='replace')}\n"
        f"stderr:\n{result.stderr.decode(errors='replace')}"
    )
    report = json.loads(result.stdout)
    assert report["version"] == "Hadoop 3.4.3" and report["bytes"] == 65_536
    hdfs.evidence.write("driver-hdfs-cli", report)
