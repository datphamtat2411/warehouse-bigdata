"""Offline contracts for the opt-in ClickHouse deployment."""

import os
import shutil
import subprocess
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_static_topology_and_resources():
    config = ROOT / "infrastructure/clickhouse/config.d"
    cluster = ET.parse(config / "warehouse-cluster.xml").getroot()
    shards = cluster.findall("remote_servers/warehouse_cluster/shard")
    assert len(shards) == 2
    for index, shard in enumerate(shards, 1):
        assert shard.findtext("internal_replication") == "false"
        replicas = shard.findall("replica")
        assert len(replicas) == 1
        assert replicas[0].findtext("host") == f"clickhouse-{index}"
        assert replicas[0].findtext("port") == "9000"
        assert replicas[0].find("password").get("from_env") == (
            "WAREHOUSE_CLICKHOUSE_PUBLISHER_PASSWORD"
        )
    resources = ET.parse(config / "warehouse-resources.xml").getroot()
    assert int(resources.findtext("max_server_memory_usage")) < 1024**3
    compose = (ROOT / "infrastructure/compose.yaml").read_text()
    assert (
        "clickhouse/clickhouse-server:26.8@sha256:8b28eb09479c36c4533eec7a6268917d04ba3a6fd90e3f4205f70e761b6d7779"
        in compose
    )
    assert '"127.0.0.1:8123:8123"' in compose
    assert "profiles: [clickhouse]" in compose
    for index in (1, 2):
        assert f"clickhouse-{index}-data:/var/lib/clickhouse" in compose


def test_initializer_preserves_values_and_is_private_and_idempotent(tmp_path):
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    shutil.copy(ROOT / "scripts/init-clickhouse-env.sh", scripts)
    shutil.copy(ROOT / ".env.example", tmp_path)
    env = tmp_path / ".env"
    env.write_text(
        "UNRELATED=keep\nWAREHOUSE_CLICKHOUSE_USER=existing_reader\n"
        "WAREHOUSE_CLICKHOUSE_PASSWORD=\n"
    )
    command = ["bash", str(scripts / "init-clickhouse-env.sh")]
    first = subprocess.run(command, capture_output=True, text=True, check=True)
    content = env.read_text()
    assert "UNRELATED=keep\n" in content
    assert "WAREHOUSE_CLICKHOUSE_USER=existing_reader\n" in content
    assert "WAREHOUSE_CLICKHOUSE_PASSWORD=\n" not in content
    assert env.stat().st_mode & 0o777 == 0o600
    subprocess.run(command, capture_output=True, check=True)
    assert env.read_text() == content
    assert "PASSWORD=" not in first.stdout + first.stderr


def test_lifecycle_rejects_destructive_commands():
    for action in ("down", "reset", "rm", "up --volumes"):
        result = subprocess.run(
            ["bash", str(ROOT / "scripts/clickhouse-cluster.sh"), action],
            capture_output=True,
            env={**os.environ},
        )
        assert result.returncode != 0
    script = (ROOT / "scripts/clickhouse-cluster.sh").read_text()
    assert "--no-deps" in script
    assert "--wait-timeout" in script
    access = (ROOT / "infrastructure/clickhouse/initdb/01-access.sh").read_text()
    assert "GRANT SELECT ON warehouse.*" in access
    assert "GRANT SELECT, INSERT ON warehouse.*" in access
    assert "IF NOT EXISTS" in access
