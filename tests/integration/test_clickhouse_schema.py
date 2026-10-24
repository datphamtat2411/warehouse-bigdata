"""Opt-in synthetic acceptance against the existing two-shard deployment."""

import json
import subprocess
import uuid
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[2]


def query(node, sql):
    command = [
        "bash",
        str(ROOT / "scripts/load-env.sh"),
        "docker",
        "compose",
        "--env-file",
        str(ROOT / ".env"),
        "-f",
        str(ROOT / "infrastructure/compose.yaml"),
        "--profile",
        "clickhouse",
        "exec",
        "-T",
        node,
        "bash",
        "-c",
        'clickhouse-client --user "$CLICKHOUSE_USER" --multiquery --query "$1"',
        "schema-test",
        sql,
    ]
    return subprocess.run(
        command, check=True, capture_output=True, text=True, timeout=60
    ).stdout.strip()


def test_schema_and_distributed_routing(tmp_path):
    subprocess.run(
        ["bash", str(ROOT / "scripts/clickhouse-cluster.sh"), "check"], check=True, timeout=150
    )
    subprocess.run(
        ["bash", str(ROOT / "scripts/clickhouse-schema.sh"), "check"], check=True, timeout=180
    )
    names = query(
        "clickhouse-1",
        "SELECT name FROM system.tables WHERE database = 'warehouse' AND engine = 'Distributed' ORDER BY name",
    ).splitlines()
    assert len(names) == 9
    for name in names:
        metadata = []
        for node in ("clickhouse-1", "clickhouse-2"):
            ddl = query(node, f"SHOW CREATE TABLE warehouse.{name}_local FORMAT TSVRaw")
            (tmp_path / f"{node}-{name}.sql").write_text(ddl)
            metadata.append(ddl)
        assert metadata[0] == metadata[1]
        (tmp_path / f"distributed-{name}.sql").write_text(
            query("clickhouse-1", f"SHOW CREATE TABLE warehouse.{name} FORMAT TSVRaw")
        )

    database = "schema_test_" + uuid.uuid4().hex
    publishers = {}
    try:
        for node in ("clickhouse-1", "clickhouse-2"):
            query(
                node,
                f"CREATE DATABASE {database}; CREATE TABLE {database}.probe_local (item_id String, store_id String) ENGINE = MergeTree ORDER BY (item_id, store_id)",
            )
            # The cluster uses the existing publisher account for remote requests.
            publishers[node] = query(
                node,
                "SELECT name FROM system.users WHERE name != currentUser() AND name IN (SELECT user_name FROM system.grants WHERE access_type = 'INSERT' AND database = 'warehouse') FORMAT TSVRaw",
            )
            query(node, f"GRANT SELECT, INSERT ON {database}.* TO {publishers[node]}")
        query(
            "clickhouse-1",
            f"CREATE TABLE {database}.probe AS {database}.probe_local ENGINE = Distributed('warehouse_cluster', '{database}', 'probe_local', cityHash64(item_id, store_id)); INSERT INTO {database}.probe SETTINGS insert_distributed_sync = 1 SELECT toString(number), 'store' FROM numbers(100)",
        )
        counts = [
            int(query(node, f"SELECT count() FROM {database}.probe_local"))
            for node in ("clickhouse-1", "clickhouse-2")
        ]
        rows = json.loads(
            query(
                "clickhouse-1",
                f"SELECT _shard_num AS shard, count() AS n FROM {database}.probe GROUP BY shard ORDER BY shard SETTINGS skip_unavailable_shards = 0 FORMAT JSON",
            )
        )["data"]
        assert [int(row["shard"]) for row in rows] == [1, 2]
        assert [int(row["n"]) for row in rows] == counts
        assert sum(counts) == 100
        with pytest.raises(subprocess.CalledProcessError):
            query(
                "clickhouse-1",
                f"SELECT count() FROM remote('127.0.0.1:1', {database}.probe_local) SETTINGS skip_unavailable_shards = 0, connect_timeout = 1",
            )
    finally:
        for node in ("clickhouse-1", "clickhouse-2"):
            if node in publishers:
                query(node, f"REVOKE SELECT, INSERT ON {database}.* FROM {publishers[node]}")
            query(node, f"DROP DATABASE IF EXISTS {database} SYNC")
