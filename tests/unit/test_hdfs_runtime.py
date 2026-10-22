"""Focused checks for the production Hadoop CLI runtime factory."""

from __future__ import annotations

import json
import subprocess

import pytest

from warehouse_bigdata.common.config import ConfigurationError, HDFSConfig
from warehouse_bigdata.common.hdfs_runtime import (
    HADOOP_CLIENT_IMAGE,
    HADOOP_CLIENT_LABEL,
    HADOOP_CLIENT_NETWORK,
    HADOOP_CLIENT_XML_TARGETS,
    create_hdfs_cli_client,
    load_hdfs_cli_config,
)

CONTAINER = "warehouse-hdfs-cli-0123456789abcdef0123456789abcdef"
OWNER = "0123456789abcdef0123456789abcdef"
HDFS_CONFIG = HDFSConfig("hdfs://hdfs-namenode:8020", "/data/warehouse")


def _container_info(
    *,
    status: str = "healthy",
    network: str = HADOOP_CLIENT_NETWORK,
    image: str = HADOOP_CLIENT_IMAGE,
    mounts: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    if mounts is None:
        mounts = [
            {"Type": "bind", "Destination": target, "RW": False}
            for target in HADOOP_CLIENT_XML_TARGETS
        ]
    return {
        "Name": f"/{CONTAINER}",
        "Config": {
            "Image": image,
            "User": "1000:100",
            "Labels": {HADOOP_CLIENT_LABEL: OWNER},
        },
        "State": {
            "Status": "running",
            "Paused": False,
            "Restarting": False,
            "Dead": False,
            "Health": {"Status": status},
        },
        "NetworkSettings": {"Networks": {network: {}}},
        "Mounts": mounts,
    }


def _patch_runtime(
    monkeypatch: pytest.MonkeyPatch,
    info: dict[str, object],
    values: dict[str, str] | None = None,
) -> list[list[str]]:
    calls: list[list[str]] = []
    monkeypatch.setattr(
        "warehouse_bigdata.common.hdfs_runtime.shutil.which", lambda _: "/usr/bin/docker"
    )

    def run(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(args)
        return subprocess.CompletedProcess(args, 0, json.dumps([info]), "")

    monkeypatch.setattr("warehouse_bigdata.common.hdfs_runtime.subprocess.run", run)

    class FakeClient:
        command_prefix = ("docker", "exec", "-i", CONTAINER)

        def __init__(self, *args: object, **kwargs: object) -> None:
            self.command_prefix = tuple(kwargs["command_prefix"])

        def getconf(self, key: str) -> str:
            return (values or {}).get(
                key,
                {
                    "fs.defaultFS": HDFS_CONFIG.uri,
                    "dfs.replication": "2",
                    "dfs.namenode.acls.enabled": "true",
                    "dfs.client.use.datanode.hostname": "true",
                    "dfs.datanode.use.datanode.hostname": "true",
                }[key],
            )

    monkeypatch.setattr("warehouse_bigdata.common.hdfs_runtime.HdfsCliClient", FakeClient)
    return calls


def test_runtime_config_requires_a_specific_client_container() -> None:
    with pytest.raises(ConfigurationError, match="WAREHOUSE_HDFS_CLI_CONTAINER"):
        load_hdfs_cli_config({})

    config = load_hdfs_cli_config(
        {
            "WAREHOUSE_HDFS_CLI_CONTAINER": CONTAINER,
            "WAREHOUSE_HDFS_CLI_OWNER": OWNER,
        }
    )
    assert config.container_name == CONTAINER
    assert config.owner_token == OWNER


def test_factory_uses_docker_exec_stdin_and_validates_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _patch_runtime(monkeypatch, _container_info())

    client = create_hdfs_cli_client(
        HDFS_CONFIG,
        {
            "WAREHOUSE_HDFS_CLI_CONTAINER": CONTAINER,
            "WAREHOUSE_HDFS_CLI_OWNER": OWNER,
        },
    )

    assert client.command_prefix == ("docker", "exec", "-i", CONTAINER)
    assert calls == [["docker", "inspect", "--type", "container", CONTAINER]]


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("status", "unhealthy", "unhealthy"),
        ("network", "other-network", "network"),
        ("image", "apache/hadoop:3.4.3", "image"),
    ],
)
def test_factory_rejects_unhealthy_or_misconfigured_client(
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: str,
    message: str,
) -> None:
    info = _container_info(**{field: value})
    _patch_runtime(monkeypatch, info)

    with pytest.raises(ConfigurationError, match=message):
        create_hdfs_cli_client(
            HDFS_CONFIG,
            {
                "WAREHOUSE_HDFS_CLI_CONTAINER": CONTAINER,
                "WAREHOUSE_HDFS_CLI_OWNER": OWNER,
            },
        )


def test_factory_rejects_wrong_effective_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_runtime(monkeypatch, _container_info(), {"fs.defaultFS": "hdfs://other:8020"})

    with pytest.raises(ConfigurationError, match="fs.defaultFS"):
        create_hdfs_cli_client(
            HDFS_CONFIG,
            {
                "WAREHOUSE_HDFS_CLI_CONTAINER": CONTAINER,
                "WAREHOUSE_HDFS_CLI_OWNER": OWNER,
            },
        )


def test_factory_reports_absent_container(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "warehouse_bigdata.common.hdfs_runtime.shutil.which", lambda _: "/usr/bin/docker"
    )

    def run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(args, 1, "", "No such container")

    monkeypatch.setattr("warehouse_bigdata.common.hdfs_runtime.subprocess.run", run)

    with pytest.raises(ConfigurationError, match="absent"):
        create_hdfs_cli_client(
            HDFS_CONFIG,
            {
                "WAREHOUSE_HDFS_CLI_CONTAINER": CONTAINER,
                "WAREHOUSE_HDFS_CLI_OWNER": OWNER,
            },
        )
