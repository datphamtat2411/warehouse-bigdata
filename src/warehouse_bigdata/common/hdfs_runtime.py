"""Validated runtime wiring for the isolated Hadoop CLI client container."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from warehouse_bigdata.common.config import ConfigurationError, HDFSConfig
from warehouse_bigdata.common.hdfs_client import HdfsCliClient, HdfsCommandError

HADOOP_CLIENT_IMAGE = (
    "apache/hadoop:3.4.3@sha256:127774dadab40ce84df7ac668a7a8c99945688b3fe336f1388f4477ca33e1529"
)
HADOOP_CLIENT_NETWORK = "warehouse-bigdata"
HADOOP_CLIENT_USER = "1000:100"
HADOOP_CLIENT_LABEL = "io.warehouse-bigdata.hdfs-cli.owner"
HADOOP_CLIENT_XML_TARGETS = (
    "/opt/hadoop/etc/hadoop/core-site.xml",
    "/opt/hadoop/etc/hadoop/hdfs-site.xml",
)
HADOOP_CLIENT_CONTAINER_VARIABLE = "WAREHOUSE_HDFS_CLI_CONTAINER"
HADOOP_CLIENT_OWNER_VARIABLE = "WAREHOUSE_HDFS_CLI_OWNER"
HADOOP_CLIENT_CHECK_TIMEOUT = 15.0
_SAFE_CONTAINER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")
_SAFE_OWNER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")


@dataclass(frozen=True, slots=True)
class HdfsCliRuntimeConfig:
    """Host-side settings for one run-owned Hadoop client container."""

    container_name: str
    owner_token: str


def load_hdfs_cli_config(env: Mapping[str, str] | None = None) -> HdfsCliRuntimeConfig:
    """Load the explicit client-container identity without contacting Docker."""
    settings = os.environ if env is None else env
    container_name = settings.get(HADOOP_CLIENT_CONTAINER_VARIABLE, "")
    if not isinstance(container_name, str) or not _SAFE_CONTAINER.fullmatch(container_name):
        raise ConfigurationError(
            f"{HADOOP_CLIENT_CONTAINER_VARIABLE} must name the run-owned Hadoop client container."
        )
    owner_token = settings.get(HADOOP_CLIENT_OWNER_VARIABLE, "")
    if not isinstance(owner_token, str) or not _SAFE_OWNER.fullmatch(owner_token):
        raise ConfigurationError(
            f"{HADOOP_CLIENT_OWNER_VARIABLE} must be supplied as a bounded container ownership token."
        )
    return HdfsCliRuntimeConfig(container_name=container_name, owner_token=owner_token)


def create_hdfs_cli_client(
    hdfs_config: HDFSConfig,
    env: Mapping[str, str] | None = None,
) -> HdfsCliClient:
    """Validate the isolated client and return a streamed ``docker exec -i`` transport."""
    runtime = load_hdfs_cli_config(env)
    if shutil.which("docker") is None:
        raise ConfigurationError("Hadoop CLI transport requires the host docker executable.")

    info = _inspect_container(runtime.container_name)
    _validate_container(info, runtime)
    client = HdfsCliClient(command_prefix=("docker", "exec", "-i", runtime.container_name))
    expected_configuration = {
        "fs.defaultFS": hdfs_config.uri,
        "dfs.replication": "2",
        "dfs.client.use.datanode.hostname": "true",
        "dfs.datanode.use.datanode.hostname": "true",
    }
    try:
        for key, expected in expected_configuration.items():
            actual = client.getconf(key).strip()
            if actual != expected:
                raise ConfigurationError(
                    f"Hadoop CLI client configuration mismatch for {key}: "
                    f"expected {expected!r}, got {actual!r}"
                )
    except ConfigurationError:
        raise
    except HdfsCommandError as error:
        raise ConfigurationError(
            f"Hadoop CLI client {runtime.container_name!r} is unhealthy or unreachable: {error}"
        ) from error
    return client


def _inspect_container(container_name: str) -> dict[str, Any]:
    try:
        result = subprocess.run(
            ["docker", "inspect", "--type", "container", container_name],
            capture_output=True,
            text=True,
            timeout=HADOOP_CLIENT_CHECK_TIMEOUT,
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        raise ConfigurationError(
            f"Hadoop CLI client inspection timed out: {container_name!r}."
        ) from error
    except OSError as error:
        raise ConfigurationError(f"Cannot inspect Hadoop CLI client: {error}") from error
    if result.returncode != 0:
        detail = result.stderr.strip()[-500:]
        suffix = f": {detail}" if detail else "."
        raise ConfigurationError(
            f"Hadoop CLI client container is absent or not inspectable: {container_name!r}{suffix}"
        )
    try:
        payload = json.loads(result.stdout)
        info = payload[0]
    except (IndexError, TypeError, json.JSONDecodeError) as error:
        raise ConfigurationError(
            "Docker returned malformed Hadoop CLI client inspection data."
        ) from error
    if not isinstance(info, dict):
        raise ConfigurationError("Docker returned a non-object Hadoop CLI client inspection.")
    return info


def _validate_container(info: Mapping[str, Any], runtime: HdfsCliRuntimeConfig) -> None:
    if info.get("Name") != f"/{runtime.container_name}":
        raise ConfigurationError("Hadoop CLI client ownership does not match its configured name.")

    config = info.get("Config")
    if not isinstance(config, Mapping):
        raise ConfigurationError("Hadoop CLI client inspection has no container configuration.")
    if config.get("Image") != HADOOP_CLIENT_IMAGE:
        raise ConfigurationError(
            "Hadoop CLI client uses an unexpected image; use the pinned Apache Hadoop 3.4.3 image."
        )
    if config.get("User") not in {HADOOP_CLIENT_USER, "1000:100", 1000}:
        raise ConfigurationError("Hadoop CLI client must run as UID 1000/GID 100.")
    labels = config.get("Labels")
    if not isinstance(labels, Mapping) or not labels.get(HADOOP_CLIENT_LABEL):
        raise ConfigurationError("Hadoop CLI client ownership label is missing.")
    if labels[HADOOP_CLIENT_LABEL] != runtime.owner_token:
        raise ConfigurationError("Hadoop CLI client ownership token does not match the invocation.")

    state = info.get("State")
    if not isinstance(state, Mapping) or state.get("Status") != "running":
        raise ConfigurationError("Hadoop CLI client is absent or not running.")
    if any(state.get(field) for field in ("Paused", "Restarting", "Dead")):
        raise ConfigurationError("Hadoop CLI client is unhealthy or transitioning state.")
    health = state.get("Health")
    if isinstance(health, Mapping) and health.get("Status") == "unhealthy":
        raise ConfigurationError("Hadoop CLI client healthcheck is unhealthy.")

    network_settings = info.get("NetworkSettings")
    networks = network_settings.get("Networks") if isinstance(network_settings, Mapping) else None
    if not isinstance(networks, Mapping) or HADOOP_CLIENT_NETWORK not in networks:
        raise ConfigurationError(
            f"Hadoop CLI client must join the {HADOOP_CLIENT_NETWORK!r} bridge network."
        )

    mounts = info.get("Mounts")
    if not isinstance(mounts, list):
        raise ConfigurationError("Hadoop CLI client has no inspectable Hadoop XML mounts.")
    selected = {mount.get("Destination"): mount for mount in mounts if isinstance(mount, Mapping)}
    if set(selected) != set(HADOOP_CLIENT_XML_TARGETS):
        raise ConfigurationError(
            "Hadoop CLI client must mount only the two Hadoop XML configuration files."
        )
    for target in HADOOP_CLIENT_XML_TARGETS:
        mount = selected[target]
        if mount.get("Type") != "bind" or mount.get("RW") is not False:
            raise ConfigurationError(f"Hadoop XML mount must be read-only: {target}")
