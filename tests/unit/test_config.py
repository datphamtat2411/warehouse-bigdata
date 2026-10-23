"""Focused tests for shared configuration loading and validation."""

import importlib
import socket
from dataclasses import FrozenInstanceError
from pathlib import Path, PurePosixPath

import pytest

from warehouse_bigdata.common.config import (
    ConfigurationError,
    HDFSConfig,
    PathsConfig,
    RuntimeConfig,
    SparkConfig,
    load_clickhouse_config,
    load_hdfs_config,
    load_paths_config,
    load_runtime_config,
    load_spark_config,
)
from warehouse_bigdata.contracts.manifests import HDFS_ARTIFACT_PATHS

RUNTIME_VARIABLES = {
    "WAREHOUSE_ENV",
    "WAREHOUSE_LOG_LEVEL",
}
ALL_VARIABLES = {
    "WAREHOUSE_ENV",
    "WAREHOUSE_LOG_LEVEL",
    "WAREHOUSE_HDFS_URI",
    "WAREHOUSE_HDFS_BASE_PATH",
    "WAREHOUSE_SPARK_MASTER_URL",
    "WAREHOUSE_CLICKHOUSE_HOST",
    "WAREHOUSE_CLICKHOUSE_PORT",
    "WAREHOUSE_CLICKHOUSE_DATABASE",
    "WAREHOUSE_CLICKHOUSE_USER",
    "WAREHOUSE_CLICKHOUSE_PASSWORD",
    "WAREHOUSE_ARTIFACTS_DIR",
    "WAREHOUSE_SCRATCH_DIR",
    "WAREHOUSE_M5_SOURCE_DIR",
    "WAREHOUSE_M5_SOURCE_URL",
    "WAREHOUSE_M5_DOWNLOADED_AT_UTC",
    "WAREHOUSE_M04_PERFORMANCE",
    "WAREHOUSE_M04_SOURCE_VERSION",
    "WAREHOUSE_M05_SOURCE_VERSION",
    "WAREHOUSE_M05_SOURCE_SNAPSHOT_ID",
    "WAREHOUSE_M05_UPSTREAM_PIPELINE_RUN_ID",
    "WAREHOUSE_SMALL_ARTIFACTS",
    "WAREHOUSE_SMALL_STAGES",
}


def clickhouse_environment() -> dict[str, str]:
    """Return a complete valid ClickHouse environment mapping."""
    return {
        "WAREHOUSE_CLICKHOUSE_HOST": "localhost",
        "WAREHOUSE_CLICKHOUSE_PORT": "8123",
        "WAREHOUSE_CLICKHOUSE_DATABASE": "warehouse",
        "WAREHOUSE_CLICKHOUSE_USER": "analyst",
        "WAREHOUSE_CLICKHOUSE_PASSWORD": "local-only-password",
    }


def test_runtime_defaults() -> None:
    assert load_runtime_config({}) == RuntimeConfig(environment="local", log_level="INFO")


def test_runtime_overrides_are_normalized() -> None:
    config = load_runtime_config(
        {
            "WAREHOUSE_ENV": "TEST",
            "WAREHOUSE_LOG_LEVEL": "debug",
        }
    )

    assert config == RuntimeConfig(environment="test", log_level="DEBUG")


@pytest.mark.parametrize("value", ["staging", ""])
def test_invalid_runtime_environment(value: str) -> None:
    with pytest.raises(ConfigurationError, match="WAREHOUSE_ENV"):
        load_runtime_config({"WAREHOUSE_ENV": value})


@pytest.mark.parametrize("value", ["TRACE", ""])
def test_invalid_log_level(value: str) -> None:
    with pytest.raises(ConfigurationError, match="WAREHOUSE_LOG_LEVEL"):
        load_runtime_config({"WAREHOUSE_LOG_LEVEL": value})


@pytest.mark.parametrize("environment", ["local", "test", "container"])
def test_hdfs_uri_is_required_in_every_runtime_environment(environment: str) -> None:
    with pytest.raises(ConfigurationError, match="WAREHOUSE_HDFS_URI"):
        load_hdfs_config({"WAREHOUSE_ENV": environment})


@pytest.mark.parametrize("environment", ["local", "test", "container"])
@pytest.mark.parametrize("uri", ["", " \t\n", None, 8020])
def test_invalid_required_hdfs_uri_does_not_use_a_default(environment: str, uri: object) -> None:
    with pytest.raises(ConfigurationError, match="WAREHOUSE_HDFS_URI"):
        load_hdfs_config(
            {
                "WAREHOUSE_ENV": environment,
                "WAREHOUSE_HDFS_URI": uri,  # type: ignore[dict-item]
            }
        )


@pytest.mark.parametrize(
    "uri",
    [
        "http://namenode:8020",
        "hdfs://",
        "hdfs://user:password@namenode:8020",
        "hdfs://namenode:not-a-port",
        "hdfs://namenode:0",
        "hdfs://namenode:65536",
        "hdfs://namenode:8020?query=1",
        "hdfs://namenode:8020#fragment",
        " hdfs://namenode:8020",
        "hdfs://name node:8020",
        "hdfs://namenode:8020\n",
        "hdfs://namenode:",
        "hdfs://[::1",
        "hdfs://[not-an-ip]:8020",
    ],
)
def test_invalid_hdfs_uri(uri: str) -> None:
    with pytest.raises(ConfigurationError, match="WAREHOUSE_HDFS_URI"):
        load_hdfs_config({"WAREHOUSE_HDFS_URI": uri})


@pytest.mark.parametrize(
    "uri",
    [
        "hdfs://hdfs-namenode:8020",
        "hdfs://localhost:19000",
        "hdfs://other-namenode:9000",
        "hdfs://namenode",
    ],
)
def test_hdfs_explicit_endpoints_use_contract_namespace_by_default(uri: str) -> None:
    assert load_hdfs_config({"WAREHOUSE_HDFS_URI": uri}) == HDFSConfig(
        uri=uri,
        base_path="/data/warehouse",
    )


def test_hdfs_default_namespace_contains_frozen_artifact_templates() -> None:
    config = load_hdfs_config({"WAREHOUSE_HDFS_URI": "hdfs://hdfs-namenode:8020"})
    namespace = PurePosixPath(config.base_path)

    for artifact in HDFS_ARTIFACT_PATHS:
        assert namespace in PurePosixPath(artifact.template).parents, artifact.name


@pytest.mark.parametrize("base_path", ["/curated/m5", "/custom/warehouse/"])
def test_hdfs_explicit_base_path_is_preserved(base_path: str) -> None:
    assert load_hdfs_config(
        {
            "WAREHOUSE_HDFS_URI": "hdfs://namenode",
            "WAREHOUSE_HDFS_BASE_PATH": base_path,
        }
    ) == HDFSConfig(uri="hdfs://namenode", base_path=base_path)


@pytest.mark.parametrize(
    "base_path",
    [
        "",
        " \t\n",
        "warehouse-bigdata",
        "file:///tmp/warehouse",
        "hdfs://namenode/path",
        "C:/warehouse",
        "//namenode/warehouse",
        "/data/warehouse?query=1",
        "/data/warehouse#fragment",
    ],
)
def test_invalid_hdfs_base_path(base_path: str) -> None:
    with pytest.raises(ConfigurationError, match="WAREHOUSE_HDFS_BASE_PATH"):
        load_hdfs_config(
            {
                "WAREHOUSE_HDFS_URI": "hdfs://namenode:8020",
                "WAREHOUSE_HDFS_BASE_PATH": base_path,
            }
        )


def test_hdfs_loads_process_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WAREHOUSE_HDFS_URI", "hdfs://localhost:19000")
    monkeypatch.delenv("WAREHOUSE_HDFS_BASE_PATH", raising=False)

    assert load_hdfs_config() == HDFSConfig(
        uri="hdfs://localhost:19000", base_path="/data/warehouse"
    )

    monkeypatch.setenv("WAREHOUSE_HDFS_BASE_PATH", "/custom/warehouse")

    assert load_hdfs_config() == HDFSConfig(
        uri="hdfs://localhost:19000", base_path="/custom/warehouse"
    )


def test_hdfs_explicit_mapping_is_isolated_from_process_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WAREHOUSE_HDFS_URI", "hdfs://hdfs-namenode:8020")
    monkeypatch.setenv("WAREHOUSE_HDFS_BASE_PATH", "/ambient/warehouse")

    assert load_hdfs_config({"WAREHOUSE_HDFS_URI": "hdfs://localhost:19000"}) == HDFSConfig(
        uri="hdfs://localhost:19000", base_path="/data/warehouse"
    )
    with pytest.raises(ConfigurationError, match="WAREHOUSE_HDFS_URI"):
        load_hdfs_config({})


@pytest.mark.parametrize("master_url", ["local[*]", "local[2]", "local[2,3]"])
def test_spark_accepts_local_master_modes(master_url: str) -> None:
    assert load_spark_config({"WAREHOUSE_SPARK_MASTER_URL": master_url}) == SparkConfig(
        master_url=master_url
    )


@pytest.mark.parametrize(
    "master_url",
    [
        "",
        "http://spark-master:7077",
        "spark://spark-master",
        "spark://spark-master:not-a-port",
        "spark://spark-master:0",
        "spark://spark-master:65536",
        "spark://user@spark-master:7077",
        "local[]",
        "local[workers]",
    ],
)
def test_invalid_spark_master_url(master_url: str) -> None:
    with pytest.raises(ConfigurationError, match="WAREHOUSE_SPARK_MASTER_URL"):
        load_spark_config({"WAREHOUSE_SPARK_MASTER_URL": master_url})


def test_spark_standalone_master_url_is_valid() -> None:
    assert load_spark_config(
        {"WAREHOUSE_SPARK_MASTER_URL": "spark://spark-master:7077"}
    ) == SparkConfig(master_url="spark://spark-master:7077")


@pytest.mark.parametrize(
    "variable",
    [
        "WAREHOUSE_CLICKHOUSE_HOST",
        "WAREHOUSE_CLICKHOUSE_PORT",
        "WAREHOUSE_CLICKHOUSE_DATABASE",
        "WAREHOUSE_CLICKHOUSE_USER",
    ],
)
def test_required_clickhouse_settings(variable: str) -> None:
    environment = clickhouse_environment()
    environment.pop(variable)

    with pytest.raises(ConfigurationError, match=variable):
        load_clickhouse_config(environment)


@pytest.mark.parametrize("port", ["", "abc", "0", "65536", "-1", " 8123"])
def test_invalid_clickhouse_port(port: str) -> None:
    environment = clickhouse_environment()
    environment["WAREHOUSE_CLICKHOUSE_PORT"] = port

    with pytest.raises(ConfigurationError, match="WAREHOUSE_CLICKHOUSE_PORT"):
        load_clickhouse_config(environment)


def test_clickhouse_password_is_optional_and_redacted() -> None:
    environment = clickhouse_environment()
    environment.pop("WAREHOUSE_CLICKHOUSE_PASSWORD")
    config = load_clickhouse_config(environment)

    assert config.password == ""
    assert "local-only-password" not in repr(load_clickhouse_config(clickhouse_environment()))
    assert "password" not in repr(config).lower()


def test_password_is_not_exposed_by_validation_errors() -> None:
    environment = clickhouse_environment()
    environment["WAREHOUSE_CLICKHOUSE_PORT"] = "not-a-port"

    with pytest.raises(ConfigurationError) as error:
        load_clickhouse_config(environment)

    assert "local-only-password" not in str(error.value)


def test_component_loaders_are_independent() -> None:
    assert load_runtime_config({}) == RuntimeConfig(environment="local", log_level="INFO")
    assert load_hdfs_config({"WAREHOUSE_HDFS_URI": "hdfs://namenode"}).uri == "hdfs://namenode"
    assert load_spark_config({"WAREHOUSE_SPARK_MASTER_URL": "local[*]"}).master_url == "local[*]"
    assert load_clickhouse_config(clickhouse_environment()).host == "localhost"
    assert load_paths_config({}).artifacts_dir == Path("artifacts")
    assert load_paths_config({}).scratch_dir == Path("artifacts/scratch")


def test_configuration_objects_are_immutable() -> None:
    configs = [
        load_runtime_config({}),
        load_hdfs_config({"WAREHOUSE_HDFS_URI": "hdfs://namenode"}),
        load_spark_config({"WAREHOUSE_SPARK_MASTER_URL": "local[*]"}),
        load_clickhouse_config(clickhouse_environment()),
        load_paths_config({}),
    ]

    for config in configs:
        field_name = type(config).__slots__[0]
        with pytest.raises(FrozenInstanceError):
            setattr(config, field_name, getattr(config, field_name))


@pytest.mark.parametrize("value", ["hdfs://namenode/path", "file:///tmp/artifacts", "s3://bucket"])
def test_artifact_path_rejects_uri_forms(value: str) -> None:
    with pytest.raises(ConfigurationError, match="WAREHOUSE_ARTIFACTS_DIR"):
        load_paths_config({"WAREHOUSE_ARTIFACTS_DIR": value})


def test_artifact_path_is_validated_without_directory_creation() -> None:
    config = load_paths_config({"WAREHOUSE_ARTIFACTS_DIR": "local-artifacts"})

    assert config == PathsConfig(artifacts_dir=Path("local-artifacts"))


def test_importing_common_package_does_not_require_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for variable in ALL_VARIABLES:
        monkeypatch.delenv(variable, raising=False)

    common = importlib.import_module("warehouse_bigdata.common")
    importlib.reload(common)


def test_configuration_loading_does_not_use_filesystem_or_network(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(*args: object, **kwargs: object) -> None:
        raise AssertionError("configuration loading must not perform I/O")

    monkeypatch.setattr(Path, "exists", fail)
    monkeypatch.setattr(Path, "mkdir", fail)
    monkeypatch.setattr(Path, "resolve", fail)
    monkeypatch.setattr(socket, "getaddrinfo", fail)
    monkeypatch.setattr(socket, "create_connection", fail)

    load_runtime_config({})
    load_hdfs_config({"WAREHOUSE_HDFS_URI": "hdfs://namenode:8020"})
    load_spark_config({"WAREHOUSE_SPARK_MASTER_URL": "local[*]"})
    load_clickhouse_config(clickhouse_environment())
    load_paths_config({"WAREHOUSE_ARTIFACTS_DIR": "artifacts"})


def test_env_example_contains_exact_supported_variables() -> None:
    env_file = Path(__file__).parents[2] / ".env.example"
    values = dict(
        line.split("=", maxsplit=1)
        for line in env_file.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#") and "=" in line
    )

    assert set(values) == ALL_VARIABLES
    assert RUNTIME_VARIABLES <= values.keys()
    assert values["WAREHOUSE_HDFS_URI"] == ""
    assert values["WAREHOUSE_HDFS_BASE_PATH"] == "/data/warehouse"
    assert values["WAREHOUSE_SPARK_MASTER_URL"] == ""
    assert values["WAREHOUSE_CLICKHOUSE_HOST"] == ""
    assert values["WAREHOUSE_CLICKHOUSE_PORT"] == ""
    assert values["WAREHOUSE_CLICKHOUSE_DATABASE"] == ""
    assert values["WAREHOUSE_CLICKHOUSE_USER"] == ""
    assert values["WAREHOUSE_CLICKHOUSE_PASSWORD"] == ""
    assert values["WAREHOUSE_M5_SOURCE_DIR"] == ""
    assert values["WAREHOUSE_M5_SOURCE_URL"] == ""
    assert values["WAREHOUSE_M5_DOWNLOADED_AT_UTC"] == ""
    assert values["WAREHOUSE_M04_PERFORMANCE"] == "0"
    assert values["WAREHOUSE_M04_SOURCE_VERSION"] == ""
    assert values["WAREHOUSE_M05_SOURCE_VERSION"] == ""
    assert values["WAREHOUSE_M05_SOURCE_SNAPSHOT_ID"] == ""
    assert values["WAREHOUSE_M05_UPSTREAM_PIPELINE_RUN_ID"] == ""


def test_local_env_is_ignored_while_the_example_remains_trackable() -> None:
    gitignore = Path(__file__).parents[2] / ".gitignore"
    entries = gitignore.read_text(encoding="utf-8").splitlines()

    assert ".env" in entries
    assert ".env.*" in entries
    assert "!.env.example" in entries
