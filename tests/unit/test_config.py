"""Focused tests for shared configuration loading and validation."""

import importlib
import socket
from dataclasses import FrozenInstanceError
from pathlib import Path

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


def test_hdfs_uri_is_required_and_empty_values_do_not_use_defaults() -> None:
    with pytest.raises(ConfigurationError, match="WAREHOUSE_HDFS_URI"):
        load_hdfs_config({})

    with pytest.raises(ConfigurationError, match="WAREHOUSE_HDFS_URI"):
        load_hdfs_config({"WAREHOUSE_HDFS_URI": ""})


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
    ],
)
def test_invalid_hdfs_uri(uri: str) -> None:
    with pytest.raises(ConfigurationError, match="WAREHOUSE_HDFS_URI"):
        load_hdfs_config({"WAREHOUSE_HDFS_URI": uri})


def test_hdfs_defaults_and_explicit_base_path() -> None:
    assert load_hdfs_config({"WAREHOUSE_HDFS_URI": "hdfs://namenode:8020"}) == HDFSConfig(
        uri="hdfs://namenode:8020",
        base_path="/warehouse-bigdata",
    )
    assert load_hdfs_config(
        {
            "WAREHOUSE_HDFS_URI": "hdfs://namenode",
            "WAREHOUSE_HDFS_BASE_PATH": "/curated/m5",
        }
    ) == HDFSConfig(uri="hdfs://namenode", base_path="/curated/m5")


@pytest.mark.parametrize(
    "base_path",
    ["", "warehouse-bigdata", "file:///tmp/warehouse", "hdfs://namenode/path", "C:/warehouse"],
)
def test_invalid_hdfs_base_path(base_path: str) -> None:
    with pytest.raises(ConfigurationError, match="WAREHOUSE_HDFS_BASE_PATH"):
        load_hdfs_config(
            {
                "WAREHOUSE_HDFS_URI": "hdfs://namenode:8020",
                "WAREHOUSE_HDFS_BASE_PATH": base_path,
            }
        )


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
    variables = {
        line.partition("=")[0]
        for line in env_file.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#") and "=" in line
    }

    assert variables == ALL_VARIABLES
    assert RUNTIME_VARIABLES <= variables
