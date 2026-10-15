"""Typed, side-effect-free configuration loaders for warehouse-bigdata."""

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from ipaddress import ip_address
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit

_MISSING = object()
_ENVIRONMENTS = frozenset({"local", "test", "container"})
_LOG_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})
_LOCAL_MASTER_PATTERN = re.compile(r"local(?:\[(?:\*|\d+)(?:,(?:\*|\d+))?\])?\Z")
_IDENTIFIER_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_HOSTNAME_PATTERN = re.compile(
    r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
    r"(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)*\Z"
)


class ConfigurationError(ValueError):
    """Raised when a supported configuration value is missing or invalid."""


@dataclass(frozen=True, slots=True)
class RuntimeConfig:
    """Configuration shared by all Python components."""

    environment: str
    log_level: str


@dataclass(frozen=True, slots=True)
class HDFSConfig:
    """Configuration for HDFS consumers."""

    uri: str
    base_path: str


@dataclass(frozen=True, slots=True)
class SparkConfig:
    """Configuration for Spark jobs."""

    master_url: str


@dataclass(frozen=True, slots=True)
class ClickHouseConfig:
    """Configuration for ClickHouse consumers."""

    host: str
    port: int
    database: str
    user: str
    password: str = field(repr=False)


@dataclass(frozen=True, slots=True)
class PathsConfig:
    """Configuration for local generated-artifact paths."""

    artifacts_dir: Path


def load_runtime_config(env: Mapping[str, str] | None = None) -> RuntimeConfig:
    """Load runtime settings from an injected mapping or the process environment."""
    settings = _settings(env)
    environment = _read(settings, "WAREHOUSE_ENV", default="local").strip().lower()
    if environment not in _ENVIRONMENTS:
        raise ConfigurationError("WAREHOUSE_ENV must be one of: local, test, or container.")

    log_level = _read(settings, "WAREHOUSE_LOG_LEVEL", default="INFO").strip().upper()
    if log_level not in _LOG_LEVELS:
        raise ConfigurationError(
            "WAREHOUSE_LOG_LEVEL must be one of: DEBUG, INFO, WARNING, ERROR, or CRITICAL."
        )

    return RuntimeConfig(environment=environment, log_level=log_level)


def load_hdfs_config(env: Mapping[str, str] | None = None) -> HDFSConfig:
    """Load and validate HDFS endpoint and base-path settings."""
    settings = _settings(env)
    uri = _read(settings, "WAREHOUSE_HDFS_URI")
    base_path = _read(settings, "WAREHOUSE_HDFS_BASE_PATH", default="/warehouse-bigdata")

    _validate_endpoint_url(uri, "WAREHOUSE_HDFS_URI", scheme="hdfs", require_port=False)
    _validate_hdfs_base_path(base_path)
    return HDFSConfig(uri=uri, base_path=base_path)


def load_spark_config(env: Mapping[str, str] | None = None) -> SparkConfig:
    """Load and validate the Spark master URL."""
    settings = _settings(env)
    master_url = _read(settings, "WAREHOUSE_SPARK_MASTER_URL")
    if not _LOCAL_MASTER_PATTERN.fullmatch(master_url):
        _validate_endpoint_url(
            master_url,
            "WAREHOUSE_SPARK_MASTER_URL",
            scheme="spark",
            require_port=True,
            allow_path=False,
        )
    return SparkConfig(master_url=master_url)


def load_clickhouse_config(env: Mapping[str, str] | None = None) -> ClickHouseConfig:
    """Load and validate ClickHouse connection fields without contacting the service."""
    settings = _settings(env)
    host = _read(settings, "WAREHOUSE_CLICKHOUSE_HOST")
    port_value = _read(settings, "WAREHOUSE_CLICKHOUSE_PORT")
    database = _read(settings, "WAREHOUSE_CLICKHOUSE_DATABASE")
    user = _read(settings, "WAREHOUSE_CLICKHOUSE_USER")
    password = _read(settings, "WAREHOUSE_CLICKHOUSE_PASSWORD", default="", allow_empty=True)

    _validate_host(host, "WAREHOUSE_CLICKHOUSE_HOST")
    port = _parse_port(port_value, "WAREHOUSE_CLICKHOUSE_PORT")
    if not _IDENTIFIER_PATTERN.fullmatch(database):
        raise ConfigurationError(
            "WAREHOUSE_CLICKHOUSE_DATABASE must be a non-empty ClickHouse identifier."
        )

    return ClickHouseConfig(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
    )


def load_paths_config(env: Mapping[str, str] | None = None) -> PathsConfig:
    """Load local artifact paths without resolving or creating them."""
    settings = _settings(env)
    artifacts_dir = _read(settings, "WAREHOUSE_ARTIFACTS_DIR", default="artifacts")
    _validate_local_path(artifacts_dir, "WAREHOUSE_ARTIFACTS_DIR")
    return PathsConfig(artifacts_dir=Path(artifacts_dir))


def _settings(env: Mapping[str, str] | None) -> Mapping[str, str]:
    return os.environ if env is None else env


def _read(
    settings: Mapping[str, str],
    variable: str,
    *,
    default: object = _MISSING,
    allow_empty: bool = False,
) -> str:
    if variable not in settings:
        if default is _MISSING:
            raise ConfigurationError(f"{variable} is required.")
        return default  # type: ignore[return-value]

    value = settings[variable]
    if not isinstance(value, str):
        raise ConfigurationError(f"{variable} must be a string.")
    if not allow_empty and not value.strip():
        raise ConfigurationError(f"{variable} must not be empty.")
    return value


def _validate_endpoint_url(
    value: str,
    variable: str,
    *,
    scheme: str,
    require_port: bool,
    allow_path: bool = True,
) -> None:
    if any(character.isspace() for character in value):
        raise ConfigurationError(f"{variable} must be a valid {scheme}://host[:port] URL.")

    try:
        parsed = urlsplit(value)
        hostname = parsed.hostname
        port = parsed.port
    except ValueError as error:
        raise ConfigurationError(
            f"{variable} must be a valid {scheme}://host[:port] URL."
        ) from error

    if parsed.scheme.lower() != scheme or not parsed.netloc or not hostname:
        raise ConfigurationError(f"{variable} must be a valid {scheme}://host[:port] URL.")
    if parsed.username is not None or parsed.password is not None or "@" in parsed.netloc:
        raise ConfigurationError(f"{variable} must not contain credentials.")
    if parsed.query or parsed.fragment:
        raise ConfigurationError(f"{variable} must not contain a query or fragment.")
    if parsed.netloc.endswith(":"):
        raise ConfigurationError(f"{variable} must use a valid port when a port is provided.")
    if not allow_path and parsed.path:
        raise ConfigurationError(f"{variable} must not contain a path.")
    if require_port and port is None:
        raise ConfigurationError(f"{variable} must include a decimal port from 1 through 65535.")
    if port is not None and not 1 <= port <= 65535:
        raise ConfigurationError(f"{variable} port must be from 1 through 65535.")


def _validate_hdfs_base_path(value: str) -> None:
    try:
        parsed = urlsplit(value)
        is_absolute = PurePosixPath(value).is_absolute()
    except ValueError as error:
        raise ConfigurationError(
            "WAREHOUSE_HDFS_BASE_PATH must be an absolute POSIX-style HDFS path."
        ) from error

    if (
        not is_absolute
        or value.startswith("//")
        or parsed.scheme
        or parsed.netloc
        or parsed.query
        or parsed.fragment
    ):
        raise ConfigurationError(
            "WAREHOUSE_HDFS_BASE_PATH must be an absolute POSIX-style HDFS path."
        )


def _validate_host(value: str, variable: str) -> None:
    if any(character.isspace() for character in value) or any(
        character in value for character in "/?#@"
    ):
        raise ConfigurationError(f"{variable} must be a hostname or IP address, not a URI.")

    if ":" in value:
        try:
            ip_address(value)
        except ValueError as error:
            raise ConfigurationError(f"{variable} must be a hostname or IP address.") from error
    elif not _HOSTNAME_PATTERN.fullmatch(value):
        raise ConfigurationError(f"{variable} must be a hostname or IP address.")


def _parse_port(value: str, variable: str) -> int:
    if not value.isdecimal():
        raise ConfigurationError(f"{variable} must be a decimal integer from 1 through 65535.")
    port = int(value)
    if not 1 <= port <= 65535:
        raise ConfigurationError(f"{variable} must be from 1 through 65535.")
    return port


def _validate_local_path(value: str, variable: str) -> None:
    try:
        parsed = urlsplit(value)
    except ValueError as error:
        raise ConfigurationError(f"{variable} must be a local filesystem path.") from error

    if parsed.scheme or parsed.netloc or parsed.query or parsed.fragment or value.startswith("//"):
        raise ConfigurationError(f"{variable} must be a local filesystem path, not a URI.")
