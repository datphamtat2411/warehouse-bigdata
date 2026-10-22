"""Bounded, short-lived PySpark submission to the existing Standalone cluster."""

import os
import re
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from xml.etree import ElementTree
from zipfile import BadZipFile, ZipFile

from warehouse_bigdata.common.config import (
    ConfigurationError,
    HDFSConfig,
    SparkConfig,
    load_hdfs_config,
    load_spark_config,
)

_SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
_DRIVER_PYTHON = "/opt/warehouse-app/.venv/bin/python"
_EXECUTOR_PYTHON = "/opt/python/bin/python3.11"
_DRIVER_HOST = "_WAREHOUSE_SPARK_DRIVER_HOST"
_HOST_PATTERN = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z")
_CONF_PATTERN = re.compile(r"[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)+\Z")
_RESERVED_SQL_KEYS = frozenset(
    {
        "spark.sql.warehouse.dir",
        "spark.sql.extensions",
        "spark.sql.catalogImplementation",
        "spark.sql.defaultCatalog",
    }
)
_RESERVED_SQL_PREFIXES = ("spark.sql.catalog.", "spark.sql.hive.")


@dataclass(frozen=True, slots=True)
class SparkSubmitConfig:
    """Validated settings; construction does not inspect files or services."""

    spark: SparkConfig
    hdfs: HDFSConfig
    driver_host: str
    hadoop_conf_dir: Path
    app_root: Path = Path("/opt/warehouse-app/src")
    project_archive: Path = Path("/opt/warehouse-app/warehouse_bigdata.zip")


def load_spark_submit_config(env: Mapping[str, str] | None = None) -> SparkSubmitConfig:
    """Reuse shared endpoint validation, enforcing Standalone for submissions only."""
    settings = os.environ if env is None else env
    spark = load_spark_config(settings)
    if not spark.master_url.startswith("spark://"):
        raise ConfigurationError("Spark submission requires a spark:// Standalone master URL.")
    hdfs = load_hdfs_config(settings)
    host = settings.get(_DRIVER_HOST, "")
    if not isinstance(host, str) or not _HOST_PATTERN.fullmatch(host) or host == "localhost":
        raise ConfigurationError(
            f"{_DRIVER_HOST} must be the invocation's internal driver container DNS name."
        )
    hadoop_dir = settings.get("HADOOP_CONF_DIR", "/opt/hadoop/etc/hadoop")
    if (
        not isinstance(hadoop_dir, str)
        or "\0" in hadoop_dir
        or not PurePosixPath(hadoop_dir).is_absolute()
    ):
        raise ConfigurationError("HADOOP_CONF_DIR must be an absolute container path.")
    return SparkSubmitConfig(spark, hdfs, host, Path(hadoop_dir))


def _application_path(config: SparkSubmitConfig, application: str) -> Path:
    if not application or "\0" in application or PurePosixPath(application).is_absolute():
        raise ConfigurationError("The application must be a relative, contained .py file.")
    try:
        root = config.app_root.resolve(strict=True)
        path = (root / application).resolve(strict=True)
        if not path.is_relative_to(root) or path.suffix != ".py" or not path.is_file():
            raise ValueError("not a contained Python file")
    except (OSError, ValueError, RuntimeError) as error:
        raise ConfigurationError(
            "The application must be an existing .py file contained under /opt/warehouse-app/src."
        ) from error
    return path


def _hadoop_properties(path: Path) -> dict[str, str]:
    try:
        root = ElementTree.parse(path).getroot()
        if root.tag != "configuration":
            raise ValueError("expected a Hadoop configuration element")
        return {
            property_.findtext("name", "").strip(): property_.findtext("value", "").strip()
            for property_ in root.findall("property")
        }
    except (OSError, ValueError, ElementTree.ParseError) as error:
        raise ConfigurationError(f"Cannot read Hadoop configuration: {path}.") from error


def preflight_spark_submit(config: SparkSubmitConfig, application: str) -> Path:
    """Check baked application/archive and mounted XML without starting any work."""
    path = _application_path(config, application)
    try:
        with ZipFile(config.project_archive) as archive:
            if "warehouse_bigdata/__init__.py" not in archive.namelist():
                raise ValueError("project package is absent")
    except (OSError, ValueError, BadZipFile) as error:
        raise ConfigurationError(
            f"Cannot read the packaged project ZIP: {config.project_archive}. Rebuild the driver image."
        ) from error
    core = _hadoop_properties(config.hadoop_conf_dir / "core-site.xml")
    hdfs = _hadoop_properties(config.hadoop_conf_dir / "hdfs-site.xml")
    if core.get("fs.defaultFS") != config.hdfs.uri:
        raise ConfigurationError("Hadoop XML fs.defaultFS must match WAREHOUSE_HDFS_URI.")
    if hdfs.get("dfs.client.use.datanode.hostname", "").lower() != "true":
        raise ConfigurationError("Hadoop XML must retain hostname-based DataNode access.")
    return path


def _task_configuration(conf: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    result = []
    for item in conf:
        key, separator, value = item.partition("=")
        if (
            not separator
            or not _CONF_PATTERN.fullmatch(key)
            or not value.strip()
            or any(character in item for character in "\0\r\n")
        ):
            raise ConfigurationError("Each --conf must be a non-empty KEY=VALUE setting.")
        if key in seen:
            raise ConfigurationError(f"Duplicate --conf key: {key}.")
        allowed = key.startswith("spark.sql.") or key == "spark.default.parallelism"
        if key == "spark.cores.max":
            if value not in {"1", "2"}:
                raise ConfigurationError("spark.cores.max must be exactly 1 or 2.")
            allowed = True
        if not allowed or key in _RESERVED_SQL_KEYS or key.startswith(_RESERVED_SQL_PREFIXES):
            raise ConfigurationError(f"Unsupported or runtime-owned --conf key: {key}.")
        if key == "spark.default.parallelism" and (not value.isdecimal() or int(value) < 1):
            raise ConfigurationError("spark.default.parallelism must be a positive integer.")
        seen.add(key)
        result.extend(("--conf", item))
    return result


def build_spark_submit_argv(
    config: SparkSubmitConfig,
    application: Path,
    application_args: Sequence[str] = (),
    conf: Sequence[str] = (),
) -> list[str]:
    """Build shell-free argv, keeping all Spark options before the application."""
    task_options = _task_configuration(conf)
    runtime_conf = {
        "spark.driver.host": config.driver_host,
        "spark.driver.bindAddress": "0.0.0.0",
        "spark.driver.port": "7079",
        "spark.driver.blockManager.port": "7080",
        "spark.port.maxRetries": "0",
        "spark.pyspark.driver.python": _DRIVER_PYTHON,
        "spark.pyspark.python": _EXECUTOR_PYTHON,
        "spark.executorEnv.PYSPARK_PYTHON": _EXECUTOR_PYTHON,
        "spark.hadoop.fs.defaultFS": config.hdfs.uri,
        "spark.hadoop.dfs.client.use.datanode.hostname": "true",
        "spark.executorEnv.WAREHOUSE_HDFS_URI": config.hdfs.uri,
        "spark.executorEnv.WAREHOUSE_HDFS_BASE_PATH": config.hdfs.base_path,
        "spark.dynamicAllocation.enabled": "false",
        "spark.ui.enabled": "false",
    }
    argv = [
        _SPARK_SUBMIT,
        "--master",
        config.spark.master_url,
        "--deploy-mode",
        "client",
        "--driver-memory",
        "512m",
        "--executor-memory",
        "512m",
        "--executor-cores",
        "1",
        "--total-executor-cores",
        "2",
        "--py-files",
        str(config.project_archive),
    ]
    for key, value in runtime_conf.items():
        argv.extend(("--conf", f"{key}={value}"))
    return [*argv, *task_options, str(application), *application_args]


def run_spark_submit(
    application: str,
    application_args: Sequence[str] = (),
    conf: Sequence[str] = (),
    *,
    env: Mapping[str, str] | None = None,
) -> int:
    """Preflight then replace this process, inheriting output and submission status."""
    settings = dict(os.environ if env is None else env)
    try:
        config = load_spark_submit_config(settings)
        path = preflight_spark_submit(config, application)
        argv = build_spark_submit_argv(config, path, application_args, conf)
    except ConfigurationError as error:
        print(f"warehouse-bigdata spark-submit: {error}", file=sys.stderr)
        return 2
    settings.update(
        HADOOP_CONF_DIR=str(config.hadoop_conf_dir),
        PYSPARK_DRIVER_PYTHON=_DRIVER_PYTHON,
        PYSPARK_PYTHON=_EXECUTOR_PYTHON,
        WAREHOUSE_HDFS_URI=config.hdfs.uri,
        WAREHOUSE_HDFS_BASE_PATH=config.hdfs.base_path,
    )
    try:
        os.execvpe(_SPARK_SUBMIT, argv, settings)
    except FileNotFoundError:
        print(
            f"warehouse-bigdata spark-submit: executable not found: {_SPARK_SUBMIT}.",
            file=sys.stderr,
        )
        return 127
    except OSError as error:
        print(
            f"warehouse-bigdata spark-submit: cannot execute {_SPARK_SUBMIT}: {error.strerror}.",
            file=sys.stderr,
        )
        return 126
    return 0  # execvpe returns only when replaced by a test double.
