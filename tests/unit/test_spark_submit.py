"""Submission validation and argv/exec contracts, without Spark or Docker."""

import errno
import os
import socket
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock
from zipfile import ZipFile

import pytest

from warehouse_bigdata.common import spark_submit as submission
from warehouse_bigdata.common.config import ConfigurationError, load_spark_config

ROOT = Path(__file__).resolve().parents[2]
ENV = {
    "WAREHOUSE_SPARK_MASTER_URL": "spark://spark-master:7077",
    "WAREHOUSE_HDFS_URI": "hdfs://hdfs-namenode:8020",
    "WAREHOUSE_HDFS_BASE_PATH": "/data/warehouse",
    "HADOOP_CONF_DIR": "/opt/hadoop/etc/hadoop",
    "_WAREHOUSE_SPARK_DRIVER_HOST": "warehouse-spark-submit-0123456789abcdef",
}


@pytest.fixture
def prepared(tmp_path: Path) -> tuple[submission.SparkSubmitConfig, str]:
    app_root = tmp_path / "src"
    application = "warehouse_bigdata/jobs/a job.py"
    app = app_root / application
    app.parent.mkdir(parents=True)
    app.write_text("raise SystemExit(0)\n")
    archive = tmp_path / "warehouse_bigdata.zip"
    with ZipFile(archive, "w") as bundle:
        bundle.writestr("warehouse_bigdata/__init__.py", "")
    hadoop = tmp_path / "hadoop"
    hadoop.mkdir()
    for name in ("core-site.xml", "hdfs-site.xml"):
        (hadoop / name).write_bytes((ROOT / "infrastructure/hadoop" / name).read_bytes())
    config = replace(
        submission.load_spark_submit_config(ENV),
        app_root=app_root,
        project_archive=archive,
        hadoop_conf_dir=hadoop,
    )
    return config, application


def test_configuration_loading_is_side_effect_free(monkeypatch: pytest.MonkeyPatch) -> None:
    unexpected = Mock(side_effect=AssertionError("configuration performed I/O"))
    monkeypatch.setattr(Path, "resolve", unexpected)
    monkeypatch.setattr(Path, "open", unexpected)
    monkeypatch.setattr(socket, "create_connection", unexpected)
    monkeypatch.setattr(os, "execvpe", unexpected)

    config = submission.load_spark_submit_config(ENV)

    assert config.spark.master_url == ENV["WAREHOUSE_SPARK_MASTER_URL"]
    assert config.hdfs.uri == ENV["WAREHOUSE_HDFS_URI"]
    assert config.driver_host == ENV["_WAREHOUSE_SPARK_DRIVER_HOST"]
    assert config.app_root == Path("/opt/warehouse-app/src")
    assert config.project_archive == Path("/opt/warehouse-app/warehouse_bigdata.zip")
    unexpected.assert_not_called()


@pytest.mark.parametrize("master", ["local", "local[*]", "local[2]", "local[2,3]"])
def test_submission_rejects_local_without_changing_generic_loader(master: str) -> None:
    env = {**ENV, "WAREHOUSE_SPARK_MASTER_URL": master}
    assert load_spark_config(env).master_url == master
    with pytest.raises(ConfigurationError, match="Standalone"):
        submission.load_spark_submit_config(env)


@pytest.mark.parametrize("missing", ["WAREHOUSE_SPARK_MASTER_URL", "WAREHOUSE_HDFS_URI"])
def test_submission_requires_explicit_endpoints(missing: str) -> None:
    env = {key: value for key, value in ENV.items() if key != missing}
    with pytest.raises(ConfigurationError, match=missing):
        submission.load_spark_submit_config(env)


@pytest.mark.parametrize("host", ["", "localhost", "0.0.0.0", "bad/name", "bad name", "x" * 64])
def test_driver_host_requires_internal_non_loopback_dns_name(host: str) -> None:
    with pytest.raises(ConfigurationError, match="_WAREHOUSE_SPARK_DRIVER_HOST"):
        submission.load_spark_submit_config({**ENV, "_WAREHOUSE_SPARK_DRIVER_HOST": host})


def test_driver_host_is_required() -> None:
    with pytest.raises(ConfigurationError, match="_WAREHOUSE_SPARK_DRIVER_HOST"):
        submission.load_spark_submit_config(
            {key: value for key, value in ENV.items() if key != "_WAREHOUSE_SPARK_DRIVER_HOST"}
        )


def test_standalone_client_argv_endpoints_python_and_resource_bounds(
    prepared: tuple[submission.SparkSubmitConfig, str],
) -> None:
    config, application = prepared
    app = submission.preflight_spark_submit(config, application)
    args = ["two words", "Tiếng Việt", "--conf", "spark.master=local[*]", "--", "$(touch x);*"]
    argv = submission.build_spark_submit_argv(
        config,
        app,
        args,
        ["spark.sql.shuffle.partitions=4", "spark.default.parallelism=2"],
    )

    assert argv[0] == "/opt/spark/bin/spark-submit"
    before = argv[1 : argv.index(str(app))]
    assert before[:4] == ["--master", "spark://spark-master:7077", "--deploy-mode", "client"]
    options = dict(zip(before[::2], before[1::2], strict=True))
    assert options["--driver-memory"] == "512m"
    assert options["--executor-memory"] == "512m"
    assert options["--executor-cores"] == "1"
    assert options["--total-executor-cores"] == "2"
    assert options["--py-files"] == str(config.project_archive)
    conf = dict(
        before[index + 1].split("=", 1)
        for index in range(0, len(before), 2)
        if before[index] == "--conf"
    )
    assert (
        conf.items()
        >= {
            "spark.driver.host": ENV["_WAREHOUSE_SPARK_DRIVER_HOST"],
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.driver.port": "7079",
            "spark.driver.blockManager.port": "7080",
            "spark.port.maxRetries": "0",
            "spark.pyspark.driver.python": "/opt/warehouse-app/.venv/bin/python",
            "spark.pyspark.python": "/opt/python/bin/python3.11",
            "spark.executorEnv.PYSPARK_PYTHON": "/opt/python/bin/python3.11",
            "spark.hadoop.fs.defaultFS": ENV["WAREHOUSE_HDFS_URI"],
            "spark.hadoop.dfs.client.use.datanode.hostname": "true",
            "spark.executorEnv.WAREHOUSE_HDFS_URI": ENV["WAREHOUSE_HDFS_URI"],
            "spark.executorEnv.WAREHOUSE_HDFS_BASE_PATH": "/data/warehouse",
            "spark.dynamicAllocation.enabled": "false",
            "spark.sql.shuffle.partitions": "4",
            "spark.default.parallelism": "2",
        }.items()
    )
    assert argv[argv.index(str(app)) :] == [str(app), *args]


@pytest.mark.parametrize("cores", ["1", "2"])
def test_bounded_cores_max_is_forwarded(
    prepared: tuple[submission.SparkSubmitConfig, str], cores: str
) -> None:
    config, application = prepared

    argv = submission.build_spark_submit_argv(
        config,
        submission.preflight_spark_submit(config, application),
        conf=[f"spark.cores.max={cores}"],
    )

    assert "--conf" in argv
    assert f"spark.cores.max={cores}" in argv


@pytest.mark.parametrize(
    "conf",
    [
        ["missing-equals"],
        ["=4"],
        ["spark.sql.=4"],
        ["spark.sql..shuffle=4"],
        ["spark.sql.shuffle.partitions="],
        ["spark.sql.shuffle.partitions=4\n--master local[*]"],
        ["spark.sql.shuffle.partitions=4", "spark.sql.shuffle.partitions=2"],
        ["spark.default.parallelism=0"],
        ["spark.cores.max=0"],
        ["spark.cores.max=3"],
        ["spark.cores.max=1.0"],
        ["spark.master=local[*]"],
        ["spark.submit.deployMode=cluster"],
        ["spark.driver.host=other"],
        ["spark.driver.memory=2g"],
        ["spark.executor.memory=2g"],
        ["spark.cores.max=20"],
        ["spark.dynamicAllocation.enabled=true"],
        ["spark.pyspark.python=/bin/python"],
        ["spark.executorEnv.PYTHONPATH=/other"],
        ["spark.hadoop.fs.defaultFS=file:///"],
        ["spark.jars.packages=org:artifact:1"],
        ["spark.submit.pyFiles=/other.zip"],
        ["spark.sql.warehouse.dir=file:///tmp"],
        ["spark.sql.extensions=some.Class"],
        ["spark.sql.catalog.custom=some.Class"],
        ["spark.sql.hive.metastore.jars=/other.jar"],
    ],
)
def test_malformed_duplicate_and_runtime_owned_conf_are_rejected(conf: list[str]) -> None:
    config = submission.load_spark_submit_config(ENV)
    with pytest.raises(ConfigurationError):
        submission.build_spark_submit_argv(config, Path("/unused/job.py"), [], conf)


def test_conf_value_is_not_shell_interpreted_or_split() -> None:
    value = "spark.sql.session.timeZone=two words;$(id)=Tiếng Việt"
    argv = submission.build_spark_submit_argv(
        submission.load_spark_submit_config(ENV), Path("/unused/job.py"), [], [value]
    )
    assert value in argv


@pytest.mark.parametrize(
    "application", ["/etc/job.py", "../outside.py", "missing.py", "", "bad\0.py"]
)
def test_preflight_rejects_invalid_application_paths(
    prepared: tuple[submission.SparkSubmitConfig, str], application: str
) -> None:
    config, _ = prepared
    with pytest.raises(ConfigurationError, match="application"):
        submission.preflight_spark_submit(config, application)


@pytest.mark.parametrize("kind", ["symlink", "directory", "not-python"])
def test_preflight_rejects_escaping_symlinks_and_non_python_files(
    prepared: tuple[submission.SparkSubmitConfig, str], tmp_path: Path, kind: str
) -> None:
    config, _ = prepared
    path = config.app_root / ("job.txt" if kind == "not-python" else "job.py")
    if kind == "symlink":
        outside = tmp_path / "outside.py"
        outside.write_text("pass\n")
        path.symlink_to(outside)
    elif kind == "directory":
        path.mkdir()
    else:
        path.write_text("pass\n")
    with pytest.raises(ConfigurationError, match="application"):
        submission.preflight_spark_submit(config, path.name)


@pytest.mark.parametrize(
    "broken",
    [
        "archive-missing",
        "archive-invalid",
        "xml-missing",
        "xml-invalid",
        "hdfs-mismatch",
        "datanode-addressing",
    ],
)
def test_preflight_checks_archive_and_existing_hadoop_xml(
    prepared: tuple[submission.SparkSubmitConfig, str], broken: str
) -> None:
    config, application = prepared
    if broken == "archive-missing":
        config.project_archive.unlink()
    elif broken == "archive-invalid":
        config.project_archive.write_text("not a ZIP")
    elif broken == "xml-missing":
        (config.hadoop_conf_dir / "core-site.xml").unlink()
    elif broken == "xml-invalid":
        (config.hadoop_conf_dir / "hdfs-site.xml").write_text("<broken>")
    elif broken == "hdfs-mismatch":
        path = config.hadoop_conf_dir / "core-site.xml"
        path.write_text(path.read_text().replace("hdfs://hdfs-namenode:8020", "file:///"))
    else:
        path = config.hadoop_conf_dir / "hdfs-site.xml"
        path.write_text(path.read_text().replace("true", "false"))
    with pytest.raises(ConfigurationError):
        submission.preflight_spark_submit(config, application)


class ReplacedProcess(BaseException):
    pass


def test_exec_replaces_process_and_inherits_environment(
    prepared: tuple[submission.SparkSubmitConfig, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    config, application = prepared
    monkeypatch.setattr(submission, "load_spark_submit_config", lambda env: config)
    execute = Mock(side_effect=ReplacedProcess)
    monkeypatch.setattr(os, "execvpe", execute)
    env = {**ENV, "CUSTOM_INHERITED": "two words", "PYSPARK_DRIVER_PYTHON": "incorrect"}
    with pytest.raises(ReplacedProcess):
        submission.run_spark_submit(application, ["--run-id", "a b"], [], env=env)
    executable, argv, child_env = execute.call_args.args
    assert executable == argv[0] == "/opt/spark/bin/spark-submit"
    assert argv[-3:] == [str(config.app_root / application), "--run-id", "a b"]
    assert child_env["CUSTOM_INHERITED"] == "two words"
    assert child_env["HADOOP_CONF_DIR"] == str(config.hadoop_conf_dir)
    assert child_env["PYSPARK_DRIVER_PYTHON"] == "/opt/warehouse-app/.venv/bin/python"
    assert child_env["PYSPARK_PYTHON"] == "/opt/python/bin/python3.11"
    assert env["PYSPARK_DRIVER_PYTHON"] == "incorrect"


@pytest.mark.parametrize(
    "number, expected", [(errno.ENOENT, 127), (errno.EACCES, 126), (errno.ENOEXEC, 126)]
)
def test_exec_errors_are_concise_and_have_shell_compatible_status(
    prepared: tuple[submission.SparkSubmitConfig, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    number: int,
    expected: int,
) -> None:
    config, application = prepared
    monkeypatch.setattr(submission, "load_spark_submit_config", lambda env: config)
    monkeypatch.setattr(os, "execvpe", Mock(side_effect=OSError(number, os.strerror(number))))
    assert submission.run_spark_submit(application, [], [], env=ENV) == expected
    error = capsys.readouterr().err
    assert "spark-submit" in error
    assert "Traceback" not in error
    assert len(error.splitlines()) == 1


def test_configuration_failure_never_executes(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    execute = Mock()
    monkeypatch.setattr(os, "execvpe", execute)
    assert submission.run_spark_submit("job.py", [], [], env={}) == 2
    execute.assert_not_called()
    assert "WAREHOUSE_SPARK_MASTER_URL" in capsys.readouterr().err
