"""Allowlisted Spark operations must never mutate the existing HDFS deployment."""

import json
import os
import subprocess
import sys
from pathlib import Path
from textwrap import dedent

import pytest

ROOT = Path(__file__).resolve().parents[2]
SERVICES = ["spark-master", "spark-worker-1", "spark-worker-2"]


@pytest.fixture
def sandbox(tmp_path: Path) -> dict:
    binaries = tmp_path / "bin"
    binaries.mkdir()
    log = tmp_path / "docker.jsonl"
    stub = f"#!{sys.executable}\n" + dedent(
        """\
        import json
        import os
        import sys

        args = sys.argv[1:]
        with open(os.environ["TEST_DOCKER_LOG"], "a") as stream:
            stream.write(json.dumps(args) + "\\n")
        if args[0] == "info":
            print(os.environ.get("TEST_MEMORY", str(10 * 1024**3)))
        failure = os.environ.get("TEST_FAIL", "")
        readiness_failed = failure == "spark-readiness" and "spark-master" in args and "wait-ready" in args
        if (failure and failure in args) or readiness_failed:
            sys.exit(17)
        """
    )
    docker = binaries / "docker"
    docker.write_text(stub)
    docker.chmod(0o755)
    return {
        "env": {
            **os.environ,
            "PATH": f"{binaries}:{os.environ['PATH']}",
            "TEST_DOCKER_LOG": str(log),
        },
        "cwd": tmp_path,
        "log": log,
    }


def invoke(sandbox: dict, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["bash", str(ROOT / "scripts/spark-cluster.sh"), *args],
        cwd=sandbox["cwd"],
        env=sandbox["env"],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )


def commands(sandbox: dict) -> list[list[str]]:
    log = sandbox["log"]
    return [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []


def compose_commands(sandbox: dict) -> list[list[str]]:
    result = []
    for command in commands(sandbox):
        if command[0] == "compose":
            assert command[1:5] == [
                "-f",
                str(ROOT / "infrastructure/compose.yaml"),
                "--profile",
                "spark",
            ]
            result.append(command[5:])
    return result


def assert_storage_safe(sandbox: dict) -> None:
    for command in commands(sandbox):
        assert not {"down", "rm", "prune", "hdfs-init", "init", "--volumes", "-v"} & set(command)
        if command[0] != "compose":
            continue
        args = command[5:]
        if args[0] in {"up", "stop", "ps", "logs"}:
            assert args[-3:] == SERVICES
        hdfs = [arg for arg in args if arg.startswith("hdfs-")]
        if hdfs:
            assert args == [
                "exec",
                "-T",
                "hdfs-namenode",
                "bash",
                "/opt/warehouse-hdfs/healthcheck.sh",
                "wait-ready",
                "30",
            ]


def test_up_only_starts_spark_after_memory_and_existing_hdfs_checks(sandbox: dict) -> None:
    result = invoke(sandbox, "up")
    assert result.returncode == 0, result.stderr
    docker = commands(sandbox)
    assert docker[0] == ["info", "--format", "{{.MemTotal}}"]
    steps = compose_commands(sandbox)
    assert steps[0] == [
        "exec",
        "-T",
        "hdfs-namenode",
        "bash",
        "/opt/warehouse-hdfs/healthcheck.sh",
        "wait-ready",
        "30",
    ]
    assert steps[1] == ["up", "--detach", "--build", "--no-deps", *SERVICES]
    assert steps[2] == [
        "exec",
        "-T",
        "spark-master",
        "/opt/python/bin/python3.11",
        "/opt/warehouse-spark/healthcheck.py",
        "wait-ready",
        "180",
    ]
    assert_storage_safe(sandbox)


@pytest.mark.parametrize("memory", [str(7269990400), "", "unavailable"])
def test_insufficient_or_unknown_memory_blocks_startup(sandbox: dict, memory: str) -> None:
    sandbox["env"]["TEST_MEMORY"] = memory
    result = invoke(sandbox, "up")
    assert result.returncode != 0
    assert "planned 10 gib budget" in result.stderr.lower()
    assert "not a measured minimum" in result.stderr.lower()
    assert not any("up" in command for command in commands(sandbox))
    assert_storage_safe(sandbox)


@pytest.mark.parametrize("failure", ["info", "hdfs-namenode", "up", "spark-readiness"])
def test_up_propagates_failure_and_prints_bounded_spark_diagnostics(
    sandbox: dict, failure: str
) -> None:
    sandbox["env"]["TEST_FAIL"] = failure
    result = invoke(sandbox, "up")
    assert result.returncode == 17
    steps = compose_commands(sandbox)
    assert ["ps", "--all", *SERVICES] in steps
    assert ["logs", "--no-color", "--tail", "100", *SERVICES] in steps
    if failure in {"info", "hdfs-namenode"}:
        assert not any(step[0] == "up" for step in steps)
    if failure == "spark-readiness":
        assert any(step[0] == "up" for step in steps)
    assert_storage_safe(sandbox)


@pytest.mark.parametrize(
    ("action", "expected"),
    [
        ("stop", ["stop", "--timeout", "60", *SERVICES]),
        ("status", ["ps", "--all", *SERVICES]),
        ("logs", ["logs", "--no-color", "--tail", "100", *SERVICES]),
        (
            "check",
            [
                "exec",
                "-T",
                "spark-master",
                "/opt/python/bin/python3.11",
                "/opt/warehouse-spark/healthcheck.py",
                "ready",
            ],
        ),
    ],
)
def test_allowlisted_commands_are_repository_relative_and_spark_only(
    sandbox: dict, action: str, expected: list[str]
) -> None:
    result = invoke(sandbox, action)
    assert result.returncode == 0, result.stderr
    assert compose_commands(sandbox) == [expected]
    assert_storage_safe(sandbox)


@pytest.mark.parametrize("args", [(), ("down",), ("restart",), ("stop", "hdfs-namenode")])
def test_unknown_actions_and_extra_arguments_never_reach_docker(sandbox: dict, args: tuple) -> None:
    assert invoke(sandbox, *args).returncode != 0
    assert not commands(sandbox)


@pytest.mark.parametrize("action", ["stop", "check"])
def test_operational_failure_is_not_hidden_by_diagnostics(sandbox: dict, action: str) -> None:
    sandbox["env"]["TEST_FAIL"] = "stop" if action == "stop" else "ready"
    assert invoke(sandbox, action).returncode == 17
    assert_storage_safe(sandbox)


@pytest.mark.parametrize("failure", ["memory", "ready", "arguments"])
def test_smoke_precondition_failures_never_start_services_or_submit_work(
    sandbox: dict, failure: str
) -> None:
    arguments = ["unexpected"] if failure == "arguments" else []
    if failure == "memory":
        sandbox["env"]["TEST_MEMORY"] = str(7269990400)
    elif failure == "ready":
        sandbox["env"]["TEST_FAIL"] = "ready"
    result = subprocess.run(
        ["bash", str(ROOT / "scripts/smoke-spark.sh"), *arguments],
        cwd=sandbox["cwd"],
        env=sandbox["env"],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode != 0
    if failure == "arguments":
        assert not commands(sandbox)
    if failure == "ready":
        assert result.returncode == 17
    for command in commands(sandbox):
        assert not {"up", "run", "build", "start", "spark-submit"} & set(command)
    assert_storage_safe(sandbox)
