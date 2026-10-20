"""Allowlisted Spark operations must never mutate the existing HDFS deployment."""

import json
import os
import pty
import select
import shutil
import signal
import subprocess
import sys
import time
import tty
from pathlib import Path
from textwrap import dedent

import pytest

ROOT = Path(__file__).resolve().parents[2]
SERVICES = ["spark-master", "spark-worker-1", "spark-worker-2"]
PROBE_DEADLINES = {"hdfs-wait-ready": "45", "spark-wait-ready": "195", "spark-ready": "15"}


@pytest.fixture
def sandbox(tmp_path: Path) -> dict:
    binaries = tmp_path / "bin"
    binaries.mkdir()
    log = tmp_path / "docker.jsonl"
    process_log = tmp_path / "processes.jsonl"
    stub = f"#!{sys.executable}\n" + dedent(
        """\
        import json
        import os
        import signal
        import sys
        import time

        args = sys.argv[1:]
        with open(os.environ["TEST_DOCKER_LOG"], "a") as stream:
            stream.write(json.dumps(args) + "\\n")
        role = args[0]
        if "/opt/warehouse-hdfs/healthcheck.sh" in args:
            role = "hdfs-wait-ready"
        elif "/opt/warehouse-spark/healthcheck.py" in args:
            role = "spark-wait-ready" if "wait-ready" in args else "spark-ready"
        elif "ps" in args:
            role = "ps"
        elif "logs" in args:
            role = "logs"

        def record(event, **values):
            with open(os.environ["TEST_PROCESS_LOG"], "a") as stream:
                stream.write(json.dumps({
                    "event": event, "role": role, "pid": os.getpid(),
                    "pgid": os.getpgrp(), **values,
                }) + "\\n")

        record("invoked", stdin=os.readlink("/proc/self/fd/0"), tty=os.isatty(0))
        if args[0] == "info":
            print(os.environ.get("TEST_MEMORY", str(10 * 1024**3)))
        if os.environ.get("TEST_SMOKE") == "1":
            if args[0] == "inspect":
                print("[]")
            elif "ps" in args:
                print("container-" + args[-1])
            elif "exec" in args:
                source = sys.stdin.read()
                if "--bulk-mib" in args:
                    service = args[args.index("exec") + 2]
                    record("native", service=service, source=source)
                    print("native load success")
                    if os.environ.get("TEST_NATIVE_FAIL") == service:
                        sys.exit(37)
            sys.exit(0)
        failure = os.environ.get("TEST_FAIL", "")
        readiness_failed = failure == "spark-readiness" and "spark-master" in args and "wait-ready" in args
        if (failure and failure in args) or readiness_failed:
            sys.exit(17)
        probe = role in {"hdfs-wait-ready", "spark-wait-ready", "spark-ready"}
        if probe and os.environ.get("TEST_INSPECT_STDIN") == "1":
            record("stdin", data=os.read(0, 1).decode())
        mode = ""
        if role == os.environ.get("TEST_PROBE"):
            mode = os.environ["TEST_PROBE_MODE"]
        elif role in {"ps", "logs"}:
            mode = os.environ.get("TEST_DIAGNOSTIC_MODE", "")
        if mode in {"term", "ignore-term"}:
            signal.signal(signal.SIGTERM, signal.SIG_IGN if mode == "ignore-term" else signal.SIG_DFL)
            if os.fork() == 0:
                record("descendant")
                while True:
                    time.sleep(1)
        if probe:
            marker = "HDFS ready probe passed" if role.startswith("hdfs-") else "Spark ready probe passed"
            print(marker, flush=True)
        elif mode:
            print(f"{role} diagnostic started", flush=True)
        if mode == "fail":
            sys.exit(17 if probe else 29)
        if mode in {"term", "ignore-term"}:
            while True:
                time.sleep(1)
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
            "TEST_PROCESS_LOG": str(process_log),
        },
        "cwd": tmp_path,
        "log": log,
        "process_log": process_log,
    }


@pytest.fixture
def bounded_sandbox(sandbox: dict) -> dict:
    """Compress deadlines without replacing GNU timeout's signal/group behavior."""
    real_timeout = shutil.which("timeout", path=os.defpath)
    assert real_timeout is not None, "GNU timeout is required by the approved offline gate"
    timeout_log = sandbox["cwd"] / "timeouts.jsonl"
    wrapper = sandbox["cwd"] / "bin/timeout"
    wrapper.write_text(
        f"#!{sys.executable}\n"
        + dedent(
            """\
            import json
            import os
            import sys

            args = sys.argv[1:]
            with open(os.environ["TEST_TIMEOUT_LOG"], "a") as stream:
                stream.write(json.dumps({"pid": os.getpid(), "args": args}) + "\\n")
            duration = next(index for index, arg in enumerate(args) if not arg.startswith("-"))
            args[duration] = "0.5s"
            args[:duration] = [
                "--kill-after=0.2s" if arg == "--kill-after=2s" else arg
                for arg in args[:duration]
            ]
            os.execv(os.environ["TEST_REAL_TIMEOUT"], [os.environ["TEST_REAL_TIMEOUT"], *args])
            """
        )
    )
    wrapper.chmod(0o755)
    sandbox["env"].update(TEST_REAL_TIMEOUT=real_timeout, TEST_TIMEOUT_LOG=str(timeout_log))
    sandbox["timeout_log"] = timeout_log
    return sandbox


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


def records(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def live_fake_processes(sandbox: dict) -> set[int]:
    alive = set()
    for entry in records(sandbox["process_log"]):
        pid = entry["pid"]
        try:
            state = Path(f"/proc/{pid}/stat").read_text().rsplit(") ", 1)[1].split()[0]
        except FileNotFoundError:
            continue
        if state != "Z":
            alive.add(pid)
    return alive


def invoke_supervised(
    sandbox: dict, action: str, stdin: int = subprocess.DEVNULL
) -> subprocess.CompletedProcess:
    process = subprocess.Popen(
        ["bash", str(ROOT / "scripts/spark-cluster.sh"), action],
        cwd=sandbox["cwd"],
        env=sandbox["env"],
        stdin=stdin,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        output, error = process.communicate(timeout=5)
        deadline = time.monotonic() + 1
        while live_fake_processes(sandbox) and time.monotonic() < deadline:
            time.sleep(0.01)
        assert not live_fake_processes(sandbox), "readiness/diagnostics left live fake descendants"
        return subprocess.CompletedProcess(process.args, process.returncode, output, error)
    finally:
        # GNU timeout owns separate groups; clean those too if a regression hangs.
        groups = {process.pid}
        groups.update(entry["pid"] for entry in records(sandbox["timeout_log"]))
        groups.update(entry["pgid"] for entry in records(sandbox["process_log"]))
        for group in groups:
            try:
                os.killpg(group, signal.SIGKILL)
            except ProcessLookupError:
                pass
        process.communicate(timeout=2)


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


def assert_storage_safe(sandbox: dict, *, smoke: bool = False) -> None:
    for command in commands(sandbox):
        assert not {"down", "rm", "prune", "hdfs-init", "init", "--volumes", "-v"} & set(command)
        if command[0] != "compose":
            continue
        args = command[5:]
        if smoke and args[:2] == ["ps", "--quiet"]:
            assert len(args) == 3 and args[2] in SERVICES
        elif args[0] in {"up", "stop", "ps", "logs"}:
            assert args[-3:] == SERVICES
        hdfs = [arg for arg in args if arg.startswith("hdfs-")]
        if hdfs:
            assert args == [
                "exec",
                "-T",
                "--interactive=false",
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
        "--interactive=false",
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
        "--interactive=false",
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
                "--interactive=false",
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


def assert_readiness_and_diagnostic_bounds(sandbox: dict) -> None:
    for entry in records(sandbox["timeout_log"]):
        args = entry["args"]
        boundary = args.index("docker")
        command = args[boundary + 1 :]
        if "/opt/warehouse-hdfs/healthcheck.sh" in command:
            deadline = PROBE_DEADLINES["hdfs-wait-ready"]
        elif "/opt/warehouse-spark/healthcheck.py" in command:
            probe = "spark-wait-ready" if "wait-ready" in command else "spark-ready"
            deadline = PROBE_DEADLINES[probe]
        elif "ps" in command or "logs" in command:
            deadline = "12"
        else:
            continue
        assert args[:boundary] == ["--kill-after=2s", deadline]
        if "exec" in command:
            assert command[5:8] == ["exec", "-T", "--interactive=false"]


def assert_descendant_group(sandbox: dict, role: str) -> None:
    events = records(sandbox["process_log"])
    parent = next(
        entry for entry in events if entry["role"] == role and entry["event"] == "invoked"
    )
    children = [
        entry for entry in events if entry["role"] == role and entry["event"] == "descendant"
    ]
    assert len(children) == 1, "the stalled fake must exercise descendant termination"
    assert children[0]["pid"] != parent["pid"]
    assert children[0]["pgid"] == parent["pgid"]


@pytest.mark.parametrize("probe", PROBE_DEADLINES)
@pytest.mark.parametrize(("mode", "status"), [("fail", 17), ("term", 124), ("ignore-term", 137)])
def test_success_marker_does_not_mask_readiness_failure_or_stall(
    bounded_sandbox: dict, probe: str, mode: str, status: int
) -> None:
    sandbox = bounded_sandbox
    sandbox["env"].update(TEST_PROBE=probe, TEST_PROBE_MODE=mode)
    result = invoke_supervised(sandbox, "check" if probe == "spark-ready" else "up")
    assert result.returncode == status, result.stderr
    marker = "HDFS ready probe passed" if probe == "hdfs-wait-ready" else "Spark ready probe passed"
    assert marker in result.stdout
    steps = compose_commands(sandbox)
    assert steps[-2:] == [
        ["ps", "--all", *SERVICES],
        ["logs", "--no-color", "--tail", "100", *SERVICES],
    ]
    assert sum(step[0] == "up" for step in steps) == (probe == "spark-wait-ready")
    probes = [
        entry["role"] for entry in records(sandbox["process_log"]) if entry["event"] == "invoked"
    ]
    assert probes.count(probe) == 1
    if probe == "hdfs-wait-ready":
        assert "spark-wait-ready" not in probes
    if mode != "fail":
        assert_descendant_group(sandbox, probe)
    assert_readiness_and_diagnostic_bounds(sandbox)
    assert_storage_safe(sandbox)


@pytest.mark.parametrize("action", ["up", "check"])
@pytest.mark.parametrize("input_kind", ["pipe", "terminal"])
@pytest.mark.parametrize("payload", [b"", b"caller-only input\n"], ids=["empty", "pending"])
def test_readiness_isolates_open_caller_input_only_at_probe_boundaries(
    bounded_sandbox: dict, action: str, input_kind: str, payload: bytes
) -> None:
    sandbox = bounded_sandbox
    sandbox["env"]["TEST_INSPECT_STDIN"] = "1"
    if input_kind == "pipe":
        input_fd, writer_fd = os.pipe()
    else:
        writer_fd, input_fd = pty.openpty()
        tty.setraw(input_fd)
    try:
        os.write(writer_fd, payload)
        result = invoke_supervised(sandbox, action, stdin=input_fd)
        assert result.returncode == 0, result.stderr
        if payload:
            assert select.select([input_fd], [], [], 0)[0], "readiness consumed the caller's input"
            assert os.read(input_fd, len(payload)) == payload
    finally:
        os.close(input_fd)
        os.close(writer_fd)
    events = records(sandbox["process_log"])
    probes = [entry for entry in events if entry["event"] == "stdin"]
    assert [entry["role"] for entry in probes] == (
        ["hdfs-wait-ready", "spark-wait-ready"] if action == "up" else ["spark-ready"]
    )
    assert all(entry["data"] == "" for entry in probes), "probe input must return immediate EOF"
    for entry in events:
        if entry["event"] != "invoked":
            continue
        if entry["role"] in PROBE_DEADLINES:
            assert entry["stdin"] == "/dev/null"
            assert not entry["tty"]
        else:
            assert entry["stdin"] != "/dev/null", "only readiness should redirect STDIN"
            assert entry["tty"] == (input_kind == "terminal")
    assert_readiness_and_diagnostic_bounds(sandbox)
    assert_storage_safe(sandbox)


@pytest.mark.parametrize(("mode", "status"), [("fail", 17), ("term", 124), ("ignore-term", 137)])
@pytest.mark.parametrize("diagnostic_mode", ["fail", "ignore-term"])
def test_failing_or_term_resistant_diagnostics_preserve_original_status(
    bounded_sandbox: dict, mode: str, status: int, diagnostic_mode: str
) -> None:
    sandbox = bounded_sandbox
    sandbox["env"].update(
        TEST_PROBE="spark-ready", TEST_PROBE_MODE=mode, TEST_DIAGNOSTIC_MODE=diagnostic_mode
    )
    result = invoke_supervised(sandbox, "check")
    assert result.returncode == status, result.stderr
    assert "Spark ready probe passed" in result.stdout
    assert [step[0] for step in compose_commands(sandbox)] == ["exec", "ps", "logs"]
    assert result.stderr.count("ps diagnostic started") == 1
    assert result.stderr.count("logs diagnostic started") == 1
    if mode != "fail":
        assert_descendant_group(sandbox, "spark-ready")
    if diagnostic_mode == "ignore-term":
        for role in ("ps", "logs"):
            assert_descendant_group(sandbox, role)
    assert_readiness_and_diagnostic_bounds(sandbox)
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


def test_compose_keeps_private_exec_within_existing_tmpfs_budgets() -> None:
    source = (ROOT / "infrastructure/compose.yaml").read_text()
    master = source.split("  spark-master:\n", 1)[1].split("  spark-worker-1:\n", 1)[0]
    worker1 = source.split("  spark-worker-1:\n", 1)[1].split("  spark-worker-2:\n", 1)[0]
    worker2 = source.split("  spark-worker-2:\n", 1)[1].split("  spark-submit:\n", 1)[0]
    driver = source.split("  spark-submit:\n", 1)[1].split("\nnetworks:", 1)[0]
    native = "/opt/warehouse-spark/native"
    mount = f"{native}:rw,exec,nosuid,nodev,size=33554432,uid=185,gid=185,mode=0700"
    assert "/tmp:rw,nosuid,nodev,noexec,size=67108864,mode=1777" in master
    assert native not in master
    assert "/tmp:rw,nosuid,nodev,noexec,size=234881024,mode=1777" in worker1
    assert mount in worker1 and "tmpfs: *spark-worker-scratch" in worker2
    assert "/tmp:rw,nosuid,nodev,noexec,size=100663296,mode=1777" in driver
    assert mount in driver
    for worker in (worker1, worker2):
        assert f"JAVA_TOOL_OPTIONS: -Djava.io.tmpdir={native}" in worker
        assert f"SPARK_DAEMON_JAVA_OPTS: -Xms128m -Djava.io.tmpdir={native}" in worker
        assert "SPARK_WORKER_HOST:" in worker and "SPARK_WORKER_OPTS:" in worker
    assert f"JAVA_TOOL_OPTIONS: -Djava.io.tmpdir={native}" in driver
    assert "WAREHOUSE_HDFS_BASE_PATH: /data/warehouse" in driver


@pytest.mark.parametrize("failed_worker", ["", "spark-worker-1", "spark-worker-2"])
def test_smoke_requires_independent_bounded_jni_checks_on_both_workers(
    sandbox: dict, failed_worker: str
) -> None:
    sandbox["env"].update(TEST_SMOKE="1", TEST_NATIVE_FAIL=failed_worker)
    result = subprocess.run(
        ["bash", str(ROOT / "scripts/smoke-spark.sh")],
        cwd=sandbox["cwd"],
        env=sandbox["env"],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == (37 if failed_worker else 0), result.stderr
    native = [entry for entry in records(sandbox["process_log"]) if entry["event"] == "native"]
    assert [entry["service"] for entry in native] == (
        ["spark-worker-1"] if failed_worker == "spark-worker-1" else SERVICES[1:]
    )
    helper = (ROOT / "scripts/check-spark-native.py").read_text()
    assert all(entry["source"] == helper for entry in native)
    for command in commands(sandbox):
        assert not {"up", "run", "build", "start", "spark-submit"} & set(command)
        if "--bulk-mib" in command:
            assert command[-6:] == ["--bulk-mib", "224", "--memory-mib", "2048", "--cpus", "1"]
    assert_storage_safe(sandbox, smoke=True)
    smoke = (ROOT / "scripts/smoke-spark.sh").read_text()
    assert 'timeout --kill-after=2s 45 "${compose[@]}" exec -T' in smoke
    assert '"noexec"' in smoke and "SPARK_DAEMON_JAVA_OPTS" in smoke
    assert 'temporary = "/tmp" if master else native' in smoke
    assert "/opt/warehouse-spark/native" in smoke
