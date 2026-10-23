"""Fake-Docker launcher lifecycle and real, isolated ZIP packaging checks."""

import json
import os
import re
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path
from textwrap import dedent
from zipfile import ZipFile

import pytest

ROOT = Path(__file__).resolve().parents[2]
IMAGE = "warehouse-bigdata-spark-submit:3.5.7-python3.11.17"
OWNER_LABEL = "io.warehouse-bigdata.spark-submit.owner"


@pytest.fixture
def sandbox(tmp_path: Path) -> dict:
    binaries = tmp_path / "bin"
    binaries.mkdir()
    log = tmp_path / "docker.jsonl"
    state = tmp_path / "driver.json"
    ready = tmp_path / "run-started"
    stub = f"#!{sys.executable}\n" + dedent(
        """\
        import json
        import os
        import signal
        import sys
        import time
        from pathlib import Path

        args = sys.argv[1:]
        with open(os.environ["TEST_DOCKER_LOG"], "a") as stream:
            stream.write(json.dumps(args) + "\\n")
        state = Path(os.environ["TEST_DRIVER_STATE"])
        if args[:2] == ["image", "inspect"]:
            sys.exit(int(os.environ.get("TEST_IMAGE_STATUS", "0")))
        if args[0] == "compose":
            if "/opt/warehouse-spark/scratch.py" in args:
                sys.exit(int(os.environ.get("TEST_SCRATCH_STATUS", "0")))
            if "ready" in args:
                sys.exit(int(os.environ.get("TEST_READY_STATUS", "0")))
            if "run" not in args:
                sys.exit(0)
            if "warehouse_bigdata.common.hdfs_publication" in args:
                sys.exit(int(os.environ.get("TEST_PUBLICATION_STATUS", os.environ.get("TEST_RUN_STATUS", "0"))))
            if os.environ.get("TEST_DRIVER_ONLY_STATUS"):
                sys.exit(int(os.environ["TEST_DRIVER_ONLY_STATUS"]))
            name = args[args.index("--name") + 1]
            label = args[args.index("--label") + 1].split("=", 1)[1]
            late = os.environ.get("TEST_LATE_CREATE") == "1"
            if late:
                signal.signal(signal.SIGTERM, signal.SIG_IGN)
            Path(os.environ["TEST_RUN_STARTED"]).write_text(name)
            if late:
                time.sleep(0.3)
            state.write_text(json.dumps({"name": name, "owner": label}))
            if os.environ.get("TEST_BLOCK") == "1":
                while True:
                    time.sleep(0.1)
            if os.environ.get("TEST_AUTO_REMOVE", "1") == "1":
                state.unlink()
            sys.exit(int(os.environ.get("TEST_RUN_STATUS", "0")))
        if args[0] == "inspect":
            if not state.exists():
                sys.exit(1)
            data = json.loads(state.read_text())
            assert args[-1] == data["name"]
            print(os.environ.get("TEST_OWNER", data["owner"]))
        elif args[0] in {"logs", "stop", "rm"}:
            data = json.loads(state.read_text())
            assert args[-1] == data["name"]
            if args[0] == "rm" and not os.environ.get("TEST_CLEANUP_STATUS"):
                state.unlink()
            sys.exit(int(os.environ.get("TEST_CLEANUP_STATUS", "0")))
        else:
            raise AssertionError(args)
        """
    )
    docker = binaries / "docker"
    docker.write_text(stub)
    docker.chmod(0o755)
    return {
        "env": {
            **os.environ,
            "PATH": f"{binaries}:{os.defpath}",
            "TEST_DOCKER_LOG": str(log),
            "TEST_DRIVER_STATE": str(state),
            "TEST_RUN_STARTED": str(ready),
        },
        "cwd": tmp_path,
        "log": log,
        "ready": ready,
    }


def invoke(sandbox: dict, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["bash", str(ROOT / "scripts/spark-submit.sh"), *args],
        cwd=sandbox["cwd"],
        env=sandbox["env"],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )


def commands(sandbox: dict) -> list[list[str]]:
    log = sandbox["log"]
    return [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []


def runs(sandbox: dict) -> list[list[str]]:
    return [args for args in commands(sandbox) if args[0] == "compose" and "run" in args]


def test_m04_success_finalizes_as_owner_only_after_driver_success(sandbox: dict) -> None:
    args = ["--", "warehouse_bigdata/jobs/normalize_sales_daily.py", "--pipeline-run-id", "run-001"]
    result = invoke(sandbox, *args)
    assert result.returncode == 0, result.stderr
    calls = commands(sandbox)
    publication = [call for call in calls if "warehouse_bigdata.common.hdfs_publication" in call]
    assert len(publication) == 2
    assert publication[0][-3:] == ["--recover", "--pipeline-run-id", "run-001"]
    assert publication[1][-2:] == ["--pipeline-run-id", "run-001"]
    assert all(call[call.index("--user") + 1] == "1000:100" for call in publication)
    assert calls.index(publication[0]) < calls.index(runs(sandbox)[1]) < calls.index(publication[1])
    sandbox["env"]["TEST_RUN_STATUS"] = "7"
    result = invoke(sandbox, *args)
    assert result.returncode == 7
    assert (
        len(
            [
                call
                for call in commands(sandbox)
                if "warehouse_bigdata.common.hdfs_publication" in call
            ]
        )
        == 3
    )


def assert_cluster_untouched(sandbox: dict) -> None:
    for args in commands(sandbox):
        assert not {"up", "down", "build", "start", "prune", "hdfs-init", "--volumes"} & set(args)
        if args[0] in {"stop", "rm", "logs", "inspect"}:
            assert args[-1].startswith("warehouse-spark-submit-")
        if args[0] == "compose":
            assert args[1:3] == ["-f", str(ROOT / "infrastructure/compose.yaml")]
            assert not any(arg.startswith("hdfs-") for arg in args)


def test_external_cwd_attached_ephemeral_run_and_exact_forwarding(sandbox: dict) -> None:
    arguments = [
        "--conf",
        "spark.sql.shuffle.partitions=4",
        "--",
        "warehouse_bigdata/jobs/future.py",
        "--event-log-dir",
        "hdfs://hdfs-namenode:8020/data/warehouse/staging/events/run-1",
        "two words",
        "Tiếng Việt",
        "--help",
        "--conf",
        "spark.master=local[*]",
        "--",
        "$(id);*",
    ]
    result = invoke(sandbox, *arguments)
    assert result.returncode == 0, result.stderr
    calls = commands(sandbox)
    assert calls[0] == ["image", "inspect", IMAGE]
    assert calls[1] == [
        "compose",
        "-f",
        str(ROOT / "infrastructure/compose.yaml"),
        "--profile",
        "spark",
        "exec",
        "-T",
        "--interactive=false",
        "spark-master",
        "/opt/python/bin/python3.11",
        "/opt/warehouse-spark/healthcheck.py",
        "ready",
    ]
    run = runs(sandbox)[0]
    assert run[3:5] == ["--profile", "spark-submit"]
    assert run[5:12] == ["run", "--rm", "--no-deps", "-T", "--pull", "never", "--name"]
    assert "--use-aliases" not in run
    name = run[run.index("--name") + 1]
    assert re.fullmatch(r"warehouse-spark-submit-[a-f0-9]{32}", name)
    assert f"_WAREHOUSE_SPARK_DRIVER_HOST={name}" in run
    assert f"{OWNER_LABEL}={name.removeprefix('warehouse-spark-submit-')}" in run
    assert run[run.index("spark-submit", 5) + 1 :] == arguments
    assert_cluster_untouched(sandbox)


def test_failing_user_local_od_does_not_shadow_system_utility(sandbox: dict) -> None:
    user_binaries = sandbox["cwd"] / "user-bin"
    user_binaries.mkdir()
    od = user_binaries / "od"
    od.write_text(f"#!{sys.executable}\nraise SystemExit(127)\n")
    od.chmod(0o755)
    sandbox["env"]["PATH"] = f"{user_binaries}:{sandbox['env']['PATH']}"

    result = invoke(sandbox, "--", "job.py")

    assert result.returncode == 0, result.stderr
    submissions = runs(sandbox)
    assert len(submissions) == 1
    run = submissions[0]
    name = run[run.index("--name") + 1]
    assert re.fullmatch(r"warehouse-spark-submit-[a-f0-9]{32}", name)
    assert f"_WAREHOUSE_SPARK_DRIVER_HOST={name}" in run
    assert f"{OWNER_LABEL}={name.removeprefix('warehouse-spark-submit-')}" in run
    assert_cluster_untouched(sandbox)


def test_each_invocation_advertises_a_unique_driver_name(sandbox: dict) -> None:
    assert invoke(sandbox, "--", "job.py").returncode == 0
    assert invoke(sandbox, "--", "job.py").returncode == 0
    names = [args[args.index("--name") + 1] for args in runs(sandbox)]
    assert len(set(names)) == 2


@pytest.mark.parametrize(
    "args",
    [
        (),
        ("job.py",),
        ("--",),
        ("--master", "local[*]", "--", "job.py"),
        ("--conf", "--", "job.py"),
    ],
)
def test_invalid_host_arguments_do_not_reach_docker(sandbox: dict, args: tuple[str, ...]) -> None:
    assert invoke(sandbox, *args).returncode == 2
    assert not commands(sandbox)


def test_host_help_is_offline(sandbox: dict) -> None:
    result = invoke(sandbox, "--help")
    assert result.returncode == 0
    assert "-- APPLICATION.py" in result.stdout
    assert not commands(sandbox)


@pytest.mark.parametrize("failure", ["IMAGE", "READY", "SCRATCH"])
def test_missing_image_and_readiness_failure_never_start_a_driver(
    sandbox: dict, failure: str
) -> None:
    sandbox["env"][f"TEST_{failure}_STATUS"] = "17"
    result = invoke(sandbox, "--", "job.py")
    assert result.returncode == 17
    assert not runs(sandbox)
    if failure == "IMAGE":
        assert "build spark-submit" in result.stderr
    assert_cluster_untouched(sandbox)


def test_failed_submission_and_diagnostics_cleanup_keep_primary_status(sandbox: dict) -> None:
    sandbox["env"].update(TEST_RUN_STATUS="37", TEST_AUTO_REMOVE="0", TEST_CLEANUP_STATUS="19")
    result = invoke(sandbox, "--", "job.py")
    assert result.returncode == 37
    calls = commands(sandbox)
    assert any(
        args[0] == "logs" and args[1:5] == ["--tail", "100", "--timestamps", "--"] for args in calls
    )
    assert any(args[0] == "stop" for args in calls)
    assert any(args[0] == "rm" for args in calls)
    assert_cluster_untouched(sandbox)


def test_cleanup_refuses_a_foreign_ownership_token(sandbox: dict) -> None:
    sandbox["env"].update(TEST_RUN_STATUS="31", TEST_AUTO_REMOVE="0", TEST_OWNER="someone-else")
    assert invoke(sandbox, "--", "job.py").returncode == 31
    assert not any(args[0] in {"logs", "stop", "rm"} for args in commands(sandbox))


@pytest.mark.parametrize(
    "cancel_signal, expected, late",
    [(signal.SIGINT, 130, False), (signal.SIGTERM, 143, False), (signal.SIGTERM, 143, True)],
)
def test_cancellation_is_finite_and_cleans_only_owned_driver(
    sandbox: dict, cancel_signal: signal.Signals, expected: int, late: bool
) -> None:
    sandbox["env"].update(TEST_BLOCK="1", TEST_LATE_CREATE="1" if late else "0")
    process = subprocess.Popen(
        ["bash", str(ROOT / "scripts/spark-submit.sh"), "--", "job.py"],
        cwd=sandbox["cwd"],
        env=sandbox["env"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        deadline = time.monotonic() + 5
        while (
            not sandbox["ready"].exists() and process.poll() is None and time.monotonic() < deadline
        ):
            time.sleep(0.02)
        assert sandbox["ready"].exists(), "launcher never reached attached execution"
        process.send_signal(cancel_signal)
        _, error = process.communicate(timeout=12)
        assert process.returncode == expected, error
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.communicate()
    calls = commands(sandbox)
    name = runs(sandbox)[0][runs(sandbox)[0].index("--name") + 1]
    assert any(args[0] == "stop" and args[-1] == name for args in calls)
    assert any(args[0] == "rm" and args[-1] == name for args in calls)
    assert_cluster_untouched(sandbox)


def test_packaging_helper_creates_reproducible_importable_project_zip(tmp_path: Path) -> None:
    source = tmp_path / "src"
    package = source / "warehouse_bigdata"
    shutil.copytree(
        ROOT / "src" / "warehouse_bigdata",
        package,
        ignore=shutil.ignore_patterns(
            "__pycache__",
            ".pytest_cache",
            ".ruff_cache",
            ".mypy_cache",
            "*.pyc",
            "*.pyo",
        ),
    )
    for cache in ("__pycache__", ".pytest_cache", ".ruff_cache", ".mypy_cache"):
        (package / cache).mkdir(exist_ok=True)
        (package / cache / "ignored").write_text("cache")
    (package / "ignored.pyc").write_bytes(b"bytecode")
    (package / "ignored.pyo").write_bytes(b"bytecode")
    archives = [tmp_path / "first.zip", tmp_path / "second.zip"]
    for archive in archives:
        subprocess.run(
            [sys.executable, str(ROOT / "scripts/package-spark-app.py"), str(source), str(archive)],
            cwd=tmp_path,
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
    assert archives[0].read_bytes() == archives[1].read_bytes()
    with ZipFile(archives[0]) as bundle:
        names = bundle.namelist()
        assert names == sorted(names)
        assert all(name.startswith("warehouse_bigdata/") for name in names)
        assert not any("cache" in name or name.endswith((".pyc", ".pyo")) for name in names)
        assert {
            "warehouse_bigdata/jobs/__init__.py",
            "warehouse_bigdata/jobs/ingest_m5.py",
            "warehouse_bigdata/jobs/ingest_m5_raw.py",
            "warehouse_bigdata/jobs/profile_m5_source.py",
            "warehouse_bigdata/jobs/normalize_sales_daily.py",
            "warehouse_bigdata/processing/normalization_snapshot.py",
            "warehouse_bigdata/common/hdfs_paths.py",
            "warehouse_bigdata/ingestion/orchestration.py",
            "warehouse_bigdata/common/hdfs_runtime.py",
        } <= set(names)
    probe = subprocess.run(
        [
            sys.executable,
            "-I",
            "-S",
            "-c",
            dedent("""\
            import sys
            sys.path.insert(0, sys.argv[1])
            import warehouse_bigdata.cli
            from warehouse_bigdata.common import config, hdfs_paths, hdfs_runtime, spark_submit
            from warehouse_bigdata.ingestion import orchestration
            from warehouse_bigdata.jobs import (
                ingest_m5,
                ingest_m5_raw,
                normalize_sales_daily,
                profile_m5_source,
            )
            from warehouse_bigdata.processing import normalization_snapshot
            for module in (
                warehouse_bigdata,
                warehouse_bigdata.cli,
                warehouse_bigdata.jobs,
                config,
                hdfs_paths,
                hdfs_runtime,
                spark_submit,
                orchestration,
                ingest_m5,
                ingest_m5_raw,
                normalize_sales_daily,
                normalization_snapshot,
                profile_m5_source,
            ):
                assert module.__file__.startswith(sys.argv[1] + '/warehouse_bigdata/'), module.__file__
            assert config.load_hdfs_config({'WAREHOUSE_HDFS_URI': 'hdfs://hdfs-namenode:8020'}).base_path == '/data/warehouse'
            """),
            str(archives[0]),
        ],
        cwd=tmp_path,
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert probe.returncode == 0, probe.stderr
