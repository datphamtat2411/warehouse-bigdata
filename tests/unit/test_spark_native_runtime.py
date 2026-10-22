"""Offline safety and evidence contracts for the streamed JNI diagnostic."""

import copy
import importlib.util
import json
import sys
import time
from pathlib import Path
from textwrap import dedent

import pytest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "spark_native_probe", ROOT / "scripts/check-spark-native.py"
)
assert SPEC is not None and SPEC.loader is not None
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)
NATIVE = "/opt/warehouse-spark/native"


def java_result(**changes: object) -> dict:
    result = {
        "exit_code": 0,
        "timed_out": False,
        "stderr": "",
        "stdout": (
            f"java_tmpdir={NATIVE}\njava_version=11.0.30\njava_user=spark\n"
            "codec_round_trip=true\nnative_loaded=true\n"
            f"native_mapping=1000-2000 r-xp 0000 00:01 42 {NATIVE}/libzstd-jni-test.so\n"
        ),
    }
    return {**result, **changes}


@pytest.fixture
def runtime() -> dict:
    return {
        "uid": 185,
        "gid": 185,
        "memory_limit": 384 * 1024**2,
        "cpus": 0.5,
        "mounts": {
            "/": {"options": ["ro"]},
            "/tmp": {
                "filesystem": "tmpfs",
                "options": ["rw", "nosuid", "nodev", "noexec"],
                "uid": 0,
                "gid": 0,
                "mode": "1777",
                "capacity": 224 * 1024**2,
                "used": 0,
            },
            NATIVE: {
                "filesystem": "tmpfs",
                "options": ["rw", "nosuid", "nodev"],
                "uid": 185,
                "gid": 185,
                "mode": "0700",
                "capacity": 32 * 1024**2,
                "used": 0,
            },
        },
    }


def test_mountinfo_uses_mount_flags_not_superblock_defaults() -> None:
    text = (
        "21 20 0:1 / /tmp rw,nosuid,nodev,noexec shared:1 - tmpfs tmpfs rw,size=229376k\n"
        f"22 20 0:2 / {NATIVE} rw,nosuid,nodev - tmpfs tmpfs rw,size=32768k,mode=700\n"
    )
    assert probe.parse_mountinfo(text, "/tmp") == ("tmpfs", {"rw", "nosuid", "nodev", "noexec"})
    assert probe.parse_mountinfo(text, NATIVE) == ("tmpfs", {"rw", "nosuid", "nodev"})
    with pytest.raises(ValueError, match="mount"):
        probe.parse_mountinfo(text, "/missing")


def test_private_exec_and_bulk_noexec_runtime_is_accepted(runtime: dict) -> None:
    probe.validate_runtime(runtime, bulk_mib=224, memory_mib=384, cpus=0.5)


@pytest.mark.parametrize(
    ("target", "field", "value"),
    [
        (NATIVE, "options", ["rw", "nosuid", "nodev", "noexec"]),
        (NATIVE, "options", ["rw", "nodev"]),
        (NATIVE, "filesystem", "ext4"),
        (NATIVE, "uid", 0),
        (NATIVE, "gid", 0),
        (NATIVE, "mode", "0777"),
        (NATIVE, "capacity", 64 * 1024**2),
        ("/tmp", "options", ["rw", "nosuid", "nodev"]),
        ("/tmp", "mode", "0777"),
        ("/tmp", "capacity", 256 * 1024**2),
        ("/", "options", ["rw"]),
    ],
)
def test_incorrect_mount_policy_is_rejected(
    runtime: dict, target: str, field: str, value: object
) -> None:
    runtime["mounts"][target][field] = value
    with pytest.raises(RuntimeError):
        probe.validate_runtime(runtime, bulk_mib=224, memory_mib=384, cpus=0.5)


@pytest.mark.parametrize(
    ("field", "value"), [("uid", 0), ("gid", 0), ("memory_limit", 512 * 1024**2), ("cpus", 1)]
)
def test_identity_and_cgroup_mismatches_are_rejected(
    runtime: dict, field: str, value: object
) -> None:
    runtime[field] = value
    with pytest.raises(RuntimeError):
        probe.validate_runtime(runtime, bulk_mib=224, memory_mib=384, cpus=0.5)


def test_java_requires_completed_codec_round_trip_and_executable_mapping() -> None:
    facts = probe.validate_java(java_result())
    assert facts["java_tmpdir"] == NATIVE
    assert facts["codec_round_trip"] == facts["native_loaded"] == "true"


@pytest.mark.parametrize(
    "replacement",
    [
        ("codec_round_trip=true", "codec_round_trip=false"),
        ("native_loaded=true", "native_loaded=false"),
        ("native_loaded=true\n", ""),
        (f"java_tmpdir={NATIVE}", "java_tmpdir=/tmp"),
        ("java_version=11.0.30", "java_version=17.0.1"),
        (" r-xp ", " r--p "),
        (f"{NATIVE}/libzstd", "/tmp/libzstd"),
    ],
)
def test_incomplete_or_mislocated_java_evidence_fails(replacement: tuple[str, str]) -> None:
    result = java_result()
    result["stdout"] = result["stdout"].replace(*replacement)
    with pytest.raises(RuntimeError):
        probe.validate_java(result)


@pytest.mark.parametrize("changes", [{"exit_code": 17}, {"timed_out": True}])
def test_success_output_never_masks_java_failure(changes: dict) -> None:
    with pytest.raises(RuntimeError):
        probe.validate_java(java_result(**changes))


def test_bounded_runner_preserves_nonzero_status_and_error() -> None:
    result = probe.run_bounded(
        [
            sys.executable,
            "-c",
            "import sys; print('success'); sys.stderr.write('failure'); sys.exit(17)",
        ],
        timeout=2,
    )
    assert result == {
        "exit_code": 17,
        "stdout": "success\n",
        "stderr": "failure",
        "timed_out": False,
    }


def test_deadline_kills_stalled_probe_and_its_descendant(tmp_path: Path) -> None:
    child_pid = tmp_path / "child.pid"
    code = dedent(
        f"""
        import os
        import signal
        import time
        from pathlib import Path

        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        pid = os.fork()
        if pid:
            Path({str(child_pid)!r}).write_text(str(pid))
        print("success", flush=True)
        time.sleep(60)
        """
    )
    started = time.monotonic()
    result = probe.run_bounded([sys.executable, "-c", code], timeout=0.5)
    assert time.monotonic() - started < 3
    assert result["timed_out"] and result["exit_code"] != 0
    assert "success" in result["stdout"]
    assert child_pid.exists(), "The test must exercise descendant cleanup"

    state = Path(f"/proc/{child_pid.read_text()}/stat")

    def process_state() -> str | None:
        try:
            return state.read_text().rsplit(") ", 1)[1].split()[0]
        except (FileNotFoundError, ProcessLookupError):
            return None

    status = process_state()
    deadline = time.monotonic() + 1
    while status not in (None, "Z") and time.monotonic() < deadline:
        time.sleep(0.01)
        status = process_state()

    # A fast reaper removes the zombie entirely; both states mean no live descendant.
    assert status in (None, "Z"), f"Descendant remained live with state {status!r}"


@pytest.mark.parametrize("failed_phase", ["compile", "java"])
def test_main_reports_failure_and_removes_only_owned_probe_files(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture,
    runtime: dict,
    failed_phase: str,
) -> None:
    sentinel = tmp_path / "retained.so"
    sentinel.write_bytes(b"unrelated")
    monkeypatch.setattr(probe, "PROBE_ROOT", tmp_path)
    monkeypatch.setattr(probe, "collect_runtime", lambda: copy.deepcopy(runtime))
    results = [
        {
            "exit_code": 19 if failed_phase == "compile" else 0,
            "timed_out": False,
            "stdout": "",
            "stderr": "compiler evidence",
        },
        java_result(exit_code=23, stderr="JNI evidence"),
    ]
    calls = []

    def run(argv: list[str], *, timeout: float) -> dict:
        calls.append((argv, timeout))
        if "-d" in argv:
            assert (Path(argv[argv.index("-d") + 1]) / "WarehouseZstdProbe.java").exists()
        return results.pop(0)

    monkeypatch.setattr(probe, "run_bounded", run)
    assert probe.main(["--bulk-mib", "224", "--memory-mib", "384", "--cpus", "0.5"]) == 1
    report = json.loads(capsys.readouterr().out)
    assert not report["ok"] and report["error"]
    assert report[failed_phase]["exit_code"] == (19 if failed_phase == "compile" else 23)
    assert len(calls) == (1 if failed_phase == "compile" else 2)
    assert all(0 < timeout <= 20 for _, timeout in calls)
    assert list(tmp_path.iterdir()) == [sentinel]
    assert sentinel.read_bytes() == b"unrelated"
