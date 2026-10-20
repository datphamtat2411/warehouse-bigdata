"""Opt-in, real Spark ZSTD JNI checks; disposable local images, no cluster access."""

import json
import subprocess
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
NATIVE = "/opt/warehouse-spark/native"
pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def model() -> dict:
    result = subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(ROOT / "infrastructure/compose.yaml"),
            "--profile",
            "spark",
            "--profile",
            "spark-submit",
            "config",
            "--format",
            "json",
        ],
        capture_output=True,
        text=True,
        check=True,
        timeout=20,
    )
    return json.loads(result.stdout)["services"]


@pytest.mark.parametrize("service", ["spark-worker-1", "spark-submit"])
@pytest.mark.parametrize("control", ["native", "noexec", "precedence"])
def test_real_spark_zstd_codec_loads_only_from_executable_jvm_temp(
    model: dict, service: str, control: str
) -> None:
    config = model[service]
    driver = service == "spark-submit"
    bulk_mib = 96 if driver else 224
    assert config["read_only"] and config["user"] == "185:185"
    assert config["environment"]["JAVA_TOOL_OPTIONS"] == f"-Djava.io.tmpdir={NATIVE}"
    if not driver:
        assert config["environment"]["SPARK_DAEMON_JAVA_OPTS"] == (
            f"-Xms128m -Djava.io.tmpdir={NATIVE}"
        )
    expected_tmpfs = [
        f"/tmp:rw,nosuid,nodev,noexec,size={bulk_mib * 1024**2},mode=1777",
        f"{NATIVE}:rw,exec,nosuid,nodev,size=33554432,uid=185,gid=185,mode=0700",
    ]
    assert config["tmpfs"] == expected_tmpfs
    # Local images only: missing Docker/images are failures of this explicitly selected gate.
    subprocess.run(
        ["docker", "image", "inspect", config["image"]],
        capture_output=True,
        text=True,
        check=True,
        timeout=15,
    )
    name = f"warehouse-zstd-test-{uuid.uuid4().hex}"
    command = [
        "docker",
        "run",
        "--rm",
        "--pull",
        "never",
        "--name",
        name,
        "--hostname",
        name,
        "--platform",
        config["platform"],
        "--network",
        "none",
        "--read-only",
        "--user",
        config["user"],
        "--memory",
        "384m",
        "--memory-swap",
        "384m",
        "--cpus",
        "0.5",
        "--pids-limit",
        "128",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges",
        "--entrypoint",
        "/usr/bin/tini",
        "-i",
    ]
    for mount in config["tmpfs"]:
        command.extend(["--tmpfs", mount])
    env = {**config["environment"], "SPARK_LOCAL_IP": "127.0.0.1"}
    if control == "noexec":
        env["JAVA_TOOL_OPTIONS"] = "-Djava.io.tmpdir=/tmp"
    for key, value in env.items():
        command.extend(["--env", f"{key}={value}"])
    command.extend(
        [
            config["image"],
            "-g",
            "--",
            "/opt/python/bin/python3.11",
            "-",
            "--bulk-mib",
            str(bulk_mib),
            "--memory-mib",
            "384",
            "--cpus",
            "0.5",
        ]
    )
    if control == "precedence":
        command.extend(["--java-tmpdir", "/tmp"])
    try:
        result = subprocess.run(
            command,
            input=(ROOT / "scripts/check-spark-native.py").read_text(),
            capture_output=True,
            text=True,
            timeout=55,
            check=False,
        )
    finally:
        # Only this invocation-owned, volume-free container can be removed.
        cleanup = subprocess.run(
            ["docker", "rm", "--force", name],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        assert cleanup.returncode == 0 or "No such container" in cleanup.stderr, cleanup.stderr
    assert result.stdout, result.stderr
    report = json.loads(result.stdout)
    assert report["compile"]["exit_code"] == 0, report
    java = report["java"]
    assert not java["timed_out"], report
    assert report["runtime"]["uid"] == report["runtime"]["gid"] == 185
    assert report["runtime"]["memory_limit"] == 384 * 1024**2
    assert report["runtime"]["cpus"] == 0.5
    if control == "native":
        assert result.returncode == java["exit_code"] == 0, report
        assert report["ok"], report
        assert report["jvm"]["codec_round_trip"] == report["jvm"]["native_loaded"] == "true"
        assert report["jvm"]["java_tmpdir"] == NATIVE
        assert report["jvm"]["native_mappings"], report
        for mapping in report["jvm"]["native_mappings"]:
            assert "x" in mapping.split()[1] and f" {NATIVE}/libzstd-jni-" in mapping
    else:
        assert result.returncode == java["exit_code"] == 1, report
        assert not report["ok"]
        assert report["jvm"]["java_tmpdir"] == "/tmp"
        assert "UnsatisfiedLinkError: /tmp/libzstd-jni-" in java["stderr"]
        assert "failed to map segment from shared object" in java["stderr"]
    print(json.dumps({"service": service, "control": control, **report}, indent=2))
