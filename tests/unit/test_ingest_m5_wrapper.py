"""Offline checks for the host-side M5 runtime wrapper."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "ingest-m5.sh"
SOURCE_URL = "https://www.kaggle.com/competitions/m5-forecasting-accuracy/data"
TIMESTAMP = "2026-10-21T12:34:56Z"


def _fake_runtime(tmp_path: Path) -> tuple[Path, Path, Path]:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    docker_log = tmp_path / "docker.log"
    uv_log = tmp_path / "uv.log"
    fake_docker = fake_bin / "docker"
    fake_docker.write_text(
        "#!/usr/bin/env bash\n"
        "set -eu\n"
        "printf '%s\\n' \"$*\" >> \"$FAKE_DOCKER_LOG\"\n"
        "case \"${1:-}\" in\n"
        "  info) exit 0 ;;\n"
        "  image|network) exit 0 ;;\n"
        "  create)\n"
        "    name= label=\n"
        "    while [[ $# -gt 0 ]]; do\n"
        "      case \"$1\" in\n"
        "        --name) name=$2; shift 2 ;;\n"
        "        --label) label=$2; shift 2 ;;\n"
        "        *) shift ;;\n"
        "      esac\n"
        "    done\n"
        "    printf '%s\\n%s\\n' \"$name\" \"$label\" > \"$FAKE_CLIENT_STATE\"\n"
        "    [[ \"${FAKE_CREATE_FAILURE:-0}\" == 1 ]] && exit 1\n"
        "    exit 0\n"
        "    ;;\n"
        "  inspect)\n"
        "    if [[ \"$*\" == *--format* ]]; then\n"
        "      mapfile -t state < \"$FAKE_CLIENT_STATE\"\n"
        "      printf '/%s|%s\\n' \"${state[0]}\" \"${state[1]#*=}\"\n"
        "    fi\n"
        "    exit 0\n"
        "    ;;\n"
        "  start|rm) exit 0 ;;\n"
        "  *) exit 0 ;;\n"
        "esac\n",
        encoding="utf-8",
    )
    fake_docker.chmod(0o755)
    fake_uv = fake_bin / "uv"
    fake_uv.write_text(
        "#!/usr/bin/env bash\n"
        "set -eu\n"
        "printf '%s\\n' \"$*\" >> \"$FAKE_UV_LOG\"\n",
        encoding="utf-8",
    )
    fake_uv.chmod(0o755)
    return fake_bin, docker_log, uv_log


def _environment(
    fake_bin: Path,
    docker_log: Path,
    uv_log: Path,
    state: Path,
    scratch_dir: Path,
) -> dict[str, str]:
    return {
        **os.environ,
        "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        "FAKE_DOCKER_LOG": str(docker_log),
        "FAKE_UV_LOG": str(uv_log),
        "FAKE_CLIENT_STATE": str(state),
        "WAREHOUSE_ARTIFACTS_DIR": str(scratch_dir.parent / "artifacts"),
        "WAREHOUSE_SCRATCH_DIR": str(scratch_dir),
        "WAREHOUSE_HDFS_URI": "hdfs://hdfs-namenode:8020",
    }


def test_wrapper_creates_owned_client_forwards_arguments_and_cleans_it(tmp_path: Path) -> None:
    fake_bin, docker_log, uv_log = _fake_runtime(tmp_path)
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    scratch_dir = tmp_path / "scratch"
    environment = _environment(
        fake_bin,
        docker_log,
        uv_log,
        tmp_path / "client-state",
        scratch_dir,
    )

    result = subprocess.run(
        [
            "/bin/bash",
            str(SCRIPT),
            "--source-dir",
            str(source_dir),
            "--source-url",
            SOURCE_URL,
            "--downloaded-at-utc",
            TIMESTAMP,
            "--pipeline-run-id",
            "run-wrapper",
        ],
        capture_output=True,
        text=True,
        check=False,
        env=environment,
    )

    assert result.returncode == 0, result.stderr
    docker_invocations = docker_log.read_text(encoding="utf-8").splitlines()
    assert any(" create " in f" {line} " for line in docker_invocations)
    assert any("--label io.warehouse-bigdata.hdfs-cli.owner=" in line for line in docker_invocations)
    assert not any("compose" in line or " run " in f" {line} " for line in docker_invocations)
    assert any(" rm --force warehouse-hdfs-cli-" in f" {line} " for line in docker_invocations)

    invocation = uv_log.read_text(encoding="utf-8")
    assert "run --locked warehouse-bigdata ingest-m5" in invocation
    assert f"--source-dir {source_dir}" in invocation
    assert f"--source-url {SOURCE_URL}" in invocation
    assert f"--downloaded-at-utc {TIMESTAMP}" in invocation
    assert "--pipeline-run-id run-wrapper" in invocation
    assert f"WAREHOUSE_SCRATCH_DIR={scratch_dir}" in invocation or scratch_dir.is_dir()


def test_wrapper_omits_unknown_download_time(tmp_path: Path) -> None:
    fake_bin, docker_log, uv_log = _fake_runtime(tmp_path)
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    environment = _environment(
        fake_bin,
        docker_log,
        uv_log,
        tmp_path / "client-state",
        tmp_path / "scratch",
    )

    result = subprocess.run(
        [
            "/bin/bash",
            str(SCRIPT),
            "--source-dir",
            str(source_dir),
            "--source-url",
            SOURCE_URL,
        ],
        capture_output=True,
        text=True,
        check=False,
        env=environment,
    )

    assert result.returncode == 0, result.stderr
    assert "--downloaded-at-utc" not in uv_log.read_text(encoding="utf-8")


def test_wrapper_reports_missing_host_prerequisite(tmp_path: Path) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    realpath = fake_bin / "realpath"
    realpath.symlink_to("/usr/bin/realpath")
    source_dir = tmp_path / "source"
    source_dir.mkdir()

    result = subprocess.run(
        [
            "/bin/bash",
            str(SCRIPT),
            "--source-dir",
            str(source_dir),
            "--source-url",
            SOURCE_URL,
        ],
        capture_output=True,
        text=True,
        check=False,
        env={"PATH": str(fake_bin)},
    )

    assert result.returncode == 127
    assert "docker" in result.stderr


def test_wrapper_cleans_a_client_left_by_a_failed_create_request(tmp_path: Path) -> None:
    fake_bin, docker_log, uv_log = _fake_runtime(tmp_path)
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    environment = _environment(
        fake_bin,
        docker_log,
        uv_log,
        tmp_path / "client-state",
        tmp_path / "scratch",
    )
    environment["FAKE_CREATE_FAILURE"] = "1"

    result = subprocess.run(
        [
            "/bin/bash",
            str(SCRIPT),
            "--source-dir",
            str(source_dir),
            "--source-url",
            SOURCE_URL,
        ],
        capture_output=True,
        text=True,
        check=False,
        env=environment,
    )

    assert result.returncode == 125
    invocations = docker_log.read_text(encoding="utf-8").splitlines()
    assert any(" rm --force warehouse-hdfs-cli-" in f" {line} " for line in invocations)
