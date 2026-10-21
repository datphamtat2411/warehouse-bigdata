"""Offline checks for optional acquisition-time forwarding in the M5 wrapper."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "ingest-m5.sh"
SOURCE_URL = "https://www.kaggle.com/competitions/m5-forecasting-accuracy/data"
TIMESTAMP = "2026-10-21T12:34:56Z"


@pytest.mark.parametrize("downloaded_at_utc", [None, TIMESTAMP])
def test_wrapper_forwards_download_time_only_when_supplied(
    tmp_path: Path,
    downloaded_at_utc: str | None,
) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    docker_log = tmp_path / "docker.log"
    fake_docker = fake_bin / "docker"
    fake_docker.write_text(
        "#!/usr/bin/env bash\n"
        "set -eu\n"
        "printf '%s\\n' \"$*\" >> \"$FAKE_DOCKER_LOG\"\n"
        "exit 0\n",
        encoding="utf-8",
    )
    fake_docker.chmod(0o755)
    source_dir = tmp_path / "source"
    source_dir.mkdir()

    arguments = [
        "bash",
        str(SCRIPT),
        "--source-dir",
        str(source_dir),
        "--source-url",
        SOURCE_URL,
    ]
    if downloaded_at_utc is not None:
        arguments.extend(["--downloaded-at-utc", downloaded_at_utc])
    result = subprocess.run(
        arguments,
        capture_output=True,
        text=True,
        check=False,
        env={
            **os.environ,
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
            "FAKE_DOCKER_LOG": str(docker_log),
            "WAREHOUSE_ARTIFACTS_DIR": str(tmp_path / "artifacts"),
        },
    )

    assert result.returncode == 0, result.stderr
    compose_invocation = docker_log.read_text(encoding="utf-8")
    if downloaded_at_utc is None:
        assert "--downloaded-at-utc" not in compose_invocation
    else:
        assert f"--downloaded-at-utc {downloaded_at_utc}" in compose_invocation
