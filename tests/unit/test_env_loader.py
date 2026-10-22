"""Focused checks for the host-side repository environment loader."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
LOADER = ROOT / "scripts" / "load-env.sh"


def _run_loader(
    tmp_path: Path,
    env_text: str | None,
    command: str,
    *,
    inherited: dict[str, str] | None = None,
    trace: bool = False,
    arguments: tuple[str, ...] = (),
) -> subprocess.CompletedProcess[str]:
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    shutil.copy2(LOADER, scripts / LOADER.name)
    if env_text is not None:
        (tmp_path / ".env").write_text(env_text, encoding="utf-8")
    caller = tmp_path / "nested" / "caller"
    caller.mkdir(parents=True)
    environment = {"PATH": os.environ["PATH"], **(inherited or {})}
    shell = "set -x; " if trace else ""
    return subprocess.run(
        [
            "bash",
            "-c",
            f"{shell}source {scripts / LOADER.name}; {command}",
            "loader-test",
            *arguments,
        ],
        cwd=caller,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )


def test_loader_exports_project_values_from_the_repository_env_file(tmp_path: Path) -> None:
    result = _run_loader(
        tmp_path,
        "WAREHOUSE_TEST_VALUE=from-file\nexport WAREHOUSE_QUOTED='two words'\n",
        'printf "%s|%s" "$WAREHOUSE_TEST_VALUE" "$WAREHOUSE_QUOTED"',
    )

    assert result.returncode == 0
    assert result.stdout == "from-file|two words"
    assert result.stderr == ""


def test_project_env_precedes_inherited_environment(tmp_path: Path) -> None:
    result = _run_loader(
        tmp_path,
        "WAREHOUSE_TEST_VALUE=from-file\n",
        'printf "%s" "$WAREHOUSE_TEST_VALUE"',
        inherited={"WAREHOUSE_TEST_VALUE": "from-inherited"},
    )

    assert result.returncode == 0
    assert result.stdout == "from-file"


def test_explicit_command_argument_can_override_loaded_value(tmp_path: Path) -> None:
    result = _run_loader(
        tmp_path,
        "WAREHOUSE_TEST_VALUE=from-file\n",
        'selected=${1:-$WAREHOUSE_TEST_VALUE}; printf "%s" "$selected"',
        arguments=("from-argument",),
    )

    assert result.returncode == 0
    assert result.stdout == "from-argument"


def test_loader_is_a_noop_when_project_env_is_missing(tmp_path: Path) -> None:
    result = _run_loader(
        tmp_path,
        None,
        'printf "%s" "$WAREHOUSE_TEST_VALUE"',
        inherited={"WAREHOUSE_TEST_VALUE": "from-inherited"},
    )

    assert result.returncode == 0
    assert result.stdout == "from-inherited"
    assert result.stderr == ""


def test_loader_resolves_env_from_repository_not_caller_directory(tmp_path: Path) -> None:
    result = _run_loader(
        tmp_path,
        "WAREHOUSE_TEST_VALUE=from-repository\n",
        'printf "%s" "$WAREHOUSE_TEST_VALUE"',
    )

    assert result.returncode == 0
    assert result.stdout == "from-repository"


def test_loader_does_not_expose_secret_values_under_bash_tracing(tmp_path: Path) -> None:
    secret = "not-a-real-secret-for-tests"
    result = _run_loader(
        tmp_path,
        f"WAREHOUSE_CLICKHOUSE_PASSWORD={secret}\nWAREHOUSE_ENV=local\n",
        'printf "%s" "$WAREHOUSE_ENV"',
        trace=True,
    )

    assert result.returncode == 0
    assert result.stdout == "local"
    assert secret not in result.stdout
    assert secret not in result.stderr


def test_loader_can_run_one_host_command_from_a_nested_directory(tmp_path: Path) -> None:
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    shutil.copy2(LOADER, scripts / LOADER.name)
    (tmp_path / ".env").write_text("WAREHOUSE_TEST_VALUE=from-file\n", encoding="utf-8")
    caller = tmp_path / "nested" / "caller"
    caller.mkdir(parents=True)

    result = subprocess.run(
        [
            "bash",
            str(scripts / LOADER.name),
            "bash",
            "-c",
            'printf "%s" "$WAREHOUSE_TEST_VALUE"',
        ],
        cwd=caller,
        env={"PATH": os.environ["PATH"], "WAREHOUSE_TEST_VALUE": "from-inherited"},
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert result.stdout == "from-file"
    assert result.stderr == ""
