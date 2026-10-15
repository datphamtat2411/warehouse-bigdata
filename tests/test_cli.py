from importlib import import_module, metadata

import pytest

from warehouse_bigdata.cli import main


def test_package_imports() -> None:
    package = import_module("warehouse_bigdata")

    assert package.__name__ == "warehouse_bigdata"


def test_help_exits_successfully(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc_info:
        main(["--help"])

    captured = capsys.readouterr()
    assert exc_info.value.code == 0
    assert "usage: warehouse-bigdata" in captured.out
    assert "--version" in captured.out


def test_version_reports_installed_package_version(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as exc_info:
        main(["--version"])

    captured = capsys.readouterr()
    expected = f"warehouse-bigdata {metadata.version('warehouse-bigdata')}"
    assert exc_info.value.code == 0
    assert captured.out.strip() == expected


def test_invalid_argument_exits_with_usage_error(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as exc_info:
        main(["--invalid"])

    captured = capsys.readouterr()
    assert exc_info.value.code == 2
    assert "unrecognized arguments: --invalid" in captured.err
