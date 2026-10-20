from importlib import import_module, metadata
from unittest.mock import Mock

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


def test_no_arguments_preserve_successful_noop(capsys: pytest.CaptureFixture[str]) -> None:
    assert main([]) == 0
    assert capsys.readouterr() == ("", "")


def test_spark_submit_help_needs_no_runtime(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc_info:
        main(["spark-submit", "--help"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "--conf" in output
    assert "APPLICATION.py" in output


@pytest.mark.parametrize(
    "arguments",
    [[], ["job.py"], ["--"], ["--conf", "spark.sql.shuffle.partitions=4", "job.py"]],
)
def test_spark_submit_requires_explicit_application_boundary(
    arguments: list[str], monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    submit = Mock()
    monkeypatch.setattr("warehouse_bigdata.cli.run_spark_submit", submit)

    with pytest.raises(SystemExit) as exc_info:
        main(["spark-submit", *arguments])

    assert exc_info.value.code == 2
    assert "-- APPLICATION.py" in capsys.readouterr().err
    submit.assert_not_called()


def test_spark_submit_forwards_application_arguments_without_reparsing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    submit = Mock(return_value=29)
    monkeypatch.setattr("warehouse_bigdata.cli.run_spark_submit", submit)
    arguments = ["two words", "Tiếng Việt", "--help", "--version", "--conf", "x=y", "--", "$(id);*"]

    assert (
        main(
            [
                "spark-submit",
                "--conf",
                "spark.sql.shuffle.partitions=4",
                "--",
                "warehouse_bigdata/jobs/future.py",
                *arguments,
            ]
        )
        == 29
    )
    submit.assert_called_once_with(
        "warehouse_bigdata/jobs/future.py", arguments, ["spark.sql.shuffle.partitions=4"]
    )
