from importlib import import_module, metadata
from pathlib import Path
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


def test_profile_m5_source_forwards_required_provenance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = Mock(return_value=7)
    monkeypatch.setattr("warehouse_bigdata.cli.run_profile_m5_source", profile)

    assert main(
        [
            "profile-m5-source",
            "--source-dir",
            "/tmp/m5-source",
            "--source-url",
            "https://www.kaggle.com/competitions/m5-forecasting-accuracy/data",
            "--downloaded-at-utc",
            "2026-10-21T12:34:56Z",
        ]
    ) == 7
    profile.assert_called_once_with(
        Path("/tmp/m5-source"),
        "https://www.kaggle.com/competitions/m5-forecasting-accuracy/data",
        "2026-10-21T12:34:56Z",
    )


def test_profile_m5_source_requires_provenance(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc_info:
        main(["profile-m5-source", "--source-dir", "/tmp/m5-source"])

    assert exc_info.value.code == 2
    assert "--source-url" in capsys.readouterr().err


def test_ingest_m5_raw_forwards_explicit_artifact_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ingest = Mock(return_value=0)
    monkeypatch.setattr("warehouse_bigdata.cli.run_ingest_m5_raw", ingest)

    assert main(
        [
            "ingest-m5-raw",
            "--source-dir",
            "/inputs/m5",
            "--profile-path",
            "/artifacts/source_profile.json",
            "--manifest-path",
            "/artifacts/source_manifests.json",
            "--pipeline-run-id",
            "run-20261021",
        ]
    ) == 0
    ingest.assert_called_once_with(
        Path("/inputs/m5"),
        Path("/artifacts/source_profile.json"),
        Path("/artifacts/source_manifests.json"),
        "run-20261021",
    )


def test_ingest_m5_raw_requires_all_artifact_paths(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc_info:
        main(["ingest-m5-raw", "--source-dir", "/inputs/m5"])

    assert exc_info.value.code == 2
    assert "--profile-path" in capsys.readouterr().err


def test_ingest_m5_forwards_source_provenance_and_optional_run_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ingest = Mock(return_value=0)
    monkeypatch.setattr("warehouse_bigdata.cli.run_ingest_m5", ingest)

    assert main(
        [
            "ingest-m5",
            "--source-dir",
            "/inputs/m5",
            "--source-url",
            "https://www.kaggle.com/competitions/m5-forecasting-accuracy/data",
            "--downloaded-at-utc",
            "2026-10-21T12:34:56Z",
            "--pipeline-run-id",
            "run-combined",
        ]
    ) == 0
    ingest.assert_called_once_with(
        Path("/inputs/m5"),
        "https://www.kaggle.com/competitions/m5-forecasting-accuracy/data",
        "2026-10-21T12:34:56Z",
        "run-combined",
    )


def test_ingest_m5_requires_truthful_provenance(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc_info:
        main(["ingest-m5", "--source-dir", "/inputs/m5"])

    assert exc_info.value.code == 2
    error = capsys.readouterr().err
    assert "--source-url" in error
    assert "--downloaded-at-utc" in error
