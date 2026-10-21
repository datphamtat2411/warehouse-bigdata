"""Focused checks for local M5 source profiling and manifest generation."""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

from warehouse_bigdata.contracts.manifests import SOURCE_MANIFEST, VALIDATED
from warehouse_bigdata.contracts.validation import validate_manifest
from warehouse_bigdata.ingestion.hdfs_raw import preflight_source_artifacts
from warehouse_bigdata.ingestion.m5_source import (
    OFFICIAL_SOURCE_URL,
    RECOGNIZED_M5_FILES,
    REQUIRED_M5_FILES,
    SourceProvenance,
    compute_source_version,
    profile_m5_source,
    write_source_artifacts,
)

EVALUATION_DAYS = 1941
VALIDATION_DAYS = 1913
CALENDAR_DAYS = 1969
TIMESTAMP = "2026-10-21T12:34:56Z"


def _write_csv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)


def _sales_header(day_count: int) -> list[str]:
    return ["id", "item_id", "dept_id", "cat_id", "store_id", "state_id"] + [
        f"d_{day}" for day in range(1, day_count + 1)
    ]


def _sales_row(identifier: str, day_count: int) -> list[str]:
    return [identifier, "FOODS_1", "FOODS_1", "FOODS", "CA_1", "CA"] + [
        "0" for _ in range(day_count)
    ]


def _write_canonical_source(
    root: Path,
    *,
    include_optional: bool = False,
) -> None:
    evaluation_header = _sales_header(EVALUATION_DAYS)
    evaluation_rows = [
        _sales_row("FOODS_1_001_CA_1_evaluation", EVALUATION_DAYS),
        _sales_row("FOODS_1_002_CA_1_evaluation", EVALUATION_DAYS),
    ]
    evaluation_rows[0][6] = "3"
    _write_csv(root / "sales_train_evaluation.csv", evaluation_header, evaluation_rows)

    calendar_header = [
        "date",
        "wm_yr_wk",
        "weekday",
        "wday",
        "month",
        "year",
        "d",
        "event_name_1",
        "event_type_1",
        "event_name_2",
        "event_type_2",
        "snap_CA",
        "snap_TX",
        "snap_WI",
    ]
    calendar_rows = []
    first_day = date(2011, 1, 29)
    for day in range(1, CALENDAR_DAYS + 1):
        calendar_rows.append(
            [
                (first_day + timedelta(days=day - 1)).isoformat(),
                "11101",
                "Saturday",
                "1",
                "1",
                "2011",
                f"d_{day}",
                "",
                "",
                "",
                "",
                "0",
                "0",
                "0",
            ]
        )
    _write_csv(root / "calendar.csv", calendar_header, calendar_rows)

    _write_csv(
        root / "sell_prices.csv",
        ["store_id", "item_id", "wm_yr_wk", "sell_price"],
        [
            ["CA_1", "FOODS_1", "11101", "1.25"],
            ["CA_1", "FOODS_2", "11101", "2.50"],
        ],
    )

    if include_optional:
        _write_csv(
            root / "sales_train_validation.csv",
            _sales_header(VALIDATION_DAYS),
            [_sales_row("FOODS_1_001_CA_1_validation", VALIDATION_DAYS)],
        )
        _write_csv(
            root / "sample_submission.csv",
            ["id"] + [f"F{day}" for day in range(1, 29)],
            [["FOODS_1_001_CA_1_validation"] + ["0" for _ in range(28)]],
        )


def _provenance(downloaded_at_utc: str | None = TIMESTAMP) -> SourceProvenance:
    return SourceProvenance(
        source_url=OFFICIAL_SOURCE_URL,
        downloaded_at_utc=downloaded_at_utc,
    )


def _profile(root: Path, downloaded_at_utc: str | None = TIMESTAMP):
    return profile_m5_source(root, _provenance(downloaded_at_utc))


def test_success_profile_preserves_zero_sales_and_validates_every_manifest(
    tmp_path: Path,
) -> None:
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    _write_canonical_source(source_dir)
    original_bytes = {
        path.name: path.read_bytes() for path in source_dir.glob("*.csv")
    }

    result = _profile(source_dir)

    assert result.status == VALIDATED
    evaluation = result.file_profiles["sales_train_evaluation.csv"]
    assert evaluation["row_count"] == 2
    assert evaluation["zero_sales_count"] == 1941 * 2 - 1
    assert evaluation["invalid_counts"] == {}
    assert result.manifests
    assert {manifest["source_version"] for manifest in result.manifests} == {
        result.source_version
    }
    assert result.profile["downloaded_at_utc"] == TIMESTAMP
    assert isinstance(result.profile["profiled_at_utc"], str)
    for manifest in result.manifests:
        validate_manifest(SOURCE_MANIFEST, manifest)
        assert manifest["downloaded_at_utc"] == TIMESTAMP

    artifact_paths = write_source_artifacts(result, tmp_path / "artifacts")
    profile_document = json.loads(artifact_paths["source_profile"].read_text(encoding="utf-8"))
    manifest_document = json.loads(artifact_paths["source_manifests"].read_text(encoding="utf-8"))
    assert profile_document["status"] == VALIDATED
    assert profile_document["reference_expectations"]["evaluation_days"] == EVALUATION_DAYS
    assert profile_document["measurements"]["evaluation_series"] == 2
    assert [manifest["source_file"] for manifest in manifest_document] == sorted(
        manifest["source_file"] for manifest in result.manifests
    )
    assert {path.name: path.read_bytes() for path in source_dir.glob("*.csv")} == original_bytes
    assert all(path.is_relative_to(tmp_path / "artifacts") for path in artifact_paths.values())


def test_three_file_profile_records_absent_optional_files_and_passes_hdfs_preflight(
    tmp_path: Path,
) -> None:
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    _write_canonical_source(source_dir)

    result = _profile(source_dir)
    artifacts = write_source_artifacts(result, tmp_path / "artifacts")

    assert set(result.file_profiles) == RECOGNIZED_M5_FILES
    for filename in {"sales_train_validation.csv", "sample_submission.csv"}:
        assert result.file_profiles[filename]["present"] is False
        assert result.file_profiles[filename]["validation_status"] == VALIDATED
        assert filename not in {manifest["source_file"] for manifest in result.manifests}

    bundle = preflight_source_artifacts(
        source_dir,
        artifacts["source_profile"],
        artifacts["source_manifests"],
    )
    assert {item.source_file for item in bundle.files} == REQUIRED_M5_FILES


def test_profile_uses_and_cleans_explicit_scratch_directory(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    _write_canonical_source(source_dir)
    scratch_dir = tmp_path / "ubuntu-local-scratch"

    result = _profile(source_dir)
    explicit = profile_m5_source(
        source_dir,
        _provenance(),
        scratch_dir=scratch_dir,
    )

    assert explicit.source_version == result.source_version
    assert scratch_dir.is_dir()
    assert tuple(scratch_dir.iterdir()) == ()


def test_profile_rejects_unusable_scratch_path(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    _write_canonical_source(source_dir)
    scratch_file = tmp_path / "scratch-file"
    scratch_file.write_text("not a directory", encoding="utf-8")

    with pytest.raises(ValueError, match="scratch"):
        profile_m5_source(source_dir, _provenance(), scratch_dir=scratch_file)


def test_unknown_download_time_is_null_and_profile_time_is_recorded(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    _write_canonical_source(source_dir)

    result = _profile(source_dir, downloaded_at_utc=None)

    assert result.status == VALIDATED
    assert result.profile["downloaded_at_utc"] is None
    profiled_at = result.profile["profiled_at_utc"]
    assert isinstance(profiled_at, str)
    assert datetime.fromisoformat(profiled_at.replace("Z", "+00:00")).tzinfo is not None
    assert all(manifest["downloaded_at_utc"] is None for manifest in result.manifests)

    artifact_paths = write_source_artifacts(result, tmp_path / "artifacts")
    profile_document = json.loads(artifact_paths["source_profile"].read_text(encoding="utf-8"))
    assert profile_document["downloaded_at_utc"] is None
    assert profile_document["profiled_at_utc"] == profiled_at


@pytest.mark.parametrize(
    "downloaded_at_utc",
    ["", "unknown", "N/A", "TBD", "2026-10-21T12:34:56", "2026-10-21T12:34:56+01:00"],
)
def test_invalid_download_timestamps_are_rejected(downloaded_at_utc: str) -> None:
    with pytest.raises(ValueError, match="downloaded_at_utc"):
        SourceProvenance(
            source_url=OFFICIAL_SOURCE_URL,
            downloaded_at_utc=downloaded_at_utc,
        )


def test_source_url_must_be_the_official_m5_source() -> None:
    with pytest.raises(ValueError, match="official Kaggle URL"):
        SourceProvenance(source_url="https://example.test/m5", downloaded_at_utc=None)


def test_source_version_ignores_acquisition_timestamp(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    _write_canonical_source(source_dir)

    known_time = _profile(source_dir, downloaded_at_utc=TIMESTAMP)
    unknown_time = _profile(source_dir, downloaded_at_utc=None)

    assert unknown_time.source_version == known_time.source_version


def test_source_version_is_content_derived_and_order_independent() -> None:
    first = {"b.csv": "b" * 64, "a.csv": "a" * 64}
    second = {"a.csv": "a" * 64, "b.csv": "b" * 64}

    assert compute_source_version(first) == compute_source_version(second)
    assert compute_source_version(first).startswith("sha256:")


def test_optional_sales_and_submission_are_profiled_separately(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    _write_canonical_source(source_dir, include_optional=True)

    result = _profile(source_dir)

    assert result.status == VALIDATED
    assert result.file_profiles["sales_train_validation.csv"]["row_count"] == 1
    assert result.file_profiles["sample_submission.csv"]["row_count"] == 1
    assert {manifest["source_file"] for manifest in result.manifests} == {
        "calendar.csv",
        "sample_submission.csv",
        "sales_train_evaluation.csv",
        "sales_train_validation.csv",
        "sell_prices.csv",
    }
    assert result.profile["measurements"]["evaluation_series"] == 2
    assert result.profile["measurements"]["validation_series"] == 1


@pytest.mark.parametrize(
    "mutation, expected_counter",
    [
        ("missing", "missing_required_files"),
        ("malformed", "csv_parse_errors"),
        ("header", "header_mismatches"),
        ("days", "day_coverage_mismatches"),
        ("null_id", "null_identifiers"),
        ("duplicate_sales", "duplicate_sales_series"),
        ("duplicate_calendar", "duplicate_calendar_keys"),
        ("date", "invalid_dates"),
        ("sales", "invalid_sales_values"),
        ("price_key", "duplicate_price_keys"),
        ("price", "invalid_prices"),
    ],
)
def test_blocking_source_defects_fail_without_validated_manifests(
    tmp_path: Path,
    mutation: str,
    expected_counter: str,
) -> None:
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    _write_canonical_source(source_dir)

    if mutation == "missing":
        (source_dir / "calendar.csv").unlink()
    elif mutation == "malformed":
        with (source_dir / "calendar.csv").open("a", encoding="utf-8") as handle:
            handle.write('"unterminated\n')
    elif mutation == "header":
        path = source_dir / "sell_prices.csv"
        _write_csv(path, ["store_id", "item_id", "wm_yr_wk", "wrong"], [])
    elif mutation == "days":
        path = source_dir / "sales_train_evaluation.csv"
        header = _sales_header(EVALUATION_DAYS)
        header[-1] = "d_9999"
        _write_csv(path, header, [_sales_row("one", EVALUATION_DAYS)])
    elif mutation == "null_id":
        path = source_dir / "sales_train_evaluation.csv"
        _write_csv(path, _sales_header(EVALUATION_DAYS), [_sales_row("", EVALUATION_DAYS)])
    elif mutation == "duplicate_sales":
        path = source_dir / "sales_train_evaluation.csv"
        _write_csv(
            path,
            _sales_header(EVALUATION_DAYS),
            [_sales_row("same", EVALUATION_DAYS), _sales_row("same", EVALUATION_DAYS)],
        )
    elif mutation == "duplicate_calendar":
        path = source_dir / "calendar.csv"
        rows = list(csv.reader(path.open(encoding="utf-8", newline="")))
        rows[2][6] = rows[1][6]
        with path.open("w", encoding="utf-8", newline="") as handle:
            csv.writer(handle, lineterminator="\n").writerows(rows)
    elif mutation == "date":
        path = source_dir / "calendar.csv"
        rows = list(csv.reader(path.open(encoding="utf-8", newline="")))
        rows[1][0] = "not-a-date"
        with path.open("w", encoding="utf-8", newline="") as handle:
            csv.writer(handle, lineterminator="\n").writerows(rows)
    elif mutation == "sales":
        path = source_dir / "sales_train_evaluation.csv"
        rows = list(csv.reader(path.open(encoding="utf-8", newline="")))
        rows[1][6] = "-1"
        with path.open("w", encoding="utf-8", newline="") as handle:
            csv.writer(handle, lineterminator="\n").writerows(rows)
    elif mutation == "price_key":
        path = source_dir / "sell_prices.csv"
        _write_csv(
            path,
            ["store_id", "item_id", "wm_yr_wk", "sell_price"],
            [["CA_1", "FOODS_1", "11101", "1.25"], ["CA_1", "FOODS_1", "11101", "2.50"]],
        )
    elif mutation == "price":
        path = source_dir / "sell_prices.csv"
        _write_csv(
            path,
            ["store_id", "item_id", "wm_yr_wk", "sell_price"],
            [["CA_1", "FOODS_1", "11101", "0"], ["CA_1", "FOODS_2", "11101", "nan"]],
        )

    result = _profile(source_dir)

    assert result.status != VALIDATED
    assert result.profile["findings"]
    assert result.profile["blocking_counts"].get(expected_counter, 0) > 0
    assert all(manifest["status"] != VALIDATED for manifest in result.manifests)
    for manifest in result.manifests:
        validate_manifest(SOURCE_MANIFEST, manifest)


def test_unexpected_csv_is_rejected_and_failure_artifacts_are_bounded(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    _write_canonical_source(source_dir)
    _write_csv(source_dir / "unexpected.csv", ["id"], [["unexpected"]])

    result = _profile(source_dir)
    artifact_paths = write_source_artifacts(result, tmp_path / "artifacts")
    manifests = json.loads(artifact_paths["source_manifests"].read_text(encoding="utf-8"))

    assert result.status != VALIDATED
    assert result.profile["blocking_counts"]["unexpected_csv_files"] == 1
    assert all(manifest["status"] == "FAILED" for manifest in manifests)
    assert all(path.is_relative_to(tmp_path / "artifacts") for path in artifact_paths.values())


def test_file_hash_matches_original_bytes(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    _write_canonical_source(source_dir)

    result = _profile(source_dir)
    expected = hashlib.sha256((source_dir / "calendar.csv").read_bytes()).hexdigest()

    assert result.file_profiles["calendar.csv"]["sha256"] == f"sha256:{expected}"
