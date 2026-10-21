"""Bounded-memory profiling for an already-acquired M5 source package."""

from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import re
import sqlite3
import tempfile
import uuid
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Final, cast
from urllib.parse import urlsplit

from warehouse_bigdata.contracts.manifests import FAILED, SOURCE_MANIFEST, VALIDATED
from warehouse_bigdata.contracts.validation import ContractValidationError, validate_manifest

M5_SOURCE_NAME: Final = "m5-forecasting-accuracy"
OFFICIAL_SOURCE_URL: Final = "https://www.kaggle.com/competitions/m5-forecasting-accuracy/data"
M5_SOURCE_URL: Final = OFFICIAL_SOURCE_URL
SOURCE_SCHEMA_VERSION: Final = "m5-source-v1"
REFERENCE_EVALUATION_SERIES: Final = 30_490
REFERENCE_EVALUATION_DAYS: Final = 1_941
REFERENCE_VALIDATION_DAYS: Final = 1_913
REFERENCE_CALENDAR_DAYS: Final = 1_969
REFERENCE_DERIVED_OBSERVATIONS: Final = 59_181_090
MAX_DIAGNOSTICS: Final = 8
CSV_CHUNK_SIZE: Final = 1024 * 1024
_ASCII_INTEGER = re.compile(r"\A[0-9]+\Z")

_SALES_IDENTIFIERS: Final = (
    "id",
    "item_id",
    "dept_id",
    "cat_id",
    "store_id",
    "state_id",
)
_CALENDAR_IDENTIFIERS: Final = ("date", "wm_yr_wk", "d")
_PRICE_IDENTIFIERS: Final = ("store_id", "item_id", "wm_yr_wk")
_SUBMISSION_IDENTIFIERS: Final = ("id",)


@dataclass(frozen=True, slots=True)
class SourceProvenance:
    """Truthful provenance supplied for one local M5 source package."""

    downloaded_at_utc: str
    source_url: str = OFFICIAL_SOURCE_URL
    source_name: str = M5_SOURCE_NAME
    source_schema_version: str = SOURCE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.source_name != M5_SOURCE_NAME:
            raise ValueError(f"source_name must be {M5_SOURCE_NAME!r}.")
        if self.source_schema_version != SOURCE_SCHEMA_VERSION:
            raise ValueError(f"source_schema_version must be {SOURCE_SCHEMA_VERSION!r}.")
        if not _is_official_source_url(self.source_url):
            raise ValueError(f"source_url must be the official Kaggle URL: {OFFICIAL_SOURCE_URL}")
        if _parse_utc_timestamp(self.downloaded_at_utc) is None:
            raise ValueError("downloaded_at_utc must be an ISO-8601 UTC timestamp.")


@dataclass(frozen=True, slots=True)
class SourceProfileResult:
    """Measured source profile and the v1 manifests derived from it."""

    status: str
    source_version: str
    file_profiles: dict[str, dict[str, object]]
    findings: tuple[str, ...]
    manifests: tuple[dict[str, object], ...]
    profile: dict[str, object]

    @property
    def manifest_records(self) -> tuple[dict[str, object], ...]:
        """Return generated manifest records using the API wording from the handoff."""
        return self.manifests


@dataclass(frozen=True, slots=True)
class _SourceFileSpec:
    filename: str
    kind: str
    required: bool
    header: tuple[str, ...]
    identifiers: tuple[str, ...]
    grain: str
    logical_unique_key: str
    expected_days: int | None = None


def _sales_header(day_count: int) -> tuple[str, ...]:
    return _SALES_IDENTIFIERS + tuple(f"d_{day}" for day in range(1, day_count + 1))


_SOURCE_FILE_SPECS: Final = (
    _SourceFileSpec(
        "sales_train_evaluation.csv",
        "sales_evaluation",
        True,
        _sales_header(REFERENCE_EVALUATION_DAYS),
        _SALES_IDENTIFIERS,
        "One observed-sales series with daily values from the evaluation source file",
        "id",
        REFERENCE_EVALUATION_DAYS,
    ),
    _SourceFileSpec(
        "sales_train_validation.csv",
        "sales_validation",
        False,
        _sales_header(REFERENCE_VALIDATION_DAYS),
        _SALES_IDENTIFIERS,
        "One observed-sales series with daily values from the validation source file",
        "id",
        REFERENCE_VALIDATION_DAYS,
    ),
    _SourceFileSpec(
        "calendar.csv",
        "calendar",
        True,
        (
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
        ),
        _CALENDAR_IDENTIFIERS,
        "One calendar day keyed by d and date",
        "d,date",
    ),
    _SourceFileSpec(
        "sell_prices.csv",
        "prices",
        True,
        ("store_id", "item_id", "wm_yr_wk", "sell_price"),
        _PRICE_IDENTIFIERS,
        "One store-item-week sell price",
        "store_id,item_id,wm_yr_wk",
    ),
    _SourceFileSpec(
        "sample_submission.csv",
        "submission",
        False,
        ("id",) + tuple(f"F{day}" for day in range(1, 29)),
        _SUBMISSION_IDENTIFIERS,
        "One forecast submission series",
        "id",
    ),
)
_SOURCE_FILE_SPECS_BY_NAME: Final = {spec.filename: spec for spec in _SOURCE_FILE_SPECS}
RECOGNIZED_M5_FILES: Final = frozenset(_SOURCE_FILE_SPECS_BY_NAME)
REQUIRED_M5_FILES: Final = frozenset(
    spec.filename for spec in _SOURCE_FILE_SPECS if spec.required
)
_CONFIGURATION_DIGEST: Final = "sha256:" + hashlib.sha256(
    json.dumps(
        {
            "source_schema_version": SOURCE_SCHEMA_VERSION,
            "files": [
                {"filename": spec.filename, "header": list(spec.header)}
                for spec in _SOURCE_FILE_SPECS
            ],
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
).hexdigest()


def compute_source_version(file_hashes: Mapping[str, str]) -> str:
    """Derive a path-independent source identity from sorted names and raw hashes."""
    canonical_entries = []
    for filename in sorted(file_hashes):
        digest = file_hashes[filename]
        if digest.startswith("sha256:"):
            digest = digest.removeprefix("sha256:")
        canonical_entries.append((filename, digest))
    canonical = json.dumps(
        canonical_entries,
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(canonical).hexdigest()}"


def profile_m5_source(source_dir: Path, provenance: SourceProvenance) -> SourceProfileResult:
    """Profile local M5 CSVs without downloading, rewriting, or normalizing them."""
    source_path = Path(source_dir)
    if not source_path.exists():
        raise ValueError(f"source directory does not exist: {source_path}")
    if not source_path.is_dir():
        raise ValueError(f"source path is not a directory: {source_path}")

    blocking_counts: dict[str, int] = {}
    file_profiles: dict[str, dict[str, object]] = {}
    file_hashes: dict[str, str] = {}
    try:
        csv_names = sorted(
            entry.name
            for entry in source_path.iterdir()
            if entry.is_file() and entry.suffix.lower() == ".csv"
        )
    except OSError as error:
        raise ValueError(f"cannot list source directory {source_path}: {error}") from error

    with tempfile.TemporaryDirectory(prefix="warehouse-m5-price-index-") as temp_dir:
        price_database = sqlite3.connect(str(Path(temp_dir) / "price-keys.sqlite3"))
        try:
            price_database.execute(
                "CREATE TABLE price_keys ("
                "store_id TEXT NOT NULL, item_id TEXT NOT NULL, wm_yr_wk TEXT NOT NULL, "
                "PRIMARY KEY (store_id, item_id, wm_yr_wk)"
                ") WITHOUT ROWID"
            )
            price_database.execute("PRAGMA synchronous = OFF")
            price_database.execute("PRAGMA journal_mode = OFF")
            for filename in csv_names:
                path = source_path / filename
                spec = _SOURCE_FILE_SPECS_BY_NAME.get(filename)
                if spec is None:
                    profile = _profile_unexpected_file(path, blocking_counts)
                else:
                    profile = _profile_known_file(
                        path,
                        spec,
                        blocking_counts,
                        price_database,
                    )
                file_profiles[filename] = profile
                digest = profile.get("sha256")
                if isinstance(digest, str):
                    file_hashes[filename] = digest
            price_database.commit()
        finally:
            price_database.close()

    for spec in _SOURCE_FILE_SPECS:
        if spec.required and spec.filename not in file_profiles:
            _add_missing_file_profile(spec, file_profiles, blocking_counts)

    source_version = compute_source_version(file_hashes)
    status = VALIDATED if not blocking_counts else FAILED
    findings = tuple(
        f"{name}={count}" for name, count in sorted(blocking_counts.items()) if count
    )
    profile_document = _build_profile_document(
        provenance,
        status,
        source_version,
        file_profiles,
        findings,
        blocking_counts,
    )
    manifests = _build_manifests(
        source_path,
        provenance,
        status,
        source_version,
        file_profiles,
    )
    return SourceProfileResult(
        status=status,
        source_version=source_version,
        file_profiles=file_profiles,
        findings=findings,
        manifests=tuple(manifests),
        profile=profile_document,
    )


def write_source_artifacts(
    result: SourceProfileResult,
    artifact_root: Path,
) -> dict[str, Path]:
    """Atomically write one run-scoped profile and manifest artifact bundle."""
    root = Path(artifact_root)
    run_stamp = _utc_now().replace("-", "").replace(":", "").replace("T", "-")
    run_dir = root / "m5-source" / f"run-{run_stamp}-{uuid.uuid4().hex[:12]}"
    run_dir.mkdir(parents=True, exist_ok=False)
    profile_path = run_dir / "source_profile.json"
    manifests_path = run_dir / "source_manifests.json"
    _atomic_write_json(profile_path, result.profile)
    _atomic_write_json(manifests_path, list(result.manifests))
    return {"source_profile": profile_path, "source_manifests": manifests_path}


def _profile_known_file(
    path: Path,
    spec: _SourceFileSpec,
    blocking_counts: dict[str, int],
    price_database: sqlite3.Connection,
) -> dict[str, object]:
    profile = _new_file_profile(path, required=spec.required)
    digest = _try_hash_file(path, profile, blocking_counts)
    if digest is not None:
        profile["sha256"] = digest

    header: list[str] | None = None
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.reader(handle, strict=True)
            header_row = next(reader, None)
            if header_row is None:
                _invalid(profile, blocking_counts, "header_mismatches", "CSV header is missing")
                header = []
            else:
                header = header_row
                _validate_header(profile, spec, header, blocking_counts)
                if spec.kind.startswith("sales_"):
                    _scan_sales_rows(reader, profile, spec, header, blocking_counts)
                elif spec.kind == "calendar":
                    _scan_calendar_rows(reader, profile, spec, header, blocking_counts)
                elif spec.kind == "prices":
                    _scan_price_rows(
                        reader,
                        profile,
                        spec,
                        header,
                        blocking_counts,
                        price_database,
                    )
                else:
                    _scan_submission_rows(reader, profile, spec, header, blocking_counts)
    except csv.Error as error:
        _invalid(profile, blocking_counts, "csv_parse_errors", f"CSV parse error: {error}")
    except (OSError, UnicodeError) as error:
        _invalid(profile, blocking_counts, "file_read_errors", f"cannot read CSV: {error}")

    if header is None:
        profile["header"] = {
            "actual": None,
            "expected": list(spec.header),
            "matches": False,
            "missing": list(spec.header),
            "unexpected": [],
            "duplicates": [],
        }
    _finalize_file_profile(profile, spec)
    return profile


def _profile_unexpected_file(
    path: Path,
    blocking_counts: dict[str, int],
) -> dict[str, object]:
    profile = _new_file_profile(path, required=False)
    digest = _try_hash_file(path, profile, blocking_counts)
    if digest is not None:
        profile["sha256"] = digest
    _invalid(
        profile,
        blocking_counts,
        "unexpected_csv_files",
        "CSV filename is not part of the recognized M5 source package",
    )
    profile["header"] = {
        "actual": None,
        "expected": [],
        "matches": False,
        "missing": [],
        "unexpected": [path.name],
        "duplicates": [],
    }
    _finalize_file_profile(profile, None)
    return profile


def _add_missing_file_profile(
    spec: _SourceFileSpec,
    file_profiles: dict[str, dict[str, object]],
    blocking_counts: dict[str, int],
) -> None:
    profile: dict[str, object] = {
        "source_file": spec.filename,
        "required": spec.required,
        "present": False,
        "row_count": 0,
        "size_bytes": None,
        "sha256": None,
        "header": {
            "actual": None,
            "expected": list(spec.header),
            "matches": False,
            "missing": list(spec.header),
            "unexpected": [],
            "duplicates": [],
        },
        "schema_fingerprint": None,
        "null_counts": {},
        "duplicate_counts": {},
        "invalid_counts": {},
        "coverage": {},
        "diagnostics": [],
        "validation_status": FAILED,
    }
    _invalid(
        profile,
        blocking_counts,
        "missing_required_files",
        "required source CSV is missing",
    )
    file_profiles[spec.filename] = profile


def _new_file_profile(path: Path, *, required: bool) -> dict[str, object]:
    try:
        size_bytes: int | None = path.stat().st_size
    except OSError:
        size_bytes = None
    return {
        "source_file": path.name,
        "required": required,
        "present": True,
        "row_count": 0,
        "size_bytes": size_bytes,
        "sha256": None,
        "header": None,
        "schema_fingerprint": None,
        "null_counts": {},
        "duplicate_counts": {},
        "invalid_counts": {},
        "coverage": {},
        "diagnostics": [],
        "validation_status": FAILED,
    }


def _try_hash_file(
    path: Path,
    profile: dict[str, object],
    blocking_counts: dict[str, int],
) -> str | None:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            while chunk := handle.read(CSV_CHUNK_SIZE):
                digest.update(chunk)
    except OSError as error:
        _invalid(profile, blocking_counts, "file_read_errors", f"cannot hash file: {error}")
        return None
    return f"sha256:{digest.hexdigest()}"


def _validate_header(
    profile: dict[str, object],
    spec: _SourceFileSpec,
    actual: list[str],
    blocking_counts: dict[str, int],
) -> None:
    expected = list(spec.header)
    seen: set[str] = set()
    duplicates = []
    for name in actual:
        if name in seen and name not in duplicates:
            duplicates.append(name)
        seen.add(name)
    header = {
        "actual": actual,
        "expected": expected,
        "matches": actual == expected,
        "missing": [name for name in expected if name not in actual],
        "unexpected": [name for name in actual if name not in expected],
        "duplicates": duplicates,
    }
    profile["header"] = header
    profile["schema_fingerprint"] = _fingerprint_header(actual)
    if actual != expected:
        _invalid(profile, blocking_counts, "header_mismatches", "CSV header does not match its M5 schema")
    if duplicates:
        _invalid(profile, blocking_counts, "duplicate_header_names", "CSV header contains duplicate names")


def _scan_sales_rows(
    reader: Iterator[list[str]],
    profile: dict[str, object],
    spec: _SourceFileSpec,
    header: list[str],
    blocking_counts: dict[str, int],
) -> None:
    positions = _positions(header)
    expected_days = tuple(f"d_{day}" for day in range(1, (spec.expected_days or 0) + 1))
    actual_days = tuple(name for name in header if name.startswith("d_"))
    _set_sales_coverage(profile, expected_days, actual_days, blocking_counts)
    seen_series: set[str] = set()
    valid_sales_values = 0
    zero_sales_values = 0
    for row_number, row in enumerate(reader, start=2):
        _increment_row_count(profile)
        _check_record_width(profile, len(row), len(spec.header), blocking_counts, row_number)
        identifiers = _identifier_values(
            row,
            positions,
            spec.identifiers,
            profile,
            blocking_counts,
            row_number,
        )
        series_id = identifiers.get("id")
        if series_id is not None:
            if series_id in seen_series:
                _duplicate(
                    profile,
                    blocking_counts,
                    "series",
                    "duplicate_sales_series",
                    f"duplicate sales series id {series_id!r}",
                    row_number,
                )
            seen_series.add(series_id)
        for day_name in expected_days:
            index = positions.get(day_name)
            if index is None or index >= len(row):
                continue
            raw_value = row[index].strip()
            if not raw_value:
                _null(profile, "sales_values")
                _invalid(
                    profile,
                    blocking_counts,
                    "invalid_sales_values",
                    f"empty sales value in {day_name}",
                    row_number,
                )
                continue
            if not _ASCII_INTEGER.fullmatch(raw_value):
                _invalid(
                    profile,
                    blocking_counts,
                    "invalid_sales_values",
                    f"sales value {raw_value!r} is not a nonnegative integer",
                    row_number,
                )
                continue
            valid_sales_values += 1
            if int(raw_value) == 0:
                zero_sales_values += 1
    profile["series_count"] = len(seen_series)
    profile["valid_sales_value_count"] = valid_sales_values
    profile["zero_sales_count"] = zero_sales_values


def _scan_calendar_rows(
    reader: Iterator[list[str]],
    profile: dict[str, object],
    spec: _SourceFileSpec,
    header: list[str],
    blocking_counts: dict[str, int],
) -> None:
    positions = _positions(header)
    seen_days: set[str] = set()
    seen_dates: set[str] = set()
    for row_number, row in enumerate(reader, start=2):
        _increment_row_count(profile)
        _check_record_width(profile, len(row), len(spec.header), blocking_counts, row_number)
        identifiers = _identifier_values(
            row,
            positions,
            spec.identifiers,
            profile,
            blocking_counts,
            row_number,
        )
        day_value = identifiers.get("d")
        date_value = identifiers.get("date")
        if day_value is not None:
            if day_value in seen_days:
                _duplicate(
                    profile,
                    blocking_counts,
                    "d",
                    "duplicate_calendar_keys",
                    f"duplicate calendar d value {day_value!r}",
                    row_number,
                )
            seen_days.add(day_value)
        if date_value is not None:
            if date_value in seen_dates:
                _duplicate(
                    profile,
                    blocking_counts,
                    "date",
                    "duplicate_calendar_keys",
                    f"duplicate calendar date {date_value!r}",
                    row_number,
                )
            seen_dates.add(date_value)
            try:
                parsed_date = date.fromisoformat(date_value)
            except ValueError:
                parsed_date = None
            if parsed_date is None or parsed_date.isoformat() != date_value:
                _invalid(
                    profile,
                    blocking_counts,
                    "invalid_dates",
                    f"calendar date {date_value!r} is not an ISO date",
                    row_number,
                )
    expected_days = {f"d_{day}" for day in range(1, REFERENCE_CALENDAR_DAYS + 1)}
    missing = expected_days - seen_days
    unexpected = seen_days - expected_days
    profile["coverage"] = {
        "expected_day_count": REFERENCE_CALENDAR_DAYS,
        "observed_day_count": len(seen_days),
        "missing_day_count": len(missing),
        "unexpected_day_count": len(unexpected),
        "first_day": min(seen_days) if seen_days else None,
        "last_day": max(seen_days) if seen_days else None,
    }
    if missing or unexpected:
        _invalid(
            profile,
            blocking_counts,
            "day_coverage_mismatches",
            "calendar d coverage must be exactly d_1 through d_1969",
        )
    profile["calendar_day_count"] = len(seen_days)


def _scan_price_rows(
    reader: Iterator[list[str]],
    profile: dict[str, object],
    spec: _SourceFileSpec,
    header: list[str],
    blocking_counts: dict[str, int],
    price_database: sqlite3.Connection,
) -> None:
    positions = _positions(header)
    valid_prices = 0
    for row_number, row in enumerate(reader, start=2):
        _increment_row_count(profile)
        _check_record_width(profile, len(row), len(spec.header), blocking_counts, row_number)
        identifiers = _identifier_values(
            row,
            positions,
            spec.identifiers,
            profile,
            blocking_counts,
            row_number,
        )
        if len(identifiers) == len(spec.identifiers):
            try:
                price_database.execute(
                    "INSERT INTO price_keys(store_id, item_id, wm_yr_wk) VALUES (?, ?, ?)",
                    tuple(identifiers[name] for name in spec.identifiers),
                )
            except sqlite3.IntegrityError:
                _duplicate(
                    profile,
                    blocking_counts,
                    "price_keys",
                    "duplicate_price_keys",
                    "duplicate (store_id, item_id, wm_yr_wk) price key",
                    row_number,
                )
        price_index = positions.get("sell_price")
        if price_index is None or price_index >= len(row):
            continue
        raw_price = row[price_index].strip()
        if not raw_price:
            _null(profile, "sell_price")
            _invalid(
                profile,
                blocking_counts,
                "invalid_prices",
                "sell_price is empty",
                row_number,
            )
            continue
        try:
            parsed_price = float(raw_price)
        except ValueError:
            parsed_price = math.nan
        if not math.isfinite(parsed_price) or parsed_price <= 0:
            _invalid(
                profile,
                blocking_counts,
                "invalid_prices",
                f"sell_price {raw_price!r} must be finite and positive",
                row_number,
            )
            continue
        valid_prices += 1
    profile["valid_price_count"] = valid_prices
    try:
        profile["unique_price_key_count"] = price_database.execute(
            "SELECT COUNT(*) FROM price_keys"
        ).fetchone()[0]
    except sqlite3.Error:
        profile["unique_price_key_count"] = None


def _scan_submission_rows(
    reader: Iterator[list[str]],
    profile: dict[str, object],
    spec: _SourceFileSpec,
    header: list[str],
    blocking_counts: dict[str, int],
) -> None:
    positions = _positions(header)
    seen_ids: set[str] = set()
    for row_number, row in enumerate(reader, start=2):
        _increment_row_count(profile)
        _check_record_width(profile, len(row), len(spec.header), blocking_counts, row_number)
        identifiers = _identifier_values(
            row,
            positions,
            spec.identifiers,
            profile,
            blocking_counts,
            row_number,
        )
        submission_id = identifiers.get("id")
        if submission_id is not None:
            if submission_id in seen_ids:
                _duplicate(
                    profile,
                    blocking_counts,
                    "id",
                    "duplicate_submission_ids",
                    f"duplicate submission id {submission_id!r}",
                    row_number,
                )
            seen_ids.add(submission_id)
    profile["submission_series_count"] = len(seen_ids)


def _set_sales_coverage(
    profile: dict[str, object],
    expected_days: tuple[str, ...],
    actual_days: tuple[str, ...],
    blocking_counts: dict[str, int],
) -> None:
    missing = set(expected_days) - set(actual_days)
    unexpected = set(actual_days) - set(expected_days)
    profile["coverage"] = {
        "expected_day_count": len(expected_days),
        "observed_day_count": len(actual_days),
        "missing_day_count": len(missing),
        "unexpected_day_count": len(unexpected),
        "first_day": actual_days[0] if actual_days else None,
        "last_day": actual_days[-1] if actual_days else None,
    }
    if missing or unexpected or actual_days != expected_days:
        _invalid(
            profile,
            blocking_counts,
            "day_coverage_mismatches",
            "sales day columns do not match the required M5 coverage",
        )


def _identifier_values(
    row: list[str],
    positions: Mapping[str, int],
    identifiers: tuple[str, ...],
    profile: dict[str, object],
    blocking_counts: dict[str, int],
    row_number: int,
) -> dict[str, str]:
    values: dict[str, str] = {}
    for name in identifiers:
        index = positions.get(name)
        if index is None or index >= len(row) or not row[index].strip():
            _null(profile, name)
            _invalid(
                profile,
                blocking_counts,
                "null_identifiers",
                f"required identifier {name!r} is null",
                row_number,
            )
            continue
        values[name] = row[index].strip()
    return values


def _check_record_width(
    profile: dict[str, object],
    actual_width: int,
    expected_width: int,
    blocking_counts: dict[str, int],
    row_number: int,
) -> None:
    if actual_width != expected_width:
        _invalid(
            profile,
            blocking_counts,
            "record_width_mismatches",
            f"record has {actual_width} fields; expected {expected_width}",
            row_number,
        )


def _positions(header: list[str]) -> dict[str, int]:
    return {name: index for index, name in enumerate(header) if name not in set(header[:index])}


def _increment_row_count(profile: dict[str, object]) -> None:
    profile["row_count"] = cast(int, profile["row_count"]) + 1


def _null(profile: dict[str, object], field: str) -> None:
    counts = cast(dict[str, int], profile["null_counts"])
    counts[field] = counts.get(field, 0) + 1


def _invalid(
    profile: dict[str, object],
    blocking_counts: dict[str, int],
    category: str,
    message: str,
    row_number: int | None = None,
) -> None:
    counts = cast(dict[str, int], profile["invalid_counts"])
    counts[category] = counts.get(category, 0) + 1
    blocking_counts[category] = blocking_counts.get(category, 0) + 1
    diagnostics = cast(list[dict[str, object]], profile["diagnostics"])
    if len(diagnostics) < MAX_DIAGNOSTICS:
        diagnostic: dict[str, object] = {"category": category, "message": message}
        if row_number is not None:
            diagnostic["row_number"] = row_number
        diagnostics.append(diagnostic)


def _duplicate(
    profile: dict[str, object],
    blocking_counts: dict[str, int],
    detail: str,
    category: str,
    message: str,
    row_number: int,
) -> None:
    counts = cast(dict[str, int], profile["duplicate_counts"])
    counts[detail] = counts.get(detail, 0) + 1
    blocking_counts[category] = blocking_counts.get(category, 0) + 1
    diagnostics = cast(list[dict[str, object]], profile["diagnostics"])
    if len(diagnostics) < MAX_DIAGNOSTICS:
        diagnostics.append(
            {"category": category, "message": message, "row_number": row_number}
        )


def _finalize_file_profile(
    profile: dict[str, object],
    spec: _SourceFileSpec | None,
) -> None:
    if spec is not None and profile.get("header") is not None:
        header = cast(dict[str, object], profile["header"])
        if header.get("actual") is not None:
            profile["schema_fingerprint"] = _fingerprint_header(
                cast(list[str], header["actual"])
            )
    invalid_counts = cast(dict[str, int], profile["invalid_counts"])
    duplicate_counts = cast(dict[str, int], profile["duplicate_counts"])
    profile["validation_status"] = VALIDATED if not invalid_counts and not duplicate_counts else FAILED


def _build_profile_document(
    provenance: SourceProvenance,
    status: str,
    source_version: str,
    file_profiles: dict[str, dict[str, object]],
    findings: tuple[str, ...],
    blocking_counts: dict[str, int],
) -> dict[str, object]:
    def measured_row_count(filename: str) -> int:
        profile = file_profiles.get(filename)
        if profile is None or not profile.get("present"):
            return 0
        return cast(int, profile.get("row_count", 0))

    evaluation_profile = file_profiles.get("sales_train_evaluation.csv", {})
    validation_profile = file_profiles.get("sales_train_validation.csv", {})
    calendar_profile = file_profiles.get("calendar.csv", {})
    evaluation_coverage = cast(dict[str, object], evaluation_profile.get("coverage", {}))
    validation_coverage = cast(dict[str, object], validation_profile.get("coverage", {}))
    measurements = {
        "present_csv_files": sum(
            1 for profile in file_profiles.values() if profile.get("present")
        ),
        "evaluation_series": measured_row_count("sales_train_evaluation.csv"),
        "validation_series": measured_row_count("sales_train_validation.csv"),
        "evaluation_days": evaluation_coverage.get("observed_day_count", 0),
        "validation_days": validation_coverage.get("observed_day_count", 0),
        "calendar_days": measured_row_count("calendar.csv"),
        "total_csv_rows": sum(
            measured_row_count(filename)
            for filename, profile in file_profiles.items()
            if profile.get("present")
        ),
    }
    return {
        "profile_schema_version": "1.0",
        "status": status,
        "source_version": source_version,
        "source_name": provenance.source_name,
        "source_url": provenance.source_url,
        "downloaded_at_utc": provenance.downloaded_at_utc,
        "source_schema_version": provenance.source_schema_version,
        "validation_scope": {
            "scope": "local_source_files",
            "hdfs_validated": False,
            "spark_validated": False,
            "curated_data_validated": False,
        },
        "reference_expectations": {
            "evaluation_series": REFERENCE_EVALUATION_SERIES,
            "evaluation_days": REFERENCE_EVALUATION_DAYS,
            "validation_days": REFERENCE_VALIDATION_DAYS,
            "calendar_days": REFERENCE_CALENDAR_DAYS,
            "derived_observations": REFERENCE_DERIVED_OBSERVATIONS,
        },
        "measurements": measurements,
        "files": file_profiles,
        "findings": list(findings),
        "blocking_counts": dict(sorted(blocking_counts.items())),
    }


def _build_manifests(
    source_path: Path,
    provenance: SourceProvenance,
    status: str,
    source_version: str,
    file_profiles: Mapping[str, dict[str, object]],
) -> list[dict[str, object]]:
    created_at = _utc_now()
    manifests: list[dict[str, object]] = []
    for filename in sorted(RECOGNIZED_M5_FILES & file_profiles.keys()):
        spec = _SOURCE_FILE_SPECS_BY_NAME[filename]
        profile = file_profiles[filename]
        if not profile.get("present"):
            continue
        path = source_path / filename
        manifest: dict[str, object] = {
            "manifest_schema_version": SOURCE_MANIFEST.version,
            "contract_name": SOURCE_MANIFEST.name,
            "contract_version": SOURCE_MANIFEST.version,
            "manifest_kind": SOURCE_MANIFEST.manifest_kind,
            "status": status,
            "producer": "warehouse-bigdata:m5-source-profile",
            "artifact_uri": path.resolve().as_uri(),
            "artifact_checksum": profile.get("sha256"),
            "schema_fingerprint": profile.get("schema_fingerprint"),
            "grain": spec.grain,
            "logical_unique_key": spec.logical_unique_key,
            "row_count": cast(int, profile.get("row_count", 0)),
            "required_field_null_counts": _json_string(profile.get("null_counts", {})),
            "code_revision": "warehouse-bigdata:m5-source-profile-v1",
            "configuration_digest": _CONFIGURATION_DIGEST,
            "created_at": created_at,
            "completed_at": created_at,
            "coverage_and_exclusion_counts": _json_string(
                {
                    "validation_scope": "local_source_file",
                    "coverage": profile.get("coverage", {}),
                    "duplicate_counts": profile.get("duplicate_counts", {}),
                    "invalid_counts": profile.get("invalid_counts", {}),
                }
            ),
            "source_version": source_version,
            "source_name": provenance.source_name,
            "source_url": provenance.source_url,
            "downloaded_at_utc": provenance.downloaded_at_utc,
            "source_file": filename,
            "size_bytes": profile.get("size_bytes")
            if isinstance(profile.get("size_bytes"), int)
            else 0,
            "source_schema_version": provenance.source_schema_version,
            "source_file_row_count": cast(int, profile.get("row_count", 0)),
        }
        try:
            validate_manifest(SOURCE_MANIFEST, manifest)
        except ContractValidationError as error:
            raise RuntimeError(f"generated invalid SOURCE_MANIFEST for {filename}: {error}") from error
        manifests.append(manifest)
    return manifests


def _fingerprint_header(header: list[str]) -> str:
    return "sha256:" + hashlib.sha256(
        json.dumps(header, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _json_string(value: object) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _is_official_source_url(value: str) -> bool:
    if not isinstance(value, str) or any(character.isspace() for character in value):
        return False
    parsed = urlsplit(value)
    return (
        parsed.scheme.lower() == "https"
        and parsed.netloc.lower() == "www.kaggle.com"
        and parsed.path.rstrip("/") == "/competitions/m5-forecasting-accuracy/data"
        and not parsed.query
        and not parsed.fragment
    )


def _parse_utc_timestamp(value: str) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        return None
    return parsed


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _atomic_write_json(path: Path, value: object) -> None:
    temporary_path: Path | None = None
    try:
        file_descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{path.name}.",
            suffix=".tmp",
            dir=path.parent,
        )
        temporary_path = Path(temporary_name)
        with os.fdopen(file_descriptor, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(value, handle, ensure_ascii=True, sort_keys=True, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
    except BaseException:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        raise
