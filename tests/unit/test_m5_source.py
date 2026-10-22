"""Regression checks for bounded M5 source profiling evidence."""

from __future__ import annotations

from pathlib import Path

from warehouse_bigdata.ingestion import m5_source


def test_calendar_coverage_bounds_use_numeric_day_order() -> None:
    spec = m5_source._SOURCE_FILE_SPECS_BY_NAME["calendar.csv"]
    profile = m5_source._new_file_profile(Path("calendar.csv"), required=True)
    rows = []
    for index, day in enumerate(("d_1", "d_2", "d_10", "d_999", "d_1969"), start=1):
        values = {field: "" for field in spec.header}
        values.update(
            date=f"2011-01-{index:02d}",
            wm_yr_wk="11101",
            weekday="Saturday",
            wday="1",
            month="1",
            year="2011",
            d=day,
            snap_CA="0",
            snap_TX="0",
            snap_WI="0",
        )
        rows.append([values[field] for field in spec.header])

    blocking_counts: dict[str, int] = {}
    m5_source._scan_calendar_rows(
        iter(rows),
        profile,
        spec,
        list(spec.header),
        blocking_counts,
    )

    assert profile["coverage"]["first_day"] == "d_1"
    assert profile["coverage"]["last_day"] == "d_1969"
