"""Explicitly gated, bounded M04 workload comparison."""

from __future__ import annotations

import os

import pytest

from tests.integration.hdfs_support import HDFSVerification
from tests.integration.m04_support import PERFORMANCE_ENV, run_benchmark_matrix

pytestmark = pytest.mark.integration

if os.environ.get(PERFORMANCE_ENV) != "1":
    pytestmark = [
        pytest.mark.integration,
        pytest.mark.skip(reason=f"Set {PERFORMANCE_ENV}=1 to run the bounded benchmark matrix."),
    ]
elif not all(
    (os.environ.get("WAREHOUSE_M5_SOURCE_DIR"), os.environ.get("WAREHOUSE_M5_SOURCE_URL"))
):
    pytestmark = [
        pytest.mark.integration,
        pytest.mark.skip(
            reason="Set WAREHOUSE_M5_SOURCE_DIR and WAREHOUSE_M5_SOURCE_URL for live M04 performance."
        ),
    ]


def test_bounded_m04_benchmark_matrix_retains_measured_results(
    tmp_path, hdfs: HDFSVerification
) -> None:
    report = run_benchmark_matrix(hdfs, tmp_path)

    assert len(report["cases"]) == 7
    assert report["workload"]["expected_row_count"] == 30_490 * 128
    assert ("COMPLETED_WITH_FAILURES", "VALIDATED").count(report["status"]) == 1
    assert all("case" in case and "status" in case for case in report["cases"])
    print("M04 benchmark report: artifacts/m04/benchmark/report.json")
