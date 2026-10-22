"""Opt-in full-scope M04 J02 to J03 correctness acceptance."""

from __future__ import annotations

import os

import pytest

from tests.integration.hdfs_support import HDFSVerification
from tests.integration.m04_support import run_correctness

pytestmark = pytest.mark.integration

if not all(
    (os.environ.get("WAREHOUSE_M5_SOURCE_DIR"), os.environ.get("WAREHOUSE_M5_SOURCE_URL"))
):
    pytestmark = [
        pytest.mark.integration,
        pytest.mark.skip(
            reason="Set WAREHOUSE_M5_SOURCE_DIR and WAREHOUSE_M5_SOURCE_URL for live M04 processing."
        ),
    ]


def test_full_scope_m04_processing_reconciles_independent_parquet_reads(
    tmp_path, hdfs: HDFSVerification
) -> None:
    report = run_correctness(hdfs, tmp_path)

    assert report["status"] == "VALIDATED"
    assert report["configuration"]["expected_row_count"] == 59_181_090
    assert report["independent_reconciliation"]["metrics"]["sales_daily"]["row_count"] == 59_181_090
    assert set(report["worker_metrics"]["normalization"]["workers"]) == {
        "spark-worker-1",
        "spark-worker-2",
    }
    assert set(report["worker_metrics"]["enrichment"]["workers"]) == {
        "spark-worker-1",
        "spark-worker-2",
    }
    print(f"M04 report: {report['evidence_directory']}")
