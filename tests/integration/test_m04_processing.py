"""Opt-in full-scope M04 J02 to J03 correctness acceptance."""

from __future__ import annotations

import os

import pytest

from tests.integration.hdfs_support import HDFSVerification
from tests.integration.m04_support import run_correctness

pytestmark = pytest.mark.integration

if not all((os.environ.get("WAREHOUSE_M5_SOURCE_DIR"), os.environ.get("WAREHOUSE_M5_SOURCE_URL"))):
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
    reconciliation = report["independent_reconciliation"]
    metrics = reconciliation["metrics"]
    assert metrics["sales_daily"]["row_count"] == 59_181_090
    assert metrics["sales_enriched"]["row_count"] == 59_181_090
    assert metrics["sales_daily"]["zero_sales_count"] == metrics["raw"]["zero_sales_count"]
    assert metrics["sales_enriched"]["price_reconciliation_mismatches"] == 0
    assert (
        metrics["sales_enriched"]["nullable_price_count"]
        + metrics["sales_enriched"]["matched_price_count"]
        == 59_181_090
    )
    assert metrics["store_daily_sales"]["unit_sales_total"] == metrics["raw"]["unit_sales_total"]
    assert (
        metrics["category_monthly_sales"]["unit_sales_total"] == metrics["raw"]["unit_sales_total"]
    )
    assert set(report["lineage"]["paths"]) == {
        "sales_daily",
        "sales_enriched",
        "store_daily_sales",
        "category_monthly_sales",
    }

    recovery = report["recovery"]
    assert recovery["enabled"] is True
    assert recovery["j02"]["retry_status"] == "REUSED"
    assert recovery["j03"]["retry_status"] == "REUSED"
    assert recovery["j02"]["immutable_unchanged"] is True
    assert recovery["j03"]["immutable_unchanged"] is True
    assert set(recovery["j03"]["outputs"]) == {
        "sales_enriched",
        "store_daily_sales",
        "category_monthly_sales",
    }

    for phase in ("normalization", "enrichment", "verification"):
        workers = report["worker_metrics"][phase]["workers"]
        assert set(workers) == {"spark-worker-1", "spark-worker-2"}
        assert all(worker["successful_tasks"] > 0 for worker in workers.values())
        assert all(worker["memory_spill_bytes"] >= 0 for worker in workers.values())
        assert all(worker["disk_spill_bytes"] >= 0 for worker in workers.values())

    assert report["storage"]["sales_daily"]["file_count"] > 0
    assert report["storage"]["sales_daily"]["files"][0]["sha256"].startswith("sha256:")
    print(f"M04 report: {report['evidence_directory']}")
