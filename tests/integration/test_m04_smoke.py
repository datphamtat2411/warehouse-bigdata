"""Opt-in 30-day M04 smoke acceptance; this does not accept full history."""

from __future__ import annotations

import pytest

from tests.integration.hdfs_support import HDFSVerification
from tests.integration.m04_support import run_smoke

pytestmark = [pytest.mark.integration, pytest.mark.m04_smoke]


def test_m04_smoke_reconciles_first_30_days(tmp_path, hdfs: HDFSVerification) -> None:
    report = run_smoke(hdfs, tmp_path)

    assert report["status"] == "VALIDATED"
    assert report["acceptance_scope"] == "reduced-history"
    assert report["full_history_accepted"] is False
    assert report["configuration"]["expected_series_count"] == 30_490
    assert report["configuration"]["expected_day_count"] == 30
    assert report["configuration"]["expected_row_count"] == 914_700
    verification = report["independent_reconciliation"]
    assert verification["source_version"] == report["lineage"]["source_version"]
    assert verification["source_snapshot_id"] == report["lineage"]["source_snapshot_id"]
    metrics = verification["metrics"]
    assert metrics["raw"]["series_count"] == 30_490
    assert metrics["raw"]["selected_day_count"] == 30
    for name in ("sales_daily", "sales_enriched"):
        assert metrics[name]["row_count"] == 914_700
        assert metrics[name]["series_count"] == 30_490
        assert metrics[name]["day_count"] == 30
        assert metrics[name]["zero_sales_count"] == metrics["raw"]["zero_sales_count"]
        assert metrics[name]["unit_sales_total"] == metrics["raw"]["unit_sales_total"]
        assert metrics[name]["unique_key_duplicates"] == 0
    assert metrics["sales_enriched"]["price_reconciliation_mismatches"] == 0
    assert (
        metrics["sales_enriched"]["nullable_price_count"]
        + metrics["sales_enriched"]["matched_price_count"]
    ) == 914_700
    for name in ("store_daily_sales", "category_monthly_sales"):
        assert metrics[name]["unit_sales_total"] == metrics["raw"]["unit_sales_total"]
    assert metrics["lineage"]["pipeline_manifests_validated"] is True
    assert report["recovery"]["enabled"] is False
    for phase in ("normalization", "enrichment", "verification"):
        workers = report["worker_metrics"][phase]["workers"]
        assert set(workers) == {"spark-worker-1", "spark-worker-2"}
        assert all(worker["successful_tasks"] > 0 for worker in workers.values())
    assert set(report["storage"]) == {
        "sales_daily",
        "sales_enriched",
        "store_daily_sales",
        "category_monthly_sales",
    }
    for storage in report["storage"].values():
        assert storage["file_count"] > 0
        assert all(file["sha256"].startswith("sha256:") for file in storage["files"])
    print(f"M04 smoke report (not full-history acceptance): {report['evidence_directory']}")
