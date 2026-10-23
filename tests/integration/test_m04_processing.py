"""Read-only real-M5 Small correctness; reuse existing validated publications."""

import pytest

from tests.integration.small_support import readback

pytestmark = [pytest.mark.integration, pytest.mark.m5_small]


def test_small_processing_reconciles_independent_parquet_reads(tmp_path):
    report = readback(tmp_path, through="j03")
    assert report["status"] == "VALIDATED"
    assert report["verified_through"] == "j03"
    assert report["dataset_scope"] == "demo_sample"
    metrics = report["processing"]["metrics"]
    assert metrics["sales_daily"]["row_count"] == 194100
    assert metrics["sales_enriched"]["row_count"] == 194100
    assert metrics["sales_daily"]["zero_sales_count"] == metrics["raw"]["zero_sales_count"]
    assert metrics["sales_enriched"]["price_reconciliation_mismatches"] == 0
    assert (
        metrics["sales_enriched"]["nullable_price_count"]
        + metrics["sales_enriched"]["matched_price_count"]
        == 194100
    )
    for name in ("sales_daily", "sales_enriched", "store_daily_sales", "category_monthly_sales"):
        assert metrics[name]["unit_sales_total"] == metrics["raw"]["unit_sales_total"]
