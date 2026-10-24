"""Serial opt-in verification of existing approved real M5 Small data."""

import os

import pytest

from warehouse_bigdata.jobs.verify_clickhouse_distributed import run_verification

pytestmark = pytest.mark.integration


@pytest.mark.skipif(
    os.environ.get("WAREHOUSE_VERIFY_APPROVED_DATA") != "1",
    reason="requires explicit approved-data live authorization",
)
def test_approved_distributed_publication():
    assert (run_verification() / "evidence.json").is_file()
