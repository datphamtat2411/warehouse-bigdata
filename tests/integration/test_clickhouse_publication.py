"""Explicitly authorized destructive offline reload against the existing two shards."""

import os

import pytest


@pytest.mark.integration
def test_selected_small_offline_publication():
    if os.environ.get("WAREHOUSE_RUN_PUBLICATION_INTEGRATION") != "confirm-offline-reload":
        pytest.skip("requires explicit WAREHOUSE_RUN_PUBLICATION_INTEGRATION authorization")
    from warehouse_bigdata.jobs.publish_clickhouse import run_publish_clickhouse

    result = run_publish_clickhouse(
        selector_path=os.environ["WAREHOUSE_SMALL_ARTIFACTS"],
        decision_run_id=os.environ["WAREHOUSE_PUBLICATION_DECISION_RUN_ID"],
        decision_pipeline_run_id=os.environ["WAREHOUSE_PUBLICATION_DECISION_PIPELINE_RUN_ID"],
        policy_evaluation_run_id=os.environ["WAREHOUSE_PUBLICATION_POLICY_EVALUATION_RUN_ID"],
        policy_evaluation_pipeline_run_id=os.environ["WAREHOUSE_PUBLICATION_POLICY_EVALUATION_PIPELINE_RUN_ID"],
        forecast_evaluation_run_id=os.environ["WAREHOUSE_PUBLICATION_FORECAST_EVALUATION_RUN_ID"],
        offline_reload=True, confirm_offline_reload=True,
    )
    assert result.status == "APPROVED"
    assert result.reconciliation.validated
    assert set(result.reconciliation.loaded_rows_per_shard["sales_daily"]) == {1, 2}
