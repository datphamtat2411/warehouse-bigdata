"""Offline persisted-artifact reconciliation checks for M06."""

from datetime import date
from types import SimpleNamespace

import pytest

from warehouse_bigdata.contracts.dataset_scope import ScopeError


def _scope():
    return {
        "dataset_scope": "demo_sample",
        "selected_sku_count": 10,
        "selected_series_count": 100,
        "selected_store_ids": [
            "CA_1",
            "CA_2",
            "CA_3",
            "CA_4",
            "TX_1",
            "TX_2",
            "TX_3",
            "WI_1",
            "WI_2",
            "WI_3",
        ],
        "historical_day_count": 1941,
        "expected_observation_count": 194100,
        "original_series_count": 30490,
        "selected_series": [
            {"item_id": f"item-{item}", "store_id": store}
            for item in range(10)
            for store in (
                "CA_1",
                "CA_2",
                "CA_3",
                "CA_4",
                "TX_1",
                "TX_2",
                "TX_3",
                "WI_1",
                "WI_2",
                "WI_3",
            )
        ],
    }


def _forecast_rows():
    return [
        {
            "forecast_run_id": "forecast",
            "model_run_id": "model",
            "source_snapshot_id": "snapshot",
            "feature_version": "historical-v1",
            "issue_date": date(2016, 5, 22),
            "forecast_date": date(2016, 5, 22 + horizon),
            "horizon": horizon,
            "item_id": "item-0",
            "store_id": "CA_1",
            "raw_prediction": 1.5,
            "predicted_units": 1.5,
            "run_type": "forward_demo",
        }
        for horizon in range(1, 8)
    ]


def test_scope_history_and_training_selection_gates_are_explicit():
    from warehouse_bigdata.forecasting.acceptance import (
        validate_canonical_scope,
        validate_training_selection,
    )

    validate_canonical_scope(_scope())
    with pytest.raises(ScopeError, match="history"):
        validate_canonical_scope({**_scope(), "historical_day_count": 30})

    keys = [
        {
            "source_snapshot_id": "snapshot",
            "feature_version": "historical-v1",
            "feature_role": "train",
            "item_id": "item-0",
            "store_id": "CA_1",
            "target_date": "2016-03-27",
        }
    ]
    validate_training_selection(
        keys,
        selected_feature_keys=keys,
        summary={"selected_count": 1, "seed": 42},
        cap=10000,
    )
    with pytest.raises(ScopeError, match="d_29"):
        validate_training_selection(
            [{**keys[0], "target_date": "2011-01-29"}],
            selected_feature_keys=keys,
            summary={"selected_count": 1, "seed": 42},
            cap=10000,
        )


def test_forecast_contract_reconciles_seven_dates_and_coverage():
    from warehouse_bigdata.forecasting.acceptance import validate_forecast_rows

    report = validate_forecast_rows(
        _forecast_rows(),
        selector={
            "forecast_run_id": "forecast",
            "model_run_id": "model",
            "source_snapshot_id": "snapshot",
            "feature_version": "historical-v1",
        },
        candidate_series=1,
        excluded_series=0,
    )
    assert report["row_count"] == 7
    assert report["successful_series"] == 1
    with pytest.raises(ScopeError, match="seven"):
        validate_forecast_rows(
            _forecast_rows()[:-1], selector={}, candidate_series=1, excluded_series=0
        )


def test_forecast_contract_allows_signed_raw_predictions_but_reconciles_clamped_units():
    from warehouse_bigdata.forecasting.acceptance import validate_forecast_rows

    rows = _forecast_rows()
    rows[0]["raw_prediction"] = -1.5
    rows[0]["predicted_units"] = 0.0
    validate_forecast_rows(rows, selector={}, candidate_series=1, excluded_series=0)


def test_reloaded_model_metadata_is_compared_to_persisted_training_evidence():
    from warehouse_bigdata.forecasting.acceptance import validate_model_metadata

    class Stage:
        def __init__(self, *, labels=None, inputs=None):
            self.labels = labels
            self._inputs = inputs

        def getInputCols(self):
            return self._inputs

    model = SimpleNamespace(
        stages=[
            None,
            Stage(labels=["CA_1", "CA_2"]),
            Stage(labels=["FOODS_1"]),
            None,
            None,
            Stage(inputs=["lag_1", "lag_7"]),
        ]
    )
    schema = {
        "predictor_order": ["lag_1", "lag_7"],
        "vector_layout": {"num_attrs": 2},
    }
    training = {
        "category_mappings": {"store_id": ["CA_1", "CA_2"], "dept_id": ["FOODS_1"]},
        "imputed_price_median": 2.5,
    }
    validate_model_metadata(
        model,
        schema=schema,
        training=training,
        vector_layout={"num_attrs": 2},
        imputed_price_median=2.5,
    )
    model.stages[1].labels.append("CA_3")
    with pytest.raises(ScopeError, match="categorical"):
        validate_model_metadata(
            model,
            schema=schema,
            training=training,
            vector_layout={"num_attrs": 2},
            imputed_price_median=2.5,
        )


def test_evaluation_alignment_and_coverage_reconcile_pairs_and_origins():
    from warehouse_bigdata.forecasting.acceptance import (
        validate_evaluation_alignment,
        validate_evaluation_coverage,
        validate_paired_rows,
    )

    issue = date(2016, 3, 27)
    selector = {
        "model_run_id": "model",
        "source_snapshot_id": "snapshot",
        "feature_version": "historical-v1",
    }
    pairs = [
        {
            "item_id": "item-0",
            "store_id": "CA_1",
            "issue_date": issue,
            "target_date": issue.fromordinal(issue.toordinal() + horizon),
            "horizon": horizon,
            "forecast_run_id": "eval-rf",
            "model_run_id": "model",
            "source_snapshot_id": "snapshot",
            "feature_version": "historical-v1",
            "rf_prediction": 2.0,
            "baseline_prediction": 1.0,
            "actual_units": 0.0,
            "baseline_source_date": issue.fromordinal(issue.toordinal() + horizon - 7),
        }
        for horizon in range(1, 8)
    ]
    expected = [
        {key: row[key] for key in ("item_id", "store_id", "issue_date", "target_date", "horizon")}
        for row in pairs
    ]
    alignment = [
        {
            **row,
            "eligible": True,
            "label_valid": True,
            "paired": True,
        }
        for row in pairs
    ]
    validate_paired_rows(
        pairs,
        expected_count=None,
        selector=selector,
        expected_issue_dates=(issue,),
        expected_forecast_ids={issue: "eval-rf"},
    )
    assert (
        validate_evaluation_alignment(
            alignment,
            pairs,
            [],
            expected,
            selector=selector,
            expected_forecast_ids={issue: "eval-rf"},
        )["paired_rows"]
        == 7
    )
    coverage = [
        {
            "issue_date": issue,
            "method_id": method,
            "candidate_rows": 7,
            "eligible_rows": 7,
            "predicted_rows": 7,
            "label_aligned_rows": 7,
            "paired_rows": 7,
            "prediction_difference_rows": 0,
            "dropped_rows": 0,
            "fully_dropped_series": 0,
            "partially_paired_series": 0,
            "complete_paired_series": 1,
        }
        for method in ("random_forest", "seasonal_naive")
    ]
    assert (
        validate_evaluation_coverage(coverage, alignment, expected_issue_dates=(issue,))[
            "coverage_rows"
        ]
        == 2
    )


def test_reference_metrics_reconciles_pairs_and_preserves_zero_wape():
    from warehouse_bigdata.forecasting.acceptance import reference_metrics

    pairs = [
        {
            "item_id": "item-0",
            "store_id": "CA_1",
            "issue_date": date(2020, 1, 1),
            "target_date": date(2020, 1, 2),
            "horizon": 1,
            "rf_prediction": 2.0,
            "baseline_prediction": 1.0,
            "actual_units": 0.0,
        }
    ]
    rows = reference_metrics(pairs, evaluation_run_id="eval", model_run_id="model", run_type="test")
    assert len(rows) == 48
    assert (
        next(
            row
            for row in rows
            if row["method_id"] == "random_forest"
            and row["horizon"] == 1
            and row["metric_name"] == "WAPE"
        )["metric_value"]
        is None
    )
    assert all(row["evaluation_run_id"] == "eval" for row in rows)


@pytest.mark.parametrize("field", ["forecast_run_id", "source_snapshot_id", "feature_version"])
def test_forecast_contract_rejects_lineage_mutation(field):
    from warehouse_bigdata.forecasting.acceptance import validate_forecast_rows

    rows = _forecast_rows()
    rows[0][field] = "wrong"
    with pytest.raises(ScopeError, match="lineage"):
        validate_forecast_rows(
            rows,
            selector={
                "forecast_run_id": "forecast",
                "model_run_id": "model",
                "source_snapshot_id": "snapshot",
                "feature_version": "historical-v1",
            },
            candidate_series=1,
            excluded_series=0,
        )


def test_explicit_historical_forecast_validation_preserves_forward_defaults():
    from datetime import timedelta

    from warehouse_bigdata.forecasting.acceptance import validate_forecast_rows

    rows = _forecast_rows()
    issue = date(2016, 4, 24)
    for row in rows:
        row.update(
            issue_date=issue, forecast_date=issue + timedelta(days=row["horizon"]), run_type="test"
        )
    assert (
        validate_forecast_rows(
            rows,
            selector={},
            candidate_series=1,
            excluded_series=0,
            issue_date=issue,
            run_type="test",
        )["row_count"]
        == 7
    )
    with pytest.raises(ScopeError):
        validate_forecast_rows(rows, selector={}, candidate_series=1, excluded_series=0)
