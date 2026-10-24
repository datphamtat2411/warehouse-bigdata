"""Synthetic paired-policy evaluation: labels are supplied only after freezing."""

from dataclasses import replace
from datetime import timedelta
from unittest.mock import Mock

import pytest

from tests.unit.test_decision_inputs import historical_rows
from warehouse_bigdata.contracts.datasets import INVENTORY_POLICY_EVALUATION
from warehouse_bigdata.contracts.validation import validate_dataset
from warehouse_bigdata.optimization import policy_evaluation as evaluation
from warehouse_bigdata.optimization.decision_inputs import bind_historical_inputs, plain


def frozen(rows=None):
    return evaluation.freeze_policy_decisions(
        bind_historical_inputs(**(rows or historical_rows())),
        proposed_decision_run_id="proposed",
        baseline_decision_run_id="baseline",
    )


def labels(batch, units=10):
    return [
        dict(
            source_snapshot_id="small-snapshot",
            item_id=r.item_id,
            store_id=r.store_id,
            date=evaluation.HISTORICAL_ISSUE_DATE + timedelta(days=h),
            unit_sales=units,
        )
        for r in batch.requests
        for h in range(1, 8)
    ]


def score(batch, rows):
    return evaluation.evaluate_frozen_policies(batch, rows, evaluation_run_id="policy-eval")


def test_exact_arithmetic_contract_pairing_reset_and_metadata():
    batch = frozen()
    result = score(batch, labels(batch))
    validate_dataset(INVENTORY_POLICY_EVALUATION, result.rows)
    assert len(result.rows) == 4
    for row in result.rows:
        assert row["actual_sales_proxy_7d_units"] == 70
        assert row["simulated_fulfilled_units"] == 38  # floor(.35*70) + 14
        assert row["simulated_unmet_units"] == 32
        assert row["ending_stock_units"] == row["excess_over_safety_units"] == 0
        assert row["simulated_stockout_days"] == 4
        assert row["run_type"] == "test"
    assert result.metadata["model_run_id"] == "model"
    assert result.metadata["decision_freeze_digest"] == batch.digest
    assert result.coverage["paired_series"] == 2
    assert result.metrics["proposed_minus_baseline"]["simulated_fulfilled_units"] == 0
    assert {r["decision_run_id"] for r in result.rows} == {"proposed", "baseline"}
    assert len({r["scenario_id"] for r in result.rows}) == 1
    for row in result.daily_rows:
        assert (
            row["simulated_fulfilled_units"] + row["simulated_unmet_units"]
            == row["observed_sales_proxy_units"]
        )
        assert (
            row["simulated_fulfilled_units"] + row["remaining_stock_units"]
            == row["opening_stock_units"]
        )
    for supply in batch.supplies:
        for allocations in batch.allocations:
            assert (
                sum(
                    r.allocated_units
                    for r in allocations.recommendations
                    if r.item_id == supply.item_id
                )
                + dict(allocations.central_remainders)[supply.item_id]
                == supply.central_available_units
            )


def test_determinism_and_shared_request_objects(monkeypatch):
    proposed, baseline = (
        evaluation.allocate_shortage_first_proportional_v1,
        evaluation.allocate_equal_share_v1,
    )
    calls = []

    def track(fn):
        def run(requests, supplies):
            calls.append((requests, supplies))
            return fn(requests, supplies)

        return run

    monkeypatch.setattr(evaluation, "allocate_shortage_first_proportional_v1", track(proposed))
    monkeypatch.setattr(evaluation, "allocate_equal_share_v1", track(baseline))
    first = frozen()
    assert calls[0][0] is calls[1][0] and calls[0][1] is calls[1][1]
    rows = historical_rows()
    for key in ("forecast_rows", "history_rows", "eligibility_rows", "exception_rows"):
        rows[key].reverse()
    second = frozen(rows)
    assert first.digest == second.digest
    assert plain(score(first, labels(first)).rows) == plain(
        score(second, reversed(labels(second))).rows
    )


@pytest.mark.parametrize("kind", ["missing", "negative", "null", "boolean"])
def test_incomplete_or_invalid_labels_exclude_both_policies(kind):
    batch = frozen()
    rows = labels(batch)
    if kind == "missing":
        rows.pop()
    else:
        rows[-1]["unit_sales"] = {"negative": -1, "null": None, "boolean": True}[kind]
    result = score(batch, rows)
    assert len(result.rows) == 2
    assert result.coverage["paired_series"] == 1
    assert any(e.stage == "LABELS" for e in result.exclusions)


@pytest.mark.parametrize("kind", ["duplicate", "future", "snapshot", "outsider"])
def test_conflicting_labels_fail(kind):
    batch = frozen()
    rows = labels(batch)
    if kind == "duplicate":
        rows.append(rows[0])
    elif kind == "future":
        rows[0]["date"] += timedelta(days=7)
    elif kind == "snapshot":
        rows[0]["source_snapshot_id"] = "wrong"
    else:
        rows[0]["store_id"] = "outside"
    with pytest.raises(ValueError):
        score(batch, rows)


def test_zero_and_empty_demand_have_null_pooled_rate():
    batch = frozen()
    for rows in (labels(batch, 0), []):
        result = score(batch, rows)
        assert all(
            m["fulfillment_rate"] is None
            for k, m in result.metrics.items()
            if k != "proposed_minus_baseline"
        )


def test_distinct_decision_ids_required():
    with pytest.raises(ValueError, match="distinct"):
        evaluation.freeze_policy_decisions(
            bind_historical_inputs(**historical_rows()),
            proposed_decision_run_id="same",
            baseline_decision_run_id="same",
        )


def test_negative_results_are_retained():
    rows = historical_rows()
    for row in rows["forecast_rows"]:
        row["predicted_units"] = row["raw_prediction"] = 100 if row["store_id"] == "CA_1" else 10
    batch = frozen(rows)
    actual = labels(batch, 0)
    for row in actual:
        if row["store_id"] == "CA_2":
            row["unit_sales"] = 10
    result = score(batch, actual)
    assert result.metrics["proposed_minus_baseline"]["simulated_fulfilled_units"] < 0
    assert result.metrics["proposed_minus_baseline"]["simulated_unmet_units"] > 0


def test_tampered_freeze_rejected():
    batch = replace(frozen(), digest="wrong")
    with pytest.raises(ValueError, match="freeze"):
        score(batch, labels(batch))


def test_all_history_excluded_is_empty_evidence_not_zero_outcomes():
    rows = historical_rows()
    rows["history_rows"] = []
    batch = frozen(rows)
    result = score(batch, [])
    assert not result.rows
    assert result.coverage["decision_eligible_series"] == 0
    assert result.coverage["excluded_series"] == 100


def test_forecast_lineage_cannot_be_changed_at_freeze_boundary():
    bound = bind_historical_inputs(**historical_rows())
    changed = replace(bound.predictions[0], model_run_id="wrong-model")
    with pytest.raises(ValueError, match="lineage"):
        evaluation.freeze_policy_decisions(
            replace(bound, predictions=(changed, *bound.predictions[1:])),
            proposed_decision_run_id="proposed",
            baseline_decision_run_id="baseline",
        )


def test_orchestrator_freezes_before_label_read(monkeypatch):
    from warehouse_bigdata.jobs import inventory_policy_evaluation as job

    events = []
    bound = bind_historical_inputs(**historical_rows())
    monkeypatch.setattr(job, "read_historical_inputs", lambda *a, **k: bound)
    real_freeze = evaluation.freeze_policy_decisions

    def freeze(*a, **k):
        events.append("freeze")
        return real_freeze(*a, **k)

    def read(*a, **k):
        assert events == ["freeze"]
        events.append("labels")
        return labels(k["frozen"])

    monkeypatch.setattr(job, "freeze_policy_decisions", freeze)
    monkeypatch.setattr(job, "read_historical_labels", read)
    spark, client = Mock(), Mock()
    result = job.run_inventory_policy_evaluation(
        spark,
        client=client,
        config=Mock(),
        proposed_decision_run_id="proposed",
        baseline_decision_run_id="baseline",
        evaluation_run_id="policy-eval",
    )
    assert len(result.rows) == 4 and events == ["freeze", "labels"]
    assert not spark.mock_calls and not client.mock_calls
