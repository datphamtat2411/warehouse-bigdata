from dataclasses import replace

import pytest

from tests.unit.test_decision_inputs import DAY, fixture_rows
from warehouse_bigdata.contracts.datasets import (
    ALLOCATION_RECOMMENDATIONS,
    CENTRAL_SUPPLY_SCENARIO,
    INVENTORY_SCENARIO,
    REPLENISHMENT_RECOMMENDATIONS,
)
from warehouse_bigdata.contracts.validation import validate_dataset
from warehouse_bigdata.optimization import decision_batch as batch
from warehouse_bigdata.optimization.decision_inputs import bind_decision_inputs


def build(rows=None, run="decision-03"):
    return batch.build_decision_batch(
        bind_decision_inputs(**(rows or fixture_rows())), decision_run_id=run
    )


def test_contract_mapping_identity_and_supply_constraints():
    result = build()
    for contract, rows in zip(
        (
            INVENTORY_SCENARIO,
            CENTRAL_SUPPLY_SCENARIO,
            REPLENISHMENT_RECOMMENDATIONS,
            ALLOCATION_RECOMMENDATIONS,
        ),
        (
            result.inventory_rows,
            result.central_supply_rows,
            result.replenishment_rows,
            result.allocation_rows,
        ),
        strict=True,
    ):
        validate_dataset(contract, rows)
        assert all(tuple(row) == contract.field_names for row in rows)
    assert len(result.inventory_rows) == len(result.allocation_rows) == 2
    (supply,) = result.central_supply_rows
    assert supply["central_available_units"] == 28
    assert sum(r["allocated_units"] for r in result.allocation_rows) == 28
    for row in result.allocation_rows:
        assert row["decision_run_id"] == "decision-03"
        assert row["allocated_units"] <= row["requested_units"]
        assert row["unmet_request_units"] == row["requested_units"] - row["allocated_units"]
        assert row["central_available_units"] == 28
        assert "feature_version" not in row
    assert result.replenishment_rows[0]["forecast_7d_units"] == 71.75
    assert result.replenishment_rows[0]["planning_demand_units"] == 72
    assert result.decision_manifest_inputs["coverage"]["excluded_series"] == 98
    for metadata in (result.scenario_manifest_inputs, result.decision_manifest_inputs):
        assert "status" not in metadata and "artifact_uri" not in metadata
        assert "configuration_digest" in metadata and "upstream_evidence" in metadata
    assert result.decision_manifest_inputs["policy_id"] == "shortage_first_proportional"
    assert result.decision_manifest_inputs["policy_version"] == "v1"


def test_reordered_inputs_produce_identical_rows_and_manifest_inputs():
    rows = fixture_rows()
    original = build(rows)
    for name in ("forecast_rows", "history_rows", "cohort_rows", "exception_rows"):
        rows[name].reverse()
    rows["scope"]["selected_series"].reverse()
    assert build(rows) == original


def test_decision_run_only_changes_decision_identity():
    first, second = build(), build(run="decision-other")
    assert first.inventory_rows == second.inventory_rows
    assert first.scenario_manifest_inputs == second.scenario_manifest_inputs
    assert all(r["decision_run_id"] == "decision-other" for r in second.replenishment_rows)
    assert first.decision_manifest_inputs != second.decision_manifest_inputs


def test_independent_per_sku_capacity():
    rows = fixture_rows()
    for collection in (rows["forecast_rows"], rows["history_rows"]):
        for row in collection:
            if row["store_id"] == "CA_2":
                row.update(item_id="sku1", store_id="CA_1")
    for row in rows["exception_rows"]:
        if (row["item_id"], row["store_id"]) == ("sku1", "CA_1"):
            row.update(item_id="sku0", store_id="CA_2")
    result = build(rows)
    assert len(result.central_supply_rows) == 2
    for supply in result.central_supply_rows:
        assert supply["central_available_units"] == 14
        assert (
            sum(
                r["allocated_units"]
                for r in result.allocation_rows
                if r["item_id"] == supply["item_id"]
            )
            == 14
        )


@pytest.mark.parametrize("run", [None, "", " ", "../bad", "x" * 300])
def test_explicit_bounded_decision_identity(run):
    with pytest.raises(ValueError):
        build(run=run)


def test_builder_rechecks_input_lineage():
    data = bind_decision_inputs(**fixture_rows())
    prediction = replace(data.predictions[0], model_run_id="wrong")
    with pytest.raises(ValueError):
        batch.build_decision_batch(
            replace(data, predictions=(prediction,) + data.predictions[1:]),
            decision_run_id="decision",
        )


@pytest.mark.parametrize("change", ["membership", "capacity", "request", "lineage"])
def test_cross_dataset_validation_rejects_inconsistent_rows(change):
    result = build()
    rows = [dict(row) for row in result.allocation_rows]
    if change == "membership":
        rows.pop()
    elif change == "capacity":
        for row in rows:
            row.update(
                allocated_units=row["requested_units"],
                unmet_request_units=0,
                critical_allocated_units=row["requested_units"],
                safety_allocated_units=0,
            )
    elif change == "request":
        rows[0]["requested_units"] += 1
        rows[0]["unmet_request_units"] += 1
    else:
        rows[0]["decision_run_id"] = "wrong"
    with pytest.raises(ValueError):
        batch.validate_decision_datasets(
            inputs=bind_decision_inputs(**fixture_rows()),
            decision_run_id="decision-03",
            inventory_rows=result.inventory_rows,
            central_supply_rows=result.central_supply_rows,
            replenishment_rows=result.replenishment_rows,
            allocation_rows=rows,
        )


def test_no_policy_selection_or_publication_side_effects(monkeypatch, capsys):
    from warehouse_bigdata.jobs import inventory_decision_batch as job

    result = build()
    spark = object()
    monkeypatch.setattr(
        job, "read_decision_inputs", lambda *a, **k: bind_decision_inputs(**fixture_rows())
    )
    summary = job.run(spark, decision_run_id="decision-03", client=object(), config=object())
    assert summary["row_counts"]["allocation_recommendations"] == len(result.allocation_rows)
    assert summary["issue_date"] == DAY.isoformat()
    assert "status" not in summary
    assert capsys.readouterr().out == ""
