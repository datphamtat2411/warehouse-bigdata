"""Synthetic product-wide allocation integrity checks."""

from dataclasses import replace

import pytest

from tests.unit.test_application_read_repository import context
from warehouse_bigdata.application.read_repository import ReadResult


def fixture():
    ctx = context()
    ctx = replace(
        ctx,
        policy_ids=("shortage_first_proportional", "equal_share"),
        coverage=replace(ctx.coverage, available_stores=("A", "B")),
    )
    supply = dict(
        item_id="I",
        scenario_id="scenario",
        issue_date=ctx.issue_date,
        source_warehouse_id="W",
        central_available_units=6,
        origin_label="simulated_central_supply",
    )
    rows = tuple(
        dict(
            item_id="I",
            store_id=s,
            policy_id=ctx.policy_ids[0],
            policy_version="v1",
            issue_date=ctx.issue_date,
            source_warehouse_id="W",
            central_available_units=6,
            requested_units=5,
            allocated_units=3,
            unmet_request_units=2,
            critical_allocated_units=2,
            safety_allocated_units=1,
            **{
                k: getattr(ctx, k)
                for k in (
                    "source_snapshot_id",
                    "model_run_id",
                    "forecast_run_id",
                    "scenario_id",
                    "decision_run_id",
                )
            },
        )
        for s in ("A", "B")
    )
    return ctx, supply, rows


def normalize(rows=None, supply=None):
    from app.distribution import normalize_allocation

    ctx, original, allocations = fixture()
    return normalize_allocation(
        ReadResult("ok", (original if supply is None else supply,), "simulated"),
        ReadResult("ok", allocations if rows is None else rows, "calculated"),
        ctx,
        "I",
        ctx.policy_ids[0],
    )


def test_capacity_once_and_chart():
    from app.distribution import allocation_figure, allocation_totals

    result = normalize()
    assert result.state == "ok"
    assert allocation_totals(fixture()[1], result.rows) == (6, 10, 6, 4)
    figure = allocation_figure(result.rows)
    assert figure.layout.barmode == "group"
    assert all(t.orientation == "h" and list(t.y) == ["A", "B"] for t in figure.data)


@pytest.mark.parametrize(
    "field",
    [
        "requested_units",
        "allocated_units",
        "unmet_request_units",
        "central_available_units",
        "critical_allocated_units",
        "safety_allocated_units",
    ],
)
@pytest.mark.parametrize("value", [None, True, -1, 1.5, "3"])
def test_invalid_quantities(field, value):
    rows = fixture()[2]
    result = normalize((dict(rows[0], **{field: value}), rows[1]))
    assert result.state == "incomplete" and not result.rows


@pytest.mark.parametrize(
    "change",
    [
        dict(source_warehouse_id="other"),
        dict(source_snapshot_id="other"),
        dict(issue_date="2016-05-22"),
        dict(policy_version="v2"),
        dict(allocated_units=6),
        dict(unmet_request_units=0),
        dict(critical_allocated_units=0),
        dict(store_id="C"),
    ],
)
def test_inconsistent_rows(change):
    rows = fixture()[2]
    assert normalize((dict(rows[0], **change), rows[1])).state == "incomplete"


def test_incomplete_coverage():
    rows = fixture()[2]
    for bad in (rows[:1], rows + rows[:1], (rows[0], rows[0])):
        assert normalize(bad).state == "incomplete"


def test_nullable_baseline():
    from app.distribution import normalize_allocation

    ctx, supply, rows = fixture()
    rows = tuple(
        dict(r, policy_id="equal_share", critical_allocated_units=None, safety_allocated_units=None)
        for r in rows
    )
    assert (
        normalize_allocation(
            ReadResult("ok", (supply,), "simulated"),
            ReadResult("ok", rows, "calculated"),
            ctx,
            "I",
            "equal_share",
        ).state
        == "ok"
    )


def test_history_complete_pair_and_fail_closed():
    from datetime import timedelta

    from app.distribution import comparison_totals, normalize_comparison

    ctx, _, _ = fixture()
    binding = dict(
        evaluation_run_id="E",
        source_snapshot_id="source",
        forecast_run_id="HF",
        scenario_id="HS",
        issue_date=ctx.issue_date.isoformat(),
        run_type="test",
        policy_ids=list(ctx.policy_ids),
        decision_run_ids={p: "D" + p for p in ctx.policy_ids},
        label_provenance="observed_m5_unit_sales_demand_proxy",
    )
    ctx = replace(
        ctx, dataset_bindings=dict(ctx.dataset_bindings, inventory_policy_evaluation=binding)
    )
    rows = tuple(
        dict(
            evaluation_run_id="E",
            source_snapshot_id="source",
            forecast_run_id="HF",
            scenario_id="HS",
            issue_date=ctx.issue_date,
            run_type="test",
            item_id="I",
            store_id=s,
            policy_id=p,
            policy_version="v1",
            decision_run_id="D" + p,
            observed_label_window_start=ctx.issue_date + timedelta(days=1),
            observed_label_window_end=ctx.issue_date + timedelta(days=7),
            actual_sales_proxy_7d_units=10,
            simulated_fulfilled_units=8,
            simulated_unmet_units=2,
            ending_stock_units=3,
            excess_over_safety_units=1,
            simulated_stockout_days=1,
        )
        for p in ctx.policy_ids
        for s in ("A", "B")
    )

    def check(payload):
        return normalize_comparison(ReadResult("ok", payload, "evaluated"), ctx, "I")

    assert check(rows).state == "ok"
    assert comparison_totals(rows)[0]["fulfillment"] == 0.8
    for payload in (
        rows[:3],
        rows + rows[:1],
        (dict(rows[0], forecast_run_id="wrong"), *rows[1:]),
        (dict(rows[0], simulated_stockout_days=8), *rows[1:]),
    ):
        assert check(payload).state == "incomplete" and not check(payload).rows
    zero = tuple(
        dict(r, actual_sales_proxy_7d_units=0, simulated_fulfilled_units=0, simulated_unmet_units=0)
        for r in rows
    )
    assert check(zero).state == "ok"
    assert comparison_totals(zero)[0]["fulfillment"] is None


def render_page():
    import streamlit as st

    from app.distribution import render_allocation
    from warehouse_bigdata.application.session_state import selections

    render_allocation(
        st.session_state["ctx"], st.session_state["repo"], selections(st.session_state)
    )


def test_product_wide_render_and_cleanup():
    from unittest.mock import Mock

    from streamlit.testing.v1 import AppTest

    from warehouse_bigdata.application.session_state import PREFIX

    ctx, supply, rows = fixture()
    repo = Mock()
    repo.get_eligible_items.side_effect = lambda store, search, limit: ReadResult(
        "ok", ({"item_id": "I", "store_id": store},), "predicted"
    )
    repo.get_central_supply.return_value = ReadResult("ok", (supply,), "simulated")
    repo.get_allocation.return_value = ReadResult("ok", rows, "calculated")
    repo.get_policy_evaluation.return_value = ReadResult("unavailable", reason="secret")
    at = AppTest.from_function(render_page)
    at.session_state["ctx"], at.session_state["repo"] = ctx, repo
    at.session_state[PREFIX + "item_id"] = "I"
    at.session_state[PREFIX + "policy_id"] = ctx.policy_ids[0]
    for store in ("A", "B"):
        at.session_state[PREFIX + "store_id"] = store
        at.run()
        assert not at.exception and len(at.metric) == 4
        assert [m.value for m in at.metric] == ["6 đơn vị", "10 đơn vị", "6 đơn vị", "4 đơn vị"]
        assert len(at.dataframe[0].value) == 2
        repo.get_allocation.assert_called_with("I", ctx.policy_ids[0])
    assert repo.get_central_supply.call_count == 2
    repo.get_allocation.return_value = ReadResult("incomplete", reason="secret")
    at.run()
    assert not at.exception and not at.metric and not at.dataframe and not at.get("plotly_chart")
