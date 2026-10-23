from dataclasses import asdict, replace
from datetime import date, timedelta

import pytest

from warehouse_bigdata.contracts.datasets import ALLOCATION_RECOMMENDATIONS
from warehouse_bigdata.contracts.validation import validate_record
from warehouse_bigdata.optimization.allocation import (
    AllocationError, CentralSupply, allocate_equal_share_v1,
    allocate_shortage_first_proportional_v1,
)
from warehouse_bigdata.optimization.replenishment import (
    ForecastPrediction, InventoryPosition, calculate_replenishment,
)


DAY = date(2026, 1, 1)


POLICIES = (allocate_shortage_first_proportional_v1, allocate_equal_share_v1)


def request(store, demand, safety=0, item="sku"):
    forecasts = [ForecastPrediction(
        item_id=item, store_id=store, issue_date=DAY, horizon=h,
        prediction_date=DAY + timedelta(days=h),
        predicted_units=demand if h == 1 else 0,
        forecast_run_id="forecast", model_run_id="model",
        source_snapshot_id="snapshot", feature_version="features",
    ) for h in range(1, 8)]
    position = InventoryPosition(item, store, DAY, "scenario", "snapshot", 0, safety)
    return calculate_replenishment(forecasts, [position])[0]


def supply(capacity, item="sku"):
    return CentralSupply(item, DAY, "scenario", "warehouse", capacity)


@pytest.mark.parametrize("policy", POLICIES)
@pytest.mark.parametrize("capacity", [0, 1, 4, 9, 100])
def test_conservation_caps_order_and_projection(policy, capacity):
    requests = [request("B", 2, 1), request("A", 4, 2)]
    result = policy(requests, [supply(capacity)])
    assert result == policy(reversed(requests), [supply(capacity)])
    assert [r.store_id for r in result.recommendations] == ["A", "B"]
    total = 0
    for row in result.recommendations:
        assert 0 <= row.allocated_units <= row.requested_units
        assert row.unmet_request_units == row.requested_units - row.allocated_units
        total += row.allocated_units
        record = asdict(row)
        record.pop("feature_version")
        record.pop("origin_label")
        record["issue_date"] = DAY.isoformat()
        record["decision_run_id"] = "decision"
        record["central_available_units"] = capacity
        validate_record(ALLOCATION_RECOMMENDATIONS, record)
    assert total == min(capacity, 9)
    assert result.central_remainders == (("sku", capacity - total),)


def test_critical_before_safety_and_exact_ties():
    result = allocate_shortage_first_proportional_v1(
        [request("B", 2, 10), request("A", 2, 10)], [supply(3)])
    assert [(r.critical_allocated_units, r.safety_allocated_units) for r in
            result.recommendations] == [(2, 0), (1, 0)]
    result = allocate_shortage_first_proportional_v1(
        [request("B", 2, 10), request("A", 2, 10)], [supply(7)])
    assert [(r.critical_allocated_units, r.safety_allocated_units) for r in
            result.recommendations] == [(2, 2), (2, 1)]


def test_large_exact_integer_remainders():
    # Above float's exact integer range; B has the larger exact remainder.
    a = request("A", 0, 2**60)
    b = request("B", 0, 2**60 + 1)
    result = allocate_shortage_first_proportional_v1([a, b], [supply(1)])
    assert [r.allocated_units for r in result.recommendations] == [0, 1]


def test_capped_equal_share_and_large_capacity():
    result = allocate_equal_share_v1(
        [request("C", 0, 100), request("B", 0, 100), request("A", 0, 1)], [supply(8)])
    assert [r.allocated_units for r in result.recommendations] == [1, 4, 3]
    assert all(r.critical_allocated_units is None and r.safety_allocated_units is None
               for r in result.recommendations)
    result = allocate_equal_share_v1(
        [request("B", 0, 2**62), request("A", 0, 1)], [supply(2**62)])
    assert [r.allocated_units for r in result.recommendations] == [1, 2**62 - 1]


@pytest.mark.parametrize("policy", POLICIES)
def test_sku_isolation_and_zero_requests(policy):
    result = policy([request("A", 0), request("B", 0, 5, "other")],
                    [supply(100), supply(1, "other")])
    assert [(r.item_id, r.allocated_units) for r in result.recommendations] == [
        ("other", 1), ("sku", 0)]


@pytest.mark.parametrize("policy", POLICIES)
def test_reject_invalid_cohorts(policy):
    r = request("A", 1)
    for requests, central in (([], []), ([r], []), ([r, r], [supply(1)]),
                              ([r], [supply(1), supply(1)]),
                              ([r], [supply(1), supply(1, "extra")]),
                              ([r], [replace(supply(1), scenario_id="other")]),
                              ([r], [replace(supply(1), issue_date=DAY.replace(day=2))]),
                              ([r, replace(request("B", 1), forecast_run_id="other")],
                               [supply(1)]), (None, []), ([object()], [supply(1)])):
        with pytest.raises(AllocationError):
            policy(requests, central)
    with pytest.raises(ValueError):
        policy([replace(r, request_units=100)], [supply(1)])


@pytest.mark.parametrize("quantity", [-1, True, 2**63, 0.5])
def test_invalid_central_quantity(quantity):
    with pytest.raises(AllocationError):
        supply(quantity)
