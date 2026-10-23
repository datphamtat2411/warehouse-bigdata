from dataclasses import asdict, replace
from datetime import date, timedelta

import pytest

from warehouse_bigdata.contracts.datasets import REPLENISHMENT_RECOMMENDATIONS
from warehouse_bigdata.contracts.validation import validate_record
from warehouse_bigdata.optimization.replenishment import (
    ForecastPrediction, InventoryPosition, ReplenishmentError, calculate_replenishment,
)


DAY = date(2026, 1, 1)


def predictions(item="sku", store="A", value=0.1):
    return tuple(ForecastPrediction(
        item_id=item, store_id=store, issue_date=DAY, horizon=h,
        prediction_date=DAY + timedelta(days=h), predicted_units=value,
        forecast_run_id="forecast", model_run_id="model",
        source_snapshot_id="snapshot", feature_version="features",
    ) for h in range(1, 8))


def inventory(item="sku", store="A", opening=0, safety=2):
    return InventoryPosition(item, store, DAY, "scenario", "snapshot", opening, safety)


def test_aggregate_once_formulas_and_projection():
    result, = calculate_replenishment(predictions(), [inventory()])
    assert result.forecast_7d_units == pytest.approx(0.7)
    assert (result.planning_demand_units, result.target_stock_units,
            result.critical_need_units, result.safety_need_units, result.request_units) == (1, 3, 1, 2, 3)
    assert result.forecast_shortage_flag is True
    row = asdict(result)
    row.pop("feature_version")
    row["issue_date"] = DAY.isoformat()
    row["decision_run_id"] = "decision"
    validate_record(REPLENISHMENT_RECOMMENDATIONS, row)


def test_zero_request_and_order_independence():
    forecasts = predictions(store="B") + predictions()
    positions = [inventory(store="B", opening=10), inventory(opening=10)]
    result = calculate_replenishment(forecasts, positions)
    assert all(r.request_units == 0 and not r.forecast_shortage_flag for r in result)
    assert [r.store_id for r in result] == ["A", "B"]
    assert result == calculate_replenishment(reversed(forecasts), reversed(positions))


@pytest.mark.parametrize("change", [
    lambda p: p[:-1], lambda p: p + p[:1],
    lambda p: (replace(p[0], prediction_date=DAY),) + p[1:],
    lambda p: (replace(p[0], forecast_run_id="other"),) + p[1:],
    lambda p: (replace(p[0], model_run_id="other"),) + p[1:],
    lambda p: (replace(p[0], feature_version="other"),) + p[1:],
    lambda p: (replace(p[0], source_snapshot_id="other"),) + p[1:],
    lambda p: (replace(p[0], horizon=0),) + p[1:],
])
def test_invalid_forecast_cohort(change):
    with pytest.raises(ReplenishmentError):
        calculate_replenishment(change(predictions()), [inventory()])


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -1, True, "1"])
def test_invalid_prediction(value):
    with pytest.raises(ReplenishmentError):
        calculate_replenishment(predictions(value=value), [inventory()])


@pytest.mark.parametrize("positions", [[], [inventory(), inventory()],
    [inventory(store="B")], [inventory(), replace(inventory(store="B"), scenario_id="other")]])
def test_invalid_inventory_membership(positions):
    with pytest.raises(ReplenishmentError):
        calculate_replenishment(predictions(), positions)


@pytest.mark.parametrize("value", [2**63, -1, True, 1.5])
def test_invalid_quantity(value):
    with pytest.raises(ReplenishmentError):
        inventory(opening=value)


def test_overflow_and_wrong_input_types():
    for value in (1e308, float(2**63)):
        with pytest.raises(ReplenishmentError):
            calculate_replenishment(predictions(value=value), [inventory()])
    with pytest.raises(ReplenishmentError):
        calculate_replenishment(predictions(value=1), [inventory(safety=2**63 - 1)])
    for p, i in ((None, []), ([], []), ([object()], [inventory()])):
        with pytest.raises(ReplenishmentError):
            calculate_replenishment(p, i)
