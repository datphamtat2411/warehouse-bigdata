"""Pure seven-day replenishment recommendations from simulated inventory."""

from __future__ import annotations

import math
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, timedelta

_MAX_INT64 = 2**63 - 1


class ReplenishmentError(ValueError):
    """Invalid replenishment input or calculated quantity."""


def _identifier(value: object, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ReplenishmentError(f"{name} must be a nonblank string")


def _quantity(value: object, name: str) -> None:
    if type(value) is not int or not 0 <= value <= _MAX_INT64:
        raise ReplenishmentError(f"{name} must be a nonnegative signed 64-bit integer")


def _date(value: object) -> None:
    if type(value) is not date:
        raise ReplenishmentError("issue_date must be a calendar date")


def _prediction(value: object) -> None:
    if type(value) not in (float, int):
        raise ReplenishmentError("predicted units must be finite and nonnegative")
    try:
        valid = math.isfinite(value) and value >= 0
    except OverflowError:
        valid = False
    if not valid:
        raise ReplenishmentError("predicted units must be finite and nonnegative")


def _lineage(record: object) -> tuple:
    return (record.source_snapshot_id, record.forecast_run_id, record.model_run_id,
            record.feature_version, record.issue_date)


@dataclass(frozen=True, slots=True)
class ForecastPrediction:
    item_id: str
    store_id: str
    issue_date: date
    horizon: int
    prediction_date: date
    predicted_units: float
    forecast_run_id: str
    model_run_id: str
    source_snapshot_id: str
    feature_version: str

    def __post_init__(self) -> None:
        for name in ("item_id", "store_id", "forecast_run_id", "model_run_id",
                     "source_snapshot_id", "feature_version"):
            _identifier(getattr(self, name), name)
        _date(self.issue_date)
        if type(self.horizon) is not int or self.horizon not in range(1, 8):
            raise ReplenishmentError("horizon must be 1..7")
        try:
            expected = self.issue_date + timedelta(days=self.horizon)
        except OverflowError as exc:
            raise ReplenishmentError("forecast date overflow") from exc
        if type(self.prediction_date) is not date or self.prediction_date != expected:
            raise ReplenishmentError("prediction_date must equal issue_date + horizon")
        _prediction(self.predicted_units)


@dataclass(frozen=True, slots=True)
class InventoryPosition:
    item_id: str
    store_id: str
    issue_date: date
    scenario_id: str
    source_snapshot_id: str
    opening_stock_units: int
    safety_stock_units: int

    def __post_init__(self) -> None:
        for name in ("item_id", "store_id", "scenario_id", "source_snapshot_id"):
            _identifier(getattr(self, name), name)
        _date(self.issue_date)
        _quantity(self.opening_stock_units, "opening_stock_units")
        _quantity(self.safety_stock_units, "safety_stock_units")


@dataclass(frozen=True, slots=True)
class ReplenishmentRecommendation:
    item_id: str
    store_id: str
    issue_date: date
    scenario_id: str
    source_snapshot_id: str
    forecast_run_id: str
    model_run_id: str
    feature_version: str
    forecast_7d_units: float
    planning_demand_units: int
    opening_stock_units: int
    safety_stock_units: int
    target_stock_units: int
    critical_need_units: int
    safety_need_units: int
    request_units: int
    forecast_shortage_flag: bool
    policy_version: str = "replenishment_v1"
    origin_label: str = "calculated_simulated_replenishment_recommendation"

    def __post_init__(self) -> None:
        for name in ("item_id", "store_id", "scenario_id", "source_snapshot_id",
                     "forecast_run_id", "model_run_id", "feature_version"):
            _identifier(getattr(self, name), name)
        _date(self.issue_date)
        _prediction(self.forecast_7d_units)
        for name in ("planning_demand_units", "opening_stock_units", "safety_stock_units",
                     "target_stock_units", "critical_need_units", "safety_need_units",
                     "request_units"):
            _quantity(getattr(self, name), name)
        demand = math.ceil(self.forecast_7d_units)
        target = demand + self.safety_stock_units
        request = max(0, target - self.opening_stock_units)
        critical = max(0, demand - self.opening_stock_units)
        if (self.planning_demand_units != demand or self.target_stock_units != target
                or self.request_units != request or self.critical_need_units != critical
                or self.safety_need_units != request - critical
                or type(self.forecast_shortage_flag) is not bool
                or self.forecast_shortage_flag != (self.opening_stock_units < demand)):
            raise ReplenishmentError("invalid replenishment equations")
        if (self.policy_version != "replenishment_v1" or self.origin_label !=
                "calculated_simulated_replenishment_recommendation"):
            raise ReplenishmentError("invalid replenishment policy or provenance")


def calculate_replenishment(
    predictions: Iterable[ForecastPrediction], inventory: Iterable[InventoryPosition],
) -> tuple[ReplenishmentRecommendation, ...]:
    """Require exact cohort membership; sum unrounded predictions before ceiling."""
    try:
        forecasts, positions = tuple(predictions), tuple(inventory)
    except TypeError as exc:
        raise ReplenishmentError("predictions and inventory must be iterable") from exc
    if not forecasts or not positions:
        raise ReplenishmentError("decision cohort must not be empty")
    grouped: dict[tuple[str, str], dict[int, ForecastPrediction]] = {}
    lineage = None
    for prediction in forecasts:
        if not isinstance(prediction, ForecastPrediction):
            raise ReplenishmentError("expected ForecastPrediction")
        prediction.__post_init__()
        if lineage is None:
            lineage = _lineage(prediction)
        if _lineage(prediction) != lineage:
            raise ReplenishmentError("forecast cohort lineage mismatch")
        group = grouped.setdefault((prediction.item_id, prediction.store_id), {})
        if prediction.horizon in group:
            raise ReplenishmentError("duplicate forecast horizon")
        group[prediction.horizon] = prediction
    stocks = {}
    scenario = None
    for position in positions:
        if not isinstance(position, InventoryPosition):
            raise ReplenishmentError("expected InventoryPosition")
        position.__post_init__()
        if scenario is None:
            scenario = position.scenario_id
        if (position.scenario_id != scenario or position.source_snapshot_id != lineage[0]
                or position.issue_date != lineage[-1]):
            raise ReplenishmentError("inventory cohort lineage mismatch")
        key = (position.item_id, position.store_id)
        if key in stocks:
            raise ReplenishmentError("duplicate inventory position")
        stocks[key] = position
    if stocks.keys() != grouped.keys():
        raise ReplenishmentError("forecast and inventory membership must match exactly")
    results = []
    for key, stock in sorted(stocks.items()):
        group = grouped[key]
        if set(group) != set(range(1, 8)):
            raise ReplenishmentError("exactly seven forecast horizons are required")
        try:
            total = math.fsum(group[h].predicted_units for h in range(1, 8))
        except OverflowError as exc:
            raise ReplenishmentError("forecast sum overflow") from exc
        _prediction(total)
        demand = math.ceil(total)
        target = demand + stock.safety_stock_units
        critical = max(0, demand - stock.opening_stock_units)
        request = max(0, target - stock.opening_stock_units)
        results.append(ReplenishmentRecommendation(
            item_id=stock.item_id, store_id=stock.store_id, issue_date=stock.issue_date,
            scenario_id=stock.scenario_id, source_snapshot_id=stock.source_snapshot_id,
            forecast_run_id=lineage[1], model_run_id=lineage[2], feature_version=lineage[3],
            forecast_7d_units=total, planning_demand_units=demand,
            opening_stock_units=stock.opening_stock_units,
            safety_stock_units=stock.safety_stock_units, target_stock_units=target,
            critical_need_units=critical, safety_need_units=request - critical,
            request_units=request, forecast_shortage_flag=stock.opening_stock_units < demand,
        ))
    return tuple(results)
