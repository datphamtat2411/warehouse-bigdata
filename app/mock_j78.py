"""Demo-only J07 decision simulation and in-memory J08 *mock serving* adapter.

Real J02/J03/J06 data originates from verified M5 Small HDFS extracts.
J07 quantities are hypothetical and deterministic; NO J07 job, ClickHouse
publication, approved pointer, HDFS mutation, or production API is involved.
"""

from __future__ import annotations

import hashlib
import math
from dataclasses import replace

from app.preview_repository import PreviewResult, make_repository

MOCK_POLICIES = ("shortage_first_proportional", "equal_share")
ORIGIN = "calculated_simulated_replenishment_recommendation"


def proportional(demands: dict[str, int], available: int) -> dict[str, int]:
    """Bounded proportional integer apportionment with stable largest remainder."""
    if available < 0 or any(type(n) is not int or n < 0 for n in demands.values()):
        raise ValueError("invalid bounded quantities")
    total = sum(demands.values())
    result = {store: 0 for store in demands}
    if not total or not available:
        return result
    capacity = min(available, total)
    for store, demand in demands.items():
        result[store] = capacity * demand // total
    remaining = capacity - sum(result.values())
    order = sorted(demands, key=lambda key: (-((capacity * demands[key]) % total), key))
    for store in order:
        if remaining <= 0:
            break
        if result[store] < demands[store]:
            result[store] += 1
            remaining -= 1
    if remaining:
        raise ValueError("unallocated remainder")
    return result


def equal_share(demands: dict[str, int], available: int) -> dict[str, int]:
    """Capped progressive equal sharing, deterministic by store ID."""
    if available < 0 or any(type(n) is not int or n < 0 for n in demands.values()):
        raise ValueError("invalid bounded quantities")
    result = {store: 0 for store in demands}
    remaining = min(available, sum(demands.values()))
    while remaining:
        active = [store for store in sorted(demands) if result[store] < demands[store]]
        if not active:
            raise ValueError("unallocated supply")
        portion = max(1, remaining // len(active))
        for store in active:
            grant = min(portion, remaining, demands[store] - result[store])
            result[store] += grant
            remaining -= grant
            if not remaining:
                break
    return result


class MockServingRepository:
    """Supply exact UI read signatures; all writes are deliberately impossible."""

    def __init__(self, real):
        self.real = real
        bindings = dict(real.context.dataset_bindings)
        for dataset in (
            "replenishment_recommendations",
            "central_supply_scenario",
            "allocation_recommendations",
        ):
            bindings[dataset] = {"dataset_scope": "demo_sample", "demo_source": "MOCK_ONLY"}
        self.context = replace(
            real.context,
            publication_id="MOCK-J08-NOT-APPROVED",
            manifest_checksum=real.context.manifest_checksum.replace("preview-only:", "mock-only:"),
            scenario_id="MOCK-J07-SCENARIO",
            decision_run_id="MOCK-J07-DECISION",
            policy_ids=MOCK_POLICIES,
            dataset_bindings=bindings,
        )
        self.limits = real.limits
        self.pairs = real.pairs
        self._replenishment: dict[tuple[str, str], dict] = {}
        self._supply: dict[str, dict] = {}
        self._allocations: dict[tuple[str, str], tuple[dict, ...]] = {}

    def __getattr__(self, name):
        # Read-only J02/J03/J06 operations are forwarded without alteration.
        return getattr(self.real, name)

    def _request(self, item, store):
        key = (item, store)
        if key in self._replenishment:
            return self._replenishment[key]
        response = self.real.get_forecast_series(item, store)
        if response.state != "ok" or len(response.rows) != 7:
            return None
        forecast = sum(float(row["predicted_units"]) for row in response.rows)
        if not math.isfinite(forecast) or forecast < 0:
            return None
        # Illustrative inputs, NOT the project's J07 inventory business rules.
        seed = int.from_bytes(hashlib.sha256(f"{item}|{store}".encode()).digest()[:4], "big")
        demand = math.ceil(forecast)
        safety = math.ceil(demand * 0.20)
        opening = max(0, math.floor(demand * (0.25 + (seed % 35) / 100)))
        critical = max(0, demand - opening)
        target = demand + safety
        request = max(0, target - opening)
        safe_need = request - critical
        ctx = self.context
        row = dict(
            item_id=item,
            store_id=store,
            issue_date=ctx.issue_date,
            source_snapshot_id=ctx.source_snapshot_id,
            forecast_run_id=ctx.forecast_run_id,
            model_run_id=ctx.model_run_id,
            feature_version="MOCK-DECISION-FROM-J06",
            scenario_id=ctx.scenario_id,
            decision_run_id=ctx.decision_run_id,
            policy_version="replenishment_v1",
            origin_label=ORIGIN,
            forecast_7d_units=forecast,
            planning_demand_units=demand,
            opening_stock_units=opening,
            safety_stock_units=safety,
            target_stock_units=target,
            critical_need_units=critical,
            safety_need_units=safe_need,
            request_units=request,
            forecast_shortage_flag=int(critical > 0),
        )
        self._replenishment[key] = row
        return row

    def get_replenishment(self, item, store):
        row = self._request(item, store)
        if row is None:
            return PreviewResult("empty", provenance="calculated")
        return PreviewResult("ok", (row,), "calculated")

    def _central(self, item):
        if item in self._supply:
            return self._supply[item]
        requests = [self._request(item, store) for store in self.context.coverage.available_stores]
        if any(row is None for row in requests):
            return None
        total = sum(row["request_units"] for row in requests)
        # Supply shortage is a hypothetical classroom demonstration parameter.
        supply = math.floor(total * 0.70)
        ctx = self.context
        row = dict(
            item_id=item,
            scenario_id=ctx.scenario_id,
            issue_date=ctx.issue_date,
            origin_label="simulated_central_supply",
            source_warehouse_id="MOCK-WH-01",
            central_available_units=supply,
        )
        self._supply[item] = row
        return row

    def get_central_supply(self, item):
        row = self._central(item)
        return (
            PreviewResult("ok", (row,), "simulated")
            if row is not None
            else PreviewResult("empty", provenance="simulated")
        )

    def get_allocation(self, item, policy):
        if policy not in MOCK_POLICIES:
            return PreviewResult("error", provenance="calculated")
        key = (item, policy)
        if key in self._allocations:
            return PreviewResult("ok", self._allocations[key], "calculated")
        central = self._central(item)
        if central is None:
            return PreviewResult("incomplete", provenance="calculated")
        stores = sorted(self.context.coverage.available_stores)
        requests = {store: self._request(item, store) for store in stores}
        if any(value is None for value in requests.values()):
            return PreviewResult("incomplete", provenance="calculated")
        critical_need = {store: row["critical_need_units"] for store, row in requests.items()}
        safety_need = {store: row["safety_need_units"] for store, row in requests.items()}
        capacity = central["central_available_units"]
        if policy == "shortage_first_proportional":
            critical_alloc = proportional(critical_need, capacity)
            extra = proportional(safety_need, capacity - sum(critical_alloc.values()))
            allocated = {store: critical_alloc[store] + extra[store] for store in stores}
        else:
            allocated = equal_share(
                {store: row["request_units"] for store, row in requests.items()}, capacity
            )
            critical_alloc = {
                store: min(allocated[store], critical_need[store]) for store in stores
            }
            extra = {store: allocated[store] - critical_alloc[store] for store in stores}
        ctx = self.context
        rows = tuple(
            dict(
                item_id=item,
                store_id=store,
                issue_date=ctx.issue_date,
                source_snapshot_id=ctx.source_snapshot_id,
                forecast_run_id=ctx.forecast_run_id,
                model_run_id=ctx.model_run_id,
                scenario_id=ctx.scenario_id,
                decision_run_id=ctx.decision_run_id,
                policy_id=policy,
                policy_version="v1",
                source_warehouse_id=central["source_warehouse_id"],
                central_available_units=capacity,
                requested_units=requests[store]["request_units"],
                allocated_units=allocated[store],
                unmet_request_units=requests[store]["request_units"] - allocated[store],
                critical_allocated_units=critical_alloc[store],
                safety_allocated_units=extra[store],
            )
            for store in stores
        )
        self._allocations[key] = rows
        return PreviewResult("ok", rows, "calculated")

    def get_policy_evaluation(self, item):
        # NEVER manufacture historical outcomes/accuracy from forward forecasts.
        return PreviewResult("unavailable", provenance="evaluated")


def make_mock_repository(base, manifest, selector, signature):
    return MockServingRepository(make_repository(base, manifest, selector, signature))
