"""Exact, deterministic allocation against independent per-SKU capacities."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date

from .replenishment import (
    ReplenishmentError,
    ReplenishmentRecommendation,
    _date,
    _identifier,
    _lineage,
    _quantity,
)

SHORTAGE_FIRST_PROPORTIONAL_V1 = "shortage_first_proportional_v1"
EQUAL_SHARE_V1 = "equal_share_v1"


class AllocationError(ValueError):
    """Invalid allocation cohort, request, capacity, or result."""


@dataclass(frozen=True, slots=True)
class CentralSupply:
    item_id: str
    issue_date: date
    scenario_id: str
    source_warehouse_id: str
    central_available_units: int

    def __post_init__(self) -> None:
        try:
            for name in ("item_id", "scenario_id", "source_warehouse_id"):
                _identifier(getattr(self, name), name)
            _date(self.issue_date)
            _quantity(self.central_available_units, "central_available_units")
        except ReplenishmentError as exc:
            raise AllocationError(str(exc)) from exc


@dataclass(frozen=True, slots=True)
class AllocationRecommendation:
    item_id: str
    store_id: str
    issue_date: date
    scenario_id: str
    source_snapshot_id: str
    forecast_run_id: str
    model_run_id: str
    feature_version: str
    source_warehouse_id: str
    policy_id: str
    policy_version: str
    requested_units: int
    critical_allocated_units: int | None
    safety_allocated_units: int | None
    allocated_units: int
    unmet_request_units: int
    origin_label: str = "calculated_simulated_allocation_recommendation"

    @property
    def policy_key(self) -> str:
        return f"{self.policy_id}_{self.policy_version}"

    def __post_init__(self) -> None:
        try:
            for name in (
                "item_id",
                "store_id",
                "scenario_id",
                "source_snapshot_id",
                "forecast_run_id",
                "model_run_id",
                "feature_version",
                "source_warehouse_id",
            ):
                _identifier(getattr(self, name), name)
            _date(self.issue_date)
            for name in ("requested_units", "allocated_units", "unmet_request_units"):
                _quantity(getattr(self, name), name)
            if (
                self.allocated_units > self.requested_units
                or self.unmet_request_units != self.requested_units - self.allocated_units
            ):
                raise AllocationError("invalid allocation equations")
            if self.policy_key == SHORTAGE_FIRST_PROPORTIONAL_V1:
                _quantity(self.critical_allocated_units, "critical_allocated_units")
                _quantity(self.safety_allocated_units, "safety_allocated_units")
                if (
                    self.critical_allocated_units + self.safety_allocated_units
                    != self.allocated_units
                ):
                    raise AllocationError("stage allocations must sum to total")
            elif self.policy_key == EQUAL_SHARE_V1:
                if (
                    self.critical_allocated_units is not None
                    or self.safety_allocated_units is not None
                ):
                    raise AllocationError("equal-share stage allocations must be None")
            else:
                raise AllocationError("unsupported allocation policy")
            if self.origin_label != "calculated_simulated_allocation_recommendation":
                raise AllocationError("invalid allocation provenance")
        except ReplenishmentError as exc:
            raise AllocationError(str(exc)) from exc


@dataclass(frozen=True, slots=True)
class AllocationBatchResult:
    """One result per request and one (item_id, unused capacity) per SKU."""

    recommendations: tuple[AllocationRecommendation, ...]
    central_remainders: tuple[tuple[str, int], ...]

    def __post_init__(self) -> None:
        if (
            type(self.recommendations) is not tuple
            or not self.recommendations
            or type(self.central_remainders) is not tuple
        ):
            raise AllocationError("batch requires nonempty immutable results")
        keys = []
        identities = set()
        for row in self.recommendations:
            if not isinstance(row, AllocationRecommendation):
                raise AllocationError("expected AllocationRecommendation")
            row.__post_init__()
            keys.append((row.item_id, row.store_id))
            identities.add((_lineage(row), row.scenario_id, row.policy_key))
        if keys != sorted(set(keys)) or len(identities) != 1:
            raise AllocationError("batch keys or lineage mismatch")
        items = []
        for entry in self.central_remainders:
            if type(entry) is not tuple or len(entry) != 2:
                raise AllocationError("invalid central remainder")
            item, quantity = entry
            try:
                _identifier(item, "item_id")
                _quantity(quantity, "central remainder")
            except ReplenishmentError as exc:
                raise AllocationError(str(exc)) from exc
            items.append(item)
        if items != sorted({item for item, _ in keys}):
            raise AllocationError("central remainders must match requested SKUs")


def _apportion(needs: dict[str, int], supply: int) -> dict[str, int]:
    """Integer largest remainder; intermediates intentionally use unbounded ints."""
    total_need = sum(needs.values())
    capacity = min(supply, total_need)
    if total_need == 0:
        return dict.fromkeys(needs, 0)
    allocated = {}
    remainder_numerators = {}
    for store, need in needs.items():
        allocated[store], remainder_numerators[store] = divmod(capacity * need, total_need)
    residual = capacity - sum(allocated.values())
    ranking = sorted(needs, key=lambda store: (-remainder_numerators[store], store))
    for store in ranking[:residual]:
        allocated[store] += 1
    return allocated


def _equal_share(needs: dict[str, int], supply: int) -> dict[str, int]:
    """Capped water-filling in at most one step per distinct store cap."""
    remaining = min(supply, sum(needs.values()))
    ordered = sorted(needs, key=lambda store: (needs[store], store))
    allocated = {}
    level = 0
    for index, store in enumerate(ordered):
        active_count = len(ordered) - index
        cost = (needs[store] - level) * active_count
        if cost <= remaining:
            remaining -= cost
            level = needs[store]
            allocated[store] = level
        else:
            increment, residual = divmod(remaining, active_count)
            for rank, active in enumerate(sorted(ordered[index:])):
                allocated[active] = level + increment + (rank < residual)
            break
    return allocated


def _allocate(requests, central_supply, policy_id: str) -> AllocationBatchResult:
    try:
        requests, central_supply = tuple(requests), tuple(central_supply)
    except TypeError as exc:
        raise AllocationError("requests and central supply must be iterable") from exc
    if not requests:
        raise AllocationError("request cohort must not be empty")
    grouped = {}
    identity = None
    try:
        for request in requests:
            if not isinstance(request, ReplenishmentRecommendation):
                raise AllocationError("expected ReplenishmentRecommendation")
            request.__post_init__()
            current = (_lineage(request), request.scenario_id, request.policy_version)
            if identity is None:
                identity = current
            if current != identity:
                raise AllocationError("request cohort lineage mismatch")
            stores = grouped.setdefault(request.item_id, {})
            if request.store_id in stores:
                raise AllocationError("duplicate store request")
            stores[request.store_id] = request
    except ReplenishmentError as exc:
        raise AllocationError(str(exc)) from exc
    capacities = {}
    for supply in central_supply:
        if not isinstance(supply, CentralSupply):
            raise AllocationError("expected CentralSupply")
        supply.__post_init__()
        if supply.item_id in capacities:
            raise AllocationError("duplicate central supply")
        if supply.scenario_id != identity[1] or supply.issue_date != identity[0][-1]:
            raise AllocationError("central supply scenario/date mismatch")
        capacities[supply.item_id] = supply
    if capacities.keys() != grouped.keys():
        raise AllocationError("central supply must match requested SKUs exactly")
    rows, remainders = [], []
    for item, stores in sorted(grouped.items()):
        supply = capacities[item]
        capacity = supply.central_available_units
        if policy_id == "shortage_first_proportional":
            critical = _apportion({s: r.critical_need_units for s, r in stores.items()}, capacity)
            safety = _apportion(
                {s: r.safety_need_units for s, r in stores.items()},
                capacity - sum(critical.values()),
            )
            allocated = {s: critical[s] + safety[s] for s in stores}
        else:
            critical = safety = dict.fromkeys(stores, None)
            allocated = _equal_share({s: r.request_units for s, r in stores.items()}, capacity)
        total = sum(allocated.values())
        if total != min(capacity, sum(r.request_units for r in stores.values())):
            raise AllocationError("per-SKU capacity conservation failed")
        remainders.append((item, capacity - total))
        for store, request in sorted(stores.items()):
            if policy_id == "shortage_first_proportional" and (
                critical[store] > request.critical_need_units
                or safety[store] > request.safety_need_units
            ):
                raise AllocationError("stage allocation exceeds need")
            rows.append(
                AllocationRecommendation(
                    item_id=item,
                    store_id=store,
                    issue_date=request.issue_date,
                    scenario_id=request.scenario_id,
                    source_snapshot_id=request.source_snapshot_id,
                    forecast_run_id=request.forecast_run_id,
                    model_run_id=request.model_run_id,
                    feature_version=request.feature_version,
                    source_warehouse_id=supply.source_warehouse_id,
                    policy_id=policy_id,
                    policy_version="v1",
                    requested_units=request.request_units,
                    critical_allocated_units=critical[store],
                    safety_allocated_units=safety[store],
                    allocated_units=allocated[store],
                    unmet_request_units=request.request_units - allocated[store],
                )
            )
    return AllocationBatchResult(tuple(rows), tuple(remainders))


def allocate_shortage_first_proportional_v1(
    requests: Iterable[ReplenishmentRecommendation],
    central_supply: Iterable[CentralSupply],
) -> AllocationBatchResult:
    """Apportion critical demand first, then safety, with exact remainder ties."""
    return _allocate(requests, central_supply, "shortage_first_proportional")


def allocate_equal_share_v1(
    requests: Iterable[ReplenishmentRecommendation],
    central_supply: Iterable[CentralSupply],
) -> AllocationBatchResult:
    """Allocate capped equal shares, with ascending store IDs for residual units."""
    return _allocate(requests, central_supply, "equal_share")
