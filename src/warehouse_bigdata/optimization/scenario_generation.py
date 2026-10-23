"""Deterministic simulated stock from an explicitly bound observed-sales window.

Callers validate upstream frozen-scope evidence before supplying its opaque digest
and membership here. This module performs no selection, I/O, or forecast inference.
The fixed factors are scenario assumptions, not calibrated inventory estimates.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, localcontext

from warehouse_bigdata.contracts.datasets import CENTRAL_SUPPLY_SCENARIO, INVENTORY_SCENARIO
from warehouse_bigdata.contracts.validation import validate_record

_ALPHA = Decimal("0.35")
_GAMMA = Decimal("0.20")
_BETA = Decimal("0.20")
_VERSION = "1.0"
_GENERATOR_VERSION = "historical_scaled_v1"
_ORIGIN = "SIMULATED_FROM_M5_HISTORY"
_WAREHOUSE = "SIMULATED_CENTRAL_WAREHOUSE"
_MAX_INT64 = 2**63 - 1


class ScenarioGenerationError(ValueError):
    """Scenario inputs or generated quantities violate the domain contract."""


@dataclass(frozen=True, slots=True)
class EligiblePair:
    """One explicitly supplied member of the validated upstream population."""

    item_id: str
    store_id: str


@dataclass(frozen=True, slots=True)
class ObservedDailySale:
    """J03-shaped observed unit sales, never forecasts or inventory measurements."""

    source_snapshot_id: str
    item_id: str
    store_id: str
    date: date
    unit_sales: int


@dataclass(frozen=True, slots=True)
class GeneratedScenario:
    """Contract v1 records and only the scenario-specific manifest fields.

    Dates and scenario_parameters use the existing contracts' string encoding.
    A persistence layer must supply any full artifact manifest envelope.
    """

    inventory_rows: tuple[Mapping[str, object], ...]
    central_supply_rows: tuple[Mapping[str, object], ...]
    metadata: Mapping[str, object]


def _identifier(value: object, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ScenarioGenerationError(f"{name} must be a nonblank string")


def _quantity(value: object, name: str) -> None:
    if type(value) is not int or not 0 <= value <= _MAX_INT64:
        raise ScenarioGenerationError(f"{name} must be a nonnegative signed 64-bit integer")


def generate_historical_scaled(
    *,
    issue_date: date,
    source_snapshot_id: str,
    selection_digest: str,
    eligible_pairs: Iterable[EligiblePair],
    observed_sales: Iterable[ObservedDailySale],
) -> GeneratedScenario:
    """Generate fixed-factor stock from exactly seven dates per eligible series.

    All supplied observations must belong to T-6..T and the explicit population;
    extra history is rejected rather than filtered, and missing dates are errors.
    The selection digest is an opaque, already-validated upstream identity.
    """
    _identifier(source_snapshot_id, "source_snapshot_id")
    _identifier(selection_digest, "selection_digest")
    if type(issue_date) is not date:
        raise ScenarioGenerationError("issue_date must be a calendar date")
    try:
        dates = tuple(issue_date - timedelta(days=n) for n in range(6, -1, -1))
    except OverflowError as exc:
        raise ScenarioGenerationError("issue_date cannot support seven days of history") from exc
    try:
        pairs = tuple(eligible_pairs)
        sales = tuple(observed_sales)
    except TypeError as exc:
        raise ScenarioGenerationError("population and sales must be iterable") from exc
    if not pairs:
        raise ScenarioGenerationError("eligible population must not be empty")

    population: set[tuple[str, str]] = set()
    for pair in pairs:
        if not isinstance(pair, EligiblePair):
            raise ScenarioGenerationError("population requires EligiblePair records")
        _identifier(pair.item_id, "item_id")
        _identifier(pair.store_id, "store_id")
        key = (pair.item_id, pair.store_id)
        if key in population:
            raise ScenarioGenerationError(f"duplicate population membership: {key}")
        population.add(key)

    observations: dict[tuple[str, str, date], int] = {}
    for sale in sales:
        if not isinstance(sale, ObservedDailySale):
            raise ScenarioGenerationError("sales require ObservedDailySale records")
        _identifier(sale.source_snapshot_id, "source_snapshot_id")
        _identifier(sale.item_id, "item_id")
        _identifier(sale.store_id, "store_id")
        _quantity(sale.unit_sales, "unit_sales")
        if sale.source_snapshot_id != source_snapshot_id:
            raise ScenarioGenerationError("observation source_snapshot_id does not match")
        if (sale.item_id, sale.store_id) not in population:
            raise ScenarioGenerationError("observation is outside the eligible population")
        if type(sale.date) is not date or sale.date not in dates:
            raise ScenarioGenerationError("observation date must be within T-6..T; no future data")
        key = (sale.item_id, sale.store_id, sale.date)
        if key in observations:
            raise ScenarioGenerationError(f"duplicate observation: {key}")
        observations[key] = sale.unit_sales
    if len(observations) != len(population) * 7:
        raise ScenarioGenerationError("each eligible series requires exactly seven observed dates")

    ordered_pairs = sorted(population)
    document = {
        "canonical_input_version": "1.0",
        "scenario_version": _VERSION,
        "scenario_generator_version": _GENERATOR_VERSION,
        "scenario_type": "historical_scaled",
        "source_snapshot_id": source_snapshot_id,
        "selection_digest": selection_digest,
        "issue_date": issue_date.isoformat(),
        "lookback_dates": [day.isoformat() for day in dates],
        "eligible_population": [dict(item_id=item, store_id=store) for item, store in ordered_pairs],
        "observed_sales": [
            dict(item_id=item, store_id=store, date=day.isoformat(), unit_sales=units)
            for (item, store, day), units in sorted(observations.items())
        ],
        "factors": {"alpha": str(_ALPHA), "gamma": str(_GAMMA), "beta": str(_BETA)},
        "rounding": {
            "opening_stock_units": "floor",
            "safety_stock_units": "ceiling",
            "central_available_units": "floor",
        },
        "origin_label": _ORIGIN,
        "source_warehouse_id": _WAREHOUSE,
        "scenario_seed": None,
    }
    canonical = json.dumps(document, sort_keys=True, ensure_ascii=True,
                           separators=(",", ":"), allow_nan=False)
    # Match the repository canonical-byte convention, including its final newline.
    digest = hashlib.sha256((canonical + "\n").encode("utf-8")).hexdigest()
    scenario_id = "historical_scaled:" + digest
    metadata = {
        "scenario_id": scenario_id,
        "scenario_version": _VERSION,
        "source_snapshot_id": source_snapshot_id,
        "issue_date": issue_date.isoformat(),
        "scenario_type": "historical_scaled",
        "scenario_seed": None,
        "scenario_parameters": canonical,
        "parameter_digest": "sha256:" + digest,
        "scenario_generator_version": _GENERATOR_VERSION,
        "origin_label": _ORIGIN,
    }
    totals = {pair: sum(observations[(*pair, day)] for day in dates) for pair in ordered_pairs}
    item_totals: dict[str, int] = {}
    for (item, _), total in totals.items():
        item_totals[item] = item_totals.get(item, 0) + total

    inventory_rows = []
    central_rows = []
    # Explicit precision makes arithmetic exact even if a caller changes its
    # Decimal context. Aggregates may exceed Int64; persisted quantities may not.
    with localcontext() as context:
        context.prec = max(28, len(str(max(item_totals.values()))) + 4)
        for (item, store), total in totals.items():
            inventory_rows.append({
                "scenario_id": scenario_id,
                "scenario_version": _VERSION,
                "scenario_type": "historical_scaled",
                "source_snapshot_id": source_snapshot_id,
                "issue_date": issue_date.isoformat(),
                "item_id": item,
                "store_id": store,
                "opening_stock_units": int((Decimal(total) * _ALPHA).to_integral_value(
                    rounding=ROUND_FLOOR)),
                "safety_stock_units": int((Decimal(total) * _GAMMA).to_integral_value(
                    rounding=ROUND_CEILING)),
                "parameter_digest": metadata["parameter_digest"],
                "origin_label": _ORIGIN,
                "scenario_seed": None,
            })
        for item, total in sorted(item_totals.items()):
            central_rows.append({
                "scenario_id": scenario_id,
                "issue_date": issue_date.isoformat(),
                "item_id": item,
                "source_warehouse_id": _WAREHOUSE,
                "central_available_units": int((Decimal(total) * _BETA).to_integral_value(
                    rounding=ROUND_FLOOR)),
                "origin_label": _ORIGIN,
            })

    for contract, rows, quantities in (
        (INVENTORY_SCENARIO, inventory_rows, ("opening_stock_units", "safety_stock_units")),
        (CENTRAL_SUPPLY_SCENARIO, central_rows, ("central_available_units",)),
    ):
        keys = set()
        for row in rows:
            validate_record(contract, row)
            key = tuple(row[field] for field in contract.unique_key)
            if key in keys or row["origin_label"] != _ORIGIN:
                raise ScenarioGenerationError("generated key or provenance invariant failed")
            keys.add(key)
            if contract is INVENTORY_SCENARIO and row["scenario_seed"] is not None:
                raise ScenarioGenerationError("historical_scaled must have a null scenario_seed")
            for field in quantities:
                _quantity(row[field], field)
    if len(central_rows) != len(item_totals) or {r["item_id"] for r in central_rows} != set(item_totals):
        raise ScenarioGenerationError("central supply requires exactly one row per item")
    return GeneratedScenario(tuple(inventory_rows), tuple(central_rows), metadata)
