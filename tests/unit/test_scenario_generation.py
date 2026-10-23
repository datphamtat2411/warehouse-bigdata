"""Hand-calculated synthetic cases for the pure historical scenario boundary."""

import hashlib
import json
from dataclasses import FrozenInstanceError, replace
from datetime import date, datetime, timedelta
from decimal import localcontext

import pytest

from warehouse_bigdata.contracts.datasets import CENTRAL_SUPPLY_SCENARIO, INVENTORY_SCENARIO
from warehouse_bigdata.contracts.manifests import SCENARIO_MANIFEST
from warehouse_bigdata.contracts.validation import validate_dataset, validate_record
from warehouse_bigdata.optimization.scenario_generation import (
    EligiblePair,
    ObservedDailySale,
    ScenarioGenerationError,
    generate_historical_scaled,
)

ISSUE_DATE = date(2016, 3, 27)
MAX_INT64 = 2**63 - 1


def inputs():
    pairs = (EligiblePair("B", "S2"), EligiblePair("A", "S2"), EligiblePair("A", "S1"))
    # A/S1 H=8, A/S2 H=3, B/S2 H=0. Central A=floor(0.20*11)=2,
    # rather than the sum of per-store floors (1+0).
    histories = ((0, 1, 2, 0, 1, 1, 3), (0, 0, 0, 0, 0, 0, 3), (0,) * 7)
    sales = tuple(
        ObservedDailySale("snapshot", item, store, ISSUE_DATE - timedelta(days=6 - n), units)
        for (item, store), history in zip((("A", "S1"), ("A", "S2"), ("B", "S2")), histories)
        for n, units in enumerate(history)
    )
    return dict(issue_date=ISSUE_DATE, source_snapshot_id="snapshot",
                selection_digest="opaque-validated-scope", eligible_pairs=pairs,
                observed_sales=sales)


def test_formulas_aggregation_order_and_contract_v1():
    result = generate_historical_scaled(**inputs())
    assert [(r["item_id"], r["store_id"], r["opening_stock_units"], r["safety_stock_units"])
            for r in result.inventory_rows] == [("A", "S1", 2, 2), ("A", "S2", 1, 1),
                                               ("B", "S2", 0, 0)]
    assert [(r["item_id"], r["central_available_units"]) for r in result.central_supply_rows] == [
        ("A", 2), ("B", 0)]
    validate_dataset(INVENTORY_SCENARIO, result.inventory_rows)
    validate_dataset(CENTRAL_SUPPLY_SCENARIO, result.central_supply_rows)
    for contract, rows in ((INVENTORY_SCENARIO, result.inventory_rows),
                           (CENTRAL_SUPPLY_SCENARIO, result.central_supply_rows)):
        for row in rows:
            assert set(row) == set(contract.field_names)
            assert row["origin_label"] == "SIMULATED_FROM_M5_HISTORY"
            assert row["scenario_id"] == result.metadata["scenario_id"]
    assert all(r["scenario_seed"] is None for r in result.inventory_rows)
    assert all(r["source_warehouse_id"] == "SIMULATED_CENTRAL_WAREHOUSE"
               for r in result.central_supply_rows)


def test_metadata_canonical_identity_and_manifest_field_compatibility():
    result = generate_historical_scaled(**inputs())
    metadata = result.metadata
    scenario_fields = SCENARIO_MANIFEST.field_names[-10:]
    assert set(metadata) == set(scenario_fields)
    # Validate the scenario-specific slice without fabricating an artifact envelope.
    partial_contract = replace(INVENTORY_SCENARIO, name="scenario_metadata",
                               fields=tuple(f for f in SCENARIO_MANIFEST.field_specs
                                            if f.name in scenario_fields),
                               required_lineage=(), unique_key=("scenario_id",))
    validate_record(partial_contract, metadata)
    assert metadata["scenario_type"] == "historical_scaled"
    assert metadata["scenario_seed"] is None
    document = json.loads(metadata["scenario_parameters"])
    assert document["factors"] == {"alpha": "0.35", "gamma": "0.20", "beta": "0.20"}
    assert document["rounding"] == {"opening_stock_units": "floor",
                                    "safety_stock_units": "ceiling",
                                    "central_available_units": "floor"}
    assert document["lookback_dates"] == ["2016-03-21", "2016-03-22", "2016-03-23",
                                          "2016-03-24", "2016-03-25", "2016-03-26",
                                          "2016-03-27"]
    assert document["selection_digest"] == "opaque-validated-scope"
    canonical = json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    assert metadata["scenario_parameters"] == canonical
    digest = hashlib.sha256((canonical + "\n").encode()).hexdigest()
    assert metadata["parameter_digest"] == "sha256:" + digest
    assert metadata["scenario_id"] == "historical_scaled:" + digest
    assert document["scenario_version"] == metadata["scenario_version"]
    assert document["scenario_generator_version"] == metadata["scenario_generator_version"]


def test_input_reordering_is_equal_and_inputs_are_not_mutated():
    kwargs = inputs()
    original = kwargs.copy()
    first = generate_historical_scaled(**kwargs)
    kwargs["eligible_pairs"] = iter(reversed(kwargs["eligible_pairs"]))
    kwargs["observed_sales"] = iter(reversed(kwargs["observed_sales"]))
    assert generate_historical_scaled(**kwargs) == first
    assert generate_historical_scaled(**original) == first
    with pytest.raises(FrozenInstanceError):
        original["eligible_pairs"][0].item_id = "changed"
    with pytest.raises(FrozenInstanceError):
        original["observed_sales"][0].unit_sales = 5


@pytest.mark.parametrize("change", ["source", "scope", "date", "membership", "sales", "redistribution"])
def test_each_canonical_input_change_changes_identity(change):
    kwargs = inputs()
    original = generate_historical_scaled(**kwargs)
    sales = kwargs["observed_sales"]
    if change == "source":
        kwargs["source_snapshot_id"] = "other-snapshot"
        kwargs["observed_sales"] = tuple(replace(s, source_snapshot_id="other-snapshot") for s in sales)
    elif change == "scope":
        kwargs["selection_digest"] = "other-scope"
    elif change == "date":
        kwargs["issue_date"] += timedelta(days=1)
        kwargs["observed_sales"] = tuple(replace(s, date=s.date + timedelta(days=1)) for s in sales)
    elif change == "membership":
        kwargs["eligible_pairs"] = tuple(p for p in kwargs["eligible_pairs"] if p.item_id == "A")
        kwargs["observed_sales"] = tuple(s for s in sales if s.item_id == "A")
    elif change == "sales":
        kwargs["observed_sales"] = (replace(sales[0], unit_sales=1),) + sales[1:]
    else:
        kwargs["observed_sales"] = (replace(sales[0], unit_sales=1),
                                    replace(sales[1], unit_sales=0)) + sales[2:]
    changed = generate_historical_scaled(**kwargs)
    assert changed.metadata["scenario_id"] != original.metadata["scenario_id"]
    assert changed.metadata["parameter_digest"] != original.metadata["parameter_digest"]


@pytest.mark.parametrize("change", ["missing", "duplicate", "future", "old", "unknown", "snapshot"])
def test_rejects_incomplete_duplicate_out_of_window_or_unbound_sales(change):
    kwargs = inputs()
    sales = kwargs["observed_sales"]
    if change == "missing":
        sales = sales[1:]
    elif change == "duplicate":
        sales += (sales[0],)
    else:
        updates = {"future": {"date": ISSUE_DATE + timedelta(days=1)},
                   "old": {"date": ISSUE_DATE - timedelta(days=7)},
                   "unknown": {"store_id": "absent"},
                   "snapshot": {"source_snapshot_id": "other"}}[change]
        sales = (replace(sales[0], **updates),) + sales[1:]
    kwargs["observed_sales"] = sales
    with pytest.raises(ScenarioGenerationError):
        generate_historical_scaled(**kwargs)


@pytest.mark.parametrize("value", [-1, True, 1.0, "1", None, MAX_INT64 + 1])
def test_rejects_invalid_quantities(value):
    kwargs = inputs()
    sales = kwargs["observed_sales"]
    kwargs["observed_sales"] = (replace(sales[0], unit_sales=value),) + sales[1:]
    with pytest.raises(ScenarioGenerationError):
        generate_historical_scaled(**kwargs)


@pytest.mark.parametrize("field,value", [("source_snapshot_id", " "), ("selection_digest", ""),
                                         ("selection_digest", None), ("issue_date", "2016-03-27"),
                                         ("issue_date", datetime(2016, 3, 27)),
                                         ("issue_date", date.min), ("eligible_pairs", ()),
                                         ("observed_sales", ())])
def test_rejects_invalid_top_level_inputs(field, value):
    kwargs = inputs()
    kwargs[field] = value
    with pytest.raises(ScenarioGenerationError):
        generate_historical_scaled(**kwargs)


@pytest.mark.parametrize("target,field,value", [("pair", "item_id", ""),
                                              ("pair", "store_id", None),
                                              ("sale", "item_id", " "),
                                              ("sale", "source_snapshot_id", ""),
                                              ("sale", "date", "2016-03-21"),
                                              ("sale", "date", datetime(2016, 3, 21))])
def test_rejects_invalid_domain_fields(target, field, value):
    kwargs = inputs()
    key = "eligible_pairs" if target == "pair" else "observed_sales"
    rows = kwargs[key]
    kwargs[key] = (replace(rows[0], **{field: value}),) + rows[1:]
    with pytest.raises(ScenarioGenerationError):
        generate_historical_scaled(**kwargs)


def test_rejects_duplicate_population_membership():
    kwargs = inputs()
    kwargs["eligible_pairs"] += (kwargs["eligible_pairs"][0],)
    with pytest.raises(ScenarioGenerationError):
        generate_historical_scaled(**kwargs)


def test_large_integer_decimal_rounding_is_independent_of_ambient_precision():
    kwargs = inputs()
    kwargs["eligible_pairs"] = (EligiblePair("A", "S1"),)
    kwargs["observed_sales"] = tuple(replace(s, unit_sales=MAX_INT64 if n == 0 else 0)
                                     for n, s in enumerate(kwargs["observed_sales"][:7]))
    with localcontext() as ctx:
        ctx.prec = 2
        result = generate_historical_scaled(**kwargs)
    assert result.inventory_rows[0]["opening_stock_units"] == 3228180212899171532
    assert result.inventory_rows[0]["safety_stock_units"] == 1844674407370955162
    assert result.central_supply_rows[0]["central_available_units"] == 1844674407370955161


def test_rejects_output_overflow():
    kwargs = inputs()
    kwargs["observed_sales"] = tuple(replace(s, unit_sales=MAX_INT64)
                                     for s in kwargs["observed_sales"])
    with pytest.raises(ScenarioGenerationError):
        generate_historical_scaled(**kwargs)


def test_rejects_central_aggregation_overflow_when_store_outputs_fit():
    kwargs = inputs()
    kwargs["eligible_pairs"] = tuple(EligiblePair("A", f"S{n}") for n in range(6))
    kwargs["observed_sales"] = tuple(
        ObservedDailySale("snapshot", "A", pair.store_id, ISSUE_DATE - timedelta(days=n),
                          MAX_INT64 if n == 0 else 0)
        for pair in kwargs["eligible_pairs"] for n in range(7))
    with pytest.raises(ScenarioGenerationError):
        generate_historical_scaled(**kwargs)
