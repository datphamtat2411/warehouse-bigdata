"""Pure forward-demo J07 orchestration and manifest-ready Contract v1 inputs."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from types import MappingProxyType

from warehouse_bigdata.common.hdfs_paths import validate_pipeline_run_id
from warehouse_bigdata.common.hdfs_publication import document_digest
from warehouse_bigdata.common.small_artifacts import validate_small_artifacts
from warehouse_bigdata.contracts.datasets import (
    ALLOCATION_RECOMMENDATIONS, CENTRAL_SUPPLY_SCENARIO, INVENTORY_SCENARIO,
    REPLENISHMENT_RECOMMENDATIONS,
)
from warehouse_bigdata.contracts.manifests import DECISION_MANIFEST, SCENARIO_MANIFEST
from warehouse_bigdata.contracts.validation import validate_dataset
from warehouse_bigdata.forecasting.acceptance import EXPECTED_ISSUE_DATE

from .allocation import CentralSupply, allocate_shortage_first_proportional_v1
from .decision_inputs import DecisionInputs, freeze, plain
from .replenishment import InventoryPosition, calculate_replenishment
from .scenario_generation import generate_historical_scaled

CONTRACTS = (INVENTORY_SCENARIO, CENTRAL_SUPPLY_SCENARIO,
             REPLENISHMENT_RECOMMENDATIONS, ALLOCATION_RECOMMENDATIONS)


class DecisionBatchError(ValueError):
    """Decision rows fail forward-origin, lineage, membership or capacity checks."""


@dataclass(frozen=True, slots=True)
class DecisionBatchResult:
    inventory_rows: tuple[Mapping[str, object], ...]
    central_supply_rows: tuple[Mapping[str, object], ...]
    replenishment_rows: tuple[Mapping[str, object], ...]
    allocation_rows: tuple[Mapping[str, object], ...]
    scenario_manifest_inputs: Mapping[str, object]
    decision_manifest_inputs: Mapping[str, object]


def _validate_inputs(inputs):
    if not isinstance(inputs, DecisionInputs):
        raise DecisionBatchError("expected immutable DecisionInputs")
    validate_small_artifacts(plain(inputs.selector), through="j06")
    if (inputs.scope.get("dataset_scope") != "demo_sample"
            or inputs.scope.get("selection_digest") != inputs.selector["selection_digest"]
            or not inputs.upstream_evidence
            or not 0 < len(inputs.predictions) <= 700
            or not 0 < len(inputs.observed_sales) <= 700):
        raise DecisionBatchError("bounded canonical small inputs and evidence are required")
    eligible = {(p.item_id, p.store_id) for p in inputs.eligible_pairs}
    successful = {(p.item_id, p.store_id) for p in inputs.predictions}
    excluded = {(p.item_id, p.store_id) for p in inputs.exclusions}
    population = {(p["item_id"], p["store_id"]) for p in inputs.scope["selected_series"]}
    counts = dict(candidate_series=len(population), successful_series=len(successful),
                  excluded_series=len(excluded), row_count=len(inputs.predictions))
    if (eligible != successful or successful & excluded or successful | excluded != population
            or len(inputs.exclusions) != len(excluded)
            or any(not r.reason_codes for r in inputs.exclusions)
            or any(inputs.coverage.get(k) != v for k, v in counts.items())):
        raise DecisionBatchError("input coverage or membership differs")
    for prediction in inputs.predictions:
        if prediction.issue_date != EXPECTED_ISSUE_DATE or any(
            getattr(prediction, name) != inputs.selector[name]
            for name in ("source_snapshot_id", "forecast_run_id", "model_run_id", "feature_version")
        ):
            raise DecisionBatchError("forecast origin or selected lineage differs")


def _map_rows(contract, rows):
    """Project only declared fields and retain their declared order."""
    return tuple(MappingProxyType({name: row[name] for name in contract.field_names})
                 for row in sorted(rows, key=lambda r: tuple(r[k] for k in contract.unique_key)))


def validate_decision_datasets(*, inputs, decision_run_id, inventory_rows,
                               central_supply_rows, replenishment_rows, allocation_rows):
    """Check Contract v1 plus exact cross-dataset identity and per-SKU conservation."""
    population = {(r.item_id, r.store_id) for r in inputs.eligible_pairs}
    items = {item for item, _ in population}
    if not inventory_rows:
        raise DecisionBatchError("scenario inventory must not be empty")
    scenario_id = inventory_rows[0]["scenario_id"]
    context = {**plain(inputs.selector), "decision_run_id": decision_run_id,
               "scenario_id": scenario_id}
    datasets = (inventory_rows, central_supply_rows, replenishment_rows, allocation_rows)
    for contract, rows in zip(CONTRACTS, datasets, strict=True):
        policy = ("replenishment_v1" if contract is REPLENISHMENT_RECOMMENDATIONS else "v1")
        validate_dataset(contract, rows, lineage_context={**context,
            "policy_id": "shortage_first_proportional", "policy_version": policy})
        if any(r["issue_date"] != EXPECTED_ISSUE_DATE.isoformat() for r in rows):
            raise DecisionBatchError("decision dataset origin differs")
        if contract is not CENTRAL_SUPPLY_SCENARIO and (
            {(r["item_id"], r["store_id"]) for r in rows} != population
            or len(rows) != len(population)
        ):
            raise DecisionBatchError("decision dataset membership differs")
    if (len(central_supply_rows) != len(items)
            or {r["item_id"] for r in central_supply_rows} != items):
        raise DecisionBatchError("central supply requires exactly one row per included SKU")
    capacities = {r["item_id"]: r for r in central_supply_rows}
    requests = {(r["item_id"], r["store_id"]): r for r in replenishment_rows}
    inventory = {(r["item_id"], r["store_id"]): r for r in inventory_rows}
    for key, request in requests.items():
        if any(request[n] != inventory[key][n] for n in ("opening_stock_units", "safety_stock_units")):
            raise DecisionBatchError("recommendation stock differs from scenario")
    totals = Counter()
    for row in allocation_rows:
        supply = capacities[row["item_id"]]
        if (row["requested_units"] != requests[(row["item_id"], row["store_id"])]["request_units"]
                or row["central_available_units"] != supply["central_available_units"]
                or row["source_warehouse_id"] != supply["source_warehouse_id"]
                or row["allocated_units"] > row["requested_units"]
                or row["unmet_request_units"] != row["requested_units"] - row["allocated_units"]):
            raise DecisionBatchError("allocation does not reconcile with requests or central supply")
        totals[row["item_id"]] += row["allocated_units"]
    if any(totals[item] > capacities[item]["central_available_units"] for item in items):
        raise DecisionBatchError("per-SKU allocation exceeds central supply")


def build_decision_batch(inputs: DecisionInputs, *, decision_run_id: str) -> DecisionBatchResult:
    """Reuse Task 01–02 algorithms without changing formulas or selecting policies."""
    validate_pipeline_run_id(decision_run_id)
    _validate_inputs(inputs)
    scenario = generate_historical_scaled(issue_date=EXPECTED_ISSUE_DATE,
        source_snapshot_id=inputs.selector["source_snapshot_id"],
        selection_digest=inputs.selector["selection_digest"],
        eligible_pairs=inputs.eligible_pairs, observed_sales=inputs.observed_sales)
    positions = tuple(InventoryPosition(**{
        **{n: r[n] for n in ("item_id", "store_id", "scenario_id", "source_snapshot_id",
                             "opening_stock_units", "safety_stock_units")},
        "issue_date": EXPECTED_ISSUE_DATE,
    }) for r in scenario.inventory_rows)
    supplies = tuple(CentralSupply(**{
        **{n: r[n] for n in ("item_id", "scenario_id", "source_warehouse_id", "central_available_units")},
        "issue_date": EXPECTED_ISSUE_DATE,
    }) for r in scenario.central_supply_rows)
    requests = calculate_replenishment(inputs.predictions, positions)
    allocations = allocate_shortage_first_proportional_v1(requests, supplies)
    capacities = {r.item_id: r.central_available_units for r in supplies}
    replenishment_rows = [dict(asdict(r), decision_run_id=decision_run_id,
                               issue_date=r.issue_date.isoformat()) for r in requests]
    allocation_rows = [dict(asdict(r), decision_run_id=decision_run_id,
        issue_date=r.issue_date.isoformat(), central_available_units=capacities[r.item_id])
        for r in allocations.recommendations]
    datasets = tuple(_map_rows(c, rows) for c, rows in zip(CONTRACTS,
        (scenario.inventory_rows, scenario.central_supply_rows, replenishment_rows, allocation_rows),
        strict=True))
    validate_decision_datasets(inputs=inputs, decision_run_id=decision_run_id,
        inventory_rows=datasets[0], central_supply_rows=datasets[1],
        replenishment_rows=datasets[2], allocation_rows=datasets[3])
    descriptors = {contract.name: dict(contract_version=contract.version,
        logical_unique_key=contract.unique_key, grain=contract.grain, row_count=len(rows),
        provenance=contract.provenance)
        for contract, rows in zip(CONTRACTS, datasets, strict=True)}
    shared = dict(
        dataset_scope="demo_sample", selection_digest=inputs.selector["selection_digest"],
        scope=plain(inputs.scope), upstream_evidence=plain(inputs.upstream_evidence),
        upstream_identities=plain(inputs.selector), coverage=plain(inputs.coverage),
        exclusions=[asdict(r) for r in sorted(inputs.exclusions, key=lambda r: (r.item_id, r.store_id))],
    )
    scenario_metadata = {**shared, **scenario.metadata,
        "contract_name": SCENARIO_MANIFEST.name, "contract_version": SCENARIO_MANIFEST.version,
        "manifest_schema_version": SCENARIO_MANIFEST.version,
        "datasets": {c.name: descriptors[c.name] for c in CONTRACTS[:2]},
        "configuration_digest": scenario.metadata["parameter_digest"]}
    decision_config = dict(issue_date=EXPECTED_ISSUE_DATE.isoformat(), horizon_days=7,
        scenario_id=scenario.metadata["scenario_id"], allocation_policy="shortage_first_proportional_v1",
        replenishment_policy="replenishment_v1", upstream_identities=plain(inputs.selector))
    decision_metadata = {**shared,
        **{k: inputs.selector[k] for k in ("source_snapshot_id", "forecast_run_id", "model_run_id", "feature_version")},
        "contract_name": DECISION_MANIFEST.name, "contract_version": DECISION_MANIFEST.version,
        "manifest_schema_version": DECISION_MANIFEST.version,
        "decision_run_id": decision_run_id, "scenario_id": scenario.metadata["scenario_id"],
        "issue_date": EXPECTED_ISSUE_DATE.isoformat(), "policy_id": "shortage_first_proportional",
        "policy_version": "v1", "replenishment_policy_version": "replenishment_v1",
        "datasets": {c.name: descriptors[c.name] for c in CONTRACTS[2:]},
        "configuration_digest": document_digest(decision_config),
    }
    return DecisionBatchResult(*datasets, freeze(scenario_metadata), freeze(decision_metadata))
