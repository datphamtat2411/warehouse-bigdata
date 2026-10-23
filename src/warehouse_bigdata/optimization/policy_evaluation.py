"""Frozen d_1913 policy comparison, followed by observed-sales-proxy scoring.

Decisions have no label argument. Both policies share the exact immutable scenario,
requests and supply. Outcomes describe simulated M5 Small inventory only.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import date, timedelta

from warehouse_bigdata.common.hdfs_paths import validate_evaluation_run_id, validate_pipeline_run_id
from warehouse_bigdata.common.hdfs_publication import document_digest
from warehouse_bigdata.common.small_artifacts import validate_small_artifacts
from warehouse_bigdata.contracts.datasets import INVENTORY_POLICY_EVALUATION
from warehouse_bigdata.contracts.validation import validate_dataset

from .allocation import (
    AllocationBatchResult, CentralSupply, allocate_equal_share_v1,
    allocate_shortage_first_proportional_v1,
)
from .decision_inputs import (
    HISTORICAL_ISSUE_DATE, DecisionInputs, ExcludedSeries, _day, _rows, freeze, plain,
)
from .replenishment import InventoryPosition, ReplenishmentRecommendation, calculate_replenishment
from .scenario_generation import generate_historical_scaled

PROPOSED = "shortage_first_proportional"
BASELINE = "equal_share"


class PolicyEvaluationError(ValueError):
    """Historical identity, pairing or conservation is invalid."""


@dataclass(frozen=True, slots=True)
class FrozenPolicyDecisions:
    inputs: DecisionInputs
    inventory_rows: tuple[Mapping[str, object], ...]
    supplies: tuple[CentralSupply, ...]
    requests: tuple[ReplenishmentRecommendation, ...]
    allocations: tuple[AllocationBatchResult, ...]
    decision_run_ids: tuple[str, str]
    digest: str


@dataclass(frozen=True, slots=True)
class PolicyEvaluationResult:
    rows: tuple[Mapping[str, object], ...]
    daily_rows: tuple[Mapping[str, object], ...]
    exclusions: tuple[ExcludedSeries, ...]
    coverage: Mapping[str, object]
    metrics: Mapping[str, object]
    metadata: Mapping[str, object]


def _json_value(value):
    if type(value) is date:
        return value.isoformat()
    if isinstance(value, Mapping):
        return {k: _json_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(v) for v in value]
    return value


def _freeze_digest(batch):
    return document_digest(_json_value(dict(
        upstream_identities=plain(batch.inputs.selector), scope=plain(batch.inputs.scope),
        upstream_evidence=plain(batch.inputs.upstream_evidence),
        forecasts=[asdict(r) for r in batch.inputs.predictions],
        inventory=plain(batch.inventory_rows), supply=[asdict(r) for r in batch.supplies],
        requests=[asdict(r) for r in batch.requests],
        allocations=[asdict(r) for r in batch.allocations],
        decision_run_ids=batch.decision_run_ids, policies=[PROPOSED, BASELINE],
        policy_version="v1", issue_date=HISTORICAL_ISSUE_DATE,
        coverage=plain(batch.inputs.coverage), exclusions=[asdict(r) for r in batch.inputs.exclusions],
    )))


def validate_decision_freeze(batch):
    if not isinstance(batch, FrozenPolicyDecisions) or batch.digest != _freeze_digest(batch):
        raise PolicyEvaluationError("decision freeze digest conflicts")


def freeze_policy_decisions(inputs: DecisionInputs, *, proposed_decision_run_id: str,
                            baseline_decision_run_id: str) -> FrozenPolicyDecisions:
    """Construct each shared input once, before any holdout label can be supplied."""
    from warehouse_bigdata.forecasting.evaluation import EvaluationContext

    for run_id in (proposed_decision_run_id, baseline_decision_run_id):
        validate_pipeline_run_id(run_id)
    if proposed_decision_run_id == baseline_decision_run_id:
        raise PolicyEvaluationError("policies require distinct decision_run_id values")
    if not isinstance(inputs, DecisionInputs):
        raise PolicyEvaluationError("expected immutable historical DecisionInputs")
    selector = plain(inputs.selector)
    validate_small_artifacts(selector, through="test")
    context = EvaluationContext(selector["test_evaluation_run_id"], selector["model_run_id"],
        selector["source_snapshot_id"], selector["feature_version"], "test")
    if (selector["forecast_run_id"] != context.forecast_id(1913)
            or inputs.scope.get("dataset_scope") != "demo_sample"
            or inputs.scope.get("selection_digest") != selector["selection_digest"]
            or not inputs.upstream_evidence):
        raise PolicyEvaluationError("historical origin lineage or scope conflicts")
    for prediction in inputs.predictions:
        if (prediction.issue_date != HISTORICAL_ISSUE_DATE or any(
                getattr(prediction, n) != selector[n] for n in (
                    "forecast_run_id", "model_run_id", "source_snapshot_id", "feature_version"))):
            raise PolicyEvaluationError("historical forecast lineage conflicts")
    population = {(p["item_id"], p["store_id"]) for p in inputs.scope["selected_series"]}
    eligible = {(p.item_id, p.store_id) for p in inputs.eligible_pairs}
    excluded = {(p.item_id, p.store_id) for p in inputs.exclusions}
    if (eligible & excluded or eligible | excluded != population
            or len(inputs.eligible_pairs) != len(eligible)
            or {(r.item_id, r.store_id) for r in inputs.predictions} != eligible
            or inputs.coverage.get("decision_eligible_series") != len(eligible)):
        raise PolicyEvaluationError("historical population or coverage conflicts")
    if eligible:
        scenario = generate_historical_scaled(issue_date=HISTORICAL_ISSUE_DATE,
            source_snapshot_id=selector["source_snapshot_id"],
            selection_digest=selector["selection_digest"],
            eligible_pairs=inputs.eligible_pairs, observed_sales=inputs.observed_sales)
        inventory = tuple(freeze(r) for r in scenario.inventory_rows)
        positions = tuple(InventoryPosition(**{n: r[n] for n in (
            "item_id", "store_id", "scenario_id", "source_snapshot_id",
            "opening_stock_units", "safety_stock_units")}, issue_date=HISTORICAL_ISSUE_DATE)
            for r in inventory)
        supplies = tuple(CentralSupply(**{n: r[n] for n in (
            "item_id", "scenario_id", "source_warehouse_id", "central_available_units")},
            issue_date=HISTORICAL_ISSUE_DATE) for r in scenario.central_supply_rows)
        requests = calculate_replenishment(inputs.predictions, positions)
        allocations = (allocate_shortage_first_proportional_v1(requests, supplies),
                       allocate_equal_share_v1(requests, supplies))
    else:
        if inputs.predictions or inputs.observed_sales:
            raise PolicyEvaluationError("empty eligible population has decision inputs")
        inventory, supplies, requests, allocations = (), (), (), ()
    batch = FrozenPolicyDecisions(inputs, inventory, supplies, requests, allocations,
        (proposed_decision_run_id, baseline_decision_run_id), "")
    return FrozenPolicyDecisions(inputs, inventory, supplies, requests, allocations,
                                batch.decision_run_ids, _freeze_digest(batch))


def evaluate_frozen_policies(batch: FrozenPolicyDecisions, label_rows, *, evaluation_run_id: str
                             ) -> PolicyEvaluationResult:
    """Score both policies on identical complete seven-day observed-sales cases."""
    validate_evaluation_run_id(evaluation_run_id)
    validate_decision_freeze(batch)
    selector = batch.inputs.selector
    population = {(r.item_id, r.store_id) for r in batch.requests}
    labels, seen, invalid = {}, set(), set()
    days = tuple(HISTORICAL_ISSUE_DATE + timedelta(days=h) for h in range(1, 8))
    for row in _rows(label_rows, 700, "labels"):
        key = (row.get("item_id"), row.get("store_id"))
        day, units = _day(row.get("date")), row.get("unit_sales")
        if (key not in population or day not in days or (*key, day) in seen
                or row.get("source_snapshot_id") != selector["source_snapshot_id"]):
            raise PolicyEvaluationError("duplicate, outside-window or conflicting label lineage")
        seen.add((*key, day))
        if type(units) is not int or not 0 <= units <= 2**63 - 1:
            invalid.add(key)
        else:
            labels.setdefault(key, {})[day] = units
    exclusions = list(batch.inputs.exclusions)
    paired = set()
    for key in sorted(population):
        if key in invalid or len(labels.get(key, {})) != 7:
            exclusions.append(ExcludedSeries(*key,
                ("INVALID_LABELS" if key in invalid else "INCOMPLETE_LABELS",), "LABELS"))
        else:
            paired.add(key)
    stocks = {(r["item_id"], r["store_id"]): r for r in batch.inventory_rows}
    rows, daily, metrics, paired_keys = [], [], {}, []
    for index, policy in enumerate((PROPOSED, BASELINE)):
        allocations = batch.allocations[index].recommendations if batch.allocations else ()
        allocation_map = {(r.item_id, r.store_id): r for r in allocations}
        capacity = sum(r.central_available_units for r in batch.supplies)
        allocated = sum(r.allocated_units for r in allocations)
        for supply in batch.supplies:
            used = sum(r.allocated_units for r in allocations if r.item_id == supply.item_id)
            remainder = dict(batch.allocations[index].central_remainders)[supply.item_id]
            if used + remainder != supply.central_available_units:
                raise PolicyEvaluationError("per-SKU inventory conservation failed")
        policy_rows = []
        for key in sorted(paired):
            stock, allocation = stocks[key], allocation_map[key]
            initial = remaining = stock["opening_stock_units"] + allocation.allocated_units
            fulfilled = unmet = stockout_days = 0
            for day in days:
                demand = labels[key][day]
                opening = remaining
                served = min(opening, demand)
                lost = demand - served
                remaining = opening - served
                if served + lost != demand or remaining + served != opening or min(remaining, lost) < 0:
                    raise PolicyEvaluationError("daily demand or inventory conservation failed")
                fulfilled += served
                unmet += lost
                stockout_days += int(lost > 0)
                daily.append(dict(policy_id=policy, item_id=key[0], store_id=key[1],
                    date=day.isoformat(), opening_stock_units=opening,
                    observed_sales_proxy_units=demand, simulated_fulfilled_units=served,
                    simulated_unmet_units=lost, remaining_stock_units=remaining))
            actual = sum(labels[key].values())
            if fulfilled + unmet != actual or fulfilled + remaining != initial:
                raise PolicyEvaluationError("seven-day demand or inventory conservation failed")
            row = dict(evaluation_run_id=evaluation_run_id,
                source_snapshot_id=selector["source_snapshot_id"], forecast_run_id=selector["forecast_run_id"],
                scenario_id=stock["scenario_id"], decision_run_id=batch.decision_run_ids[index],
                policy_id=policy, policy_version="v1", issue_date=HISTORICAL_ISSUE_DATE.isoformat(),
                item_id=key[0], store_id=key[1], actual_sales_proxy_7d_units=actual,
                simulated_fulfilled_units=fulfilled, simulated_unmet_units=unmet,
                ending_stock_units=remaining,
                excess_over_safety_units=max(0, remaining - stock["safety_stock_units"]),
                simulated_stockout_days=stockout_days, run_type="test",
                observed_label_window_start=days[0].isoformat(), observed_label_window_end=days[-1].isoformat())
            policy_rows.append(row)
        paired_keys.append({(r["item_id"], r["store_id"], r["issue_date"]) for r in policy_rows})
        rows.extend(policy_rows)
        totals = {n: sum(r[n] for r in policy_rows) for n in (
            "actual_sales_proxy_7d_units", "simulated_fulfilled_units", "simulated_unmet_units",
            "ending_stock_units", "excess_over_safety_units", "simulated_stockout_days")}
        demand = totals["actual_sales_proxy_7d_units"]
        metrics[policy] = dict(**totals,
            stockout_horizon_count=sum(r["simulated_unmet_units"] > 0 for r in policy_rows),
            unmet_request_units=sum(allocation_map[k].unmet_request_units for k in paired),
            fulfillment_rate=totals["simulated_fulfilled_units"] / demand if demand else None,
            central_allocated_units=allocated, central_available_units=capacity,
            central_utilization=allocated / capacity if capacity else None,
            denominators=dict(paired_series=len(paired), paired_series_days=len(paired) * 7,
                observed_sales_proxy_units=demand, central_available_units=capacity,
                central_decision_series=len(population)))
    if paired_keys[0] != paired_keys[1]:
        raise PolicyEvaluationError("policy evaluation keys are not paired")
    validate_dataset(INVENTORY_POLICY_EVALUATION, rows)
    metrics["proposed_minus_baseline"] = {
        name: (metrics[PROPOSED][name] - metrics[BASELINE][name]
               if metrics[PROPOSED][name] is not None and metrics[BASELINE][name] is not None else None)
        for name in metrics[PROPOSED] if name != "denominators"}
    counts = Counter((r.stage, code) for r in exclusions for code in r.reason_codes)
    coverage = {**plain(batch.inputs.coverage), "complete_label_series": len(paired),
        "paired_series": len(paired), "evaluation_row_count": len(rows),
        "daily_row_count": len(daily), "excluded_series": len(exclusions),
        "exclusions_by_stage_reason": [dict(stage=s, reason_code=c, series_count=n)
                                      for (s, c), n in sorted(counts.items())]}
    metadata = dict(**plain(selector), evaluation_run_id=evaluation_run_id, run_type="test",
        issue_date=HISTORICAL_ISSUE_DATE.isoformat(), decision_freeze_digest=batch.digest,
        upstream_evidence=plain(batch.inputs.upstream_evidence),
        decision_run_ids=dict(zip((PROPOSED, BASELINE), batch.decision_run_ids, strict=True)),
        outcome_provenance=INVENTORY_POLICY_EVALUATION.provenance,
        label_provenance="observed_m5_unit_sales_demand_proxy",
        metric_population="demo_sample paired series; central utilization uses all frozen decisions",
        difference_convention="proposed_minus_baseline; positive fulfillment/rate is better; "
            "negative unmet/stockout/excess is better; no winner selected",
        exclusions=[asdict(r) for r in sorted(exclusions, key=lambda r: (r.item_id, r.store_id, r.stage))])
    return PolicyEvaluationResult(tuple(freeze(r) for r in rows), tuple(freeze(r) for r in daily),
        tuple(sorted(exclusions, key=lambda r: (r.item_id, r.store_id, r.stage))),
        freeze(coverage), freeze(metrics), freeze(metadata))
