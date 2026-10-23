"""Canonical metadata-only M5 population selection and versioned scope evidence.

Selection never inspects daily labels. JSON uses sorted keys, ASCII escaping, compact
separators, no nonfinite numbers, and one trailing LF. The digest excludes itself.
"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict

from warehouse_bigdata.common.hdfs_paths import validate_source_snapshot_id, validate_source_version

DEMO_SAMPLE = "demo_sample"
FULL_M5 = "full_m5"
SCOPE_VERSION = "m5-scope-v1"
SELECTION_POLICY = "metadata-department-round-robin-sha256-v1"
SELECTION_SEED = 42
DEMO_SKUS = 10
DEMO_SERIES = 100
HISTORICAL_DAYS = 1941
DEMO_ROWS = 194100
STORES = ("CA_1", "CA_2", "CA_3", "CA_4", "TX_1", "TX_2", "TX_3", "WI_1", "WI_2", "WI_3")
DEPARTMENTS = (
    "FOODS_1",
    "FOODS_2",
    "FOODS_3",
    "HOBBIES_1",
    "HOBBIES_2",
    "HOUSEHOLD_1",
    "HOUSEHOLD_2",
)
IDENTITY_FIELDS = ("id", "item_id", "dept_id", "cat_id", "store_id", "state_id")


class ScopeError(ValueError):
    """A selected population, its lineage, or execution authorization is invalid."""


def canonical_bytes(document):
    return (
        json.dumps(
            document, sort_keys=True, ensure_ascii=True, separators=(",", ":"), allow_nan=False
        )
        + "\n"
    ).encode("utf-8")


def scope_digest(document):
    return (
        "sha256:"
        + hashlib.sha256(
            canonical_bytes({k: v for k, v in document.items() if k != "selection_digest"})
        ).hexdigest()
    )


def require_scope_execution(dataset_scope, *, confirm_large_workload=False):
    if dataset_scope not in {DEMO_SAMPLE, FULL_M5}:
        raise ScopeError("dataset_scope must explicitly identify demo_sample or full_m5")
    if dataset_scope == FULL_M5 and confirm_large_workload is not True:
        raise ScopeError("full_m5 requires explicit --confirm-large-workload authorization")


def _identities(rows):
    records, ids, pairs, items = [], set(), set(), defaultdict(list)
    for row in rows:
        record = {name: row[name] for name in IDENTITY_FIELDS}
        if any(type(value) is not str or not value.strip() for value in record.values()):
            raise ScopeError("null or blank source identity")
        item, store = record["item_id"], record["store_id"]
        if record["id"] in ids or (item, store) in pairs:
            raise ScopeError("duplicate source id or item-store identity")
        if (
            store not in STORES
            or record["state_id"] != store[:2]
            or record["dept_id"] not in DEPARTMENTS
            or record["cat_id"] != record["dept_id"].rsplit("_", 1)[0]
        ):
            raise ScopeError("inconsistent store/state or department/category identity")
        ids.add(record["id"])
        pairs.add((item, store))
        records.append(record)
        items[item].append(record)
    if not records:
        raise ScopeError("empty source identity universe")
    for item, values in items.items():
        if (
            {v["store_id"] for v in values} != set(STORES)
            or len(values) != len(STORES)
            or len({(v["dept_id"], v["cat_id"]) for v in values}) != 1
        ):
            raise ScopeError("incomplete ten-store or inconsistent item metadata: " + item)
    return sorted(records, key=lambda r: (r["item_id"], r["store_id"])), items


def select_scope(
    rows, *, source_version, source_snapshot_id, dataset_scope=DEMO_SAMPLE, seed=SELECTION_SEED
):
    """Cover departments first, then round-robin additional seeded item rankings."""
    validate_source_version(source_version)
    validate_source_snapshot_id(source_snapshot_id)
    if (
        dataset_scope not in {DEMO_SAMPLE, FULL_M5}
        or seed != SELECTION_SEED
        or type(seed) is not int
    ):
        raise ScopeError("unsupported scope or selection seed; canonical seed is 42")
    records, items = _identities(rows)
    by_department = defaultdict(list)
    for item, values in items.items():
        by_department[values[0]["dept_id"]].append(item)
    ranked = {
        dept: sorted(
            values,
            key=lambda item: (hashlib.sha256(canonical_bytes([seed, item])).hexdigest(), item),
        )
        for dept, values in by_department.items()
    }
    order = [
        values[rank]
        for rank in range(max(map(len, ranked.values())))
        for _, values in sorted(ranked.items())
        if rank < len(values)
    ]
    if dataset_scope == DEMO_SAMPLE and len(order) < DEMO_SKUS:
        raise ScopeError("source supports fewer than ten complete SKUs")
    chosen = sorted(order[:DEMO_SKUS] if dataset_scope == DEMO_SAMPLE else order)
    selected = [r for r in records if r["item_id"] in set(chosen)]
    document = dict(
        schema_version=SCOPE_VERSION,
        dataset_scope=dataset_scope,
        selection_policy=SELECTION_POLICY,
        selection_seed=seed,
        source_version=source_version,
        source_snapshot_id=source_snapshot_id,
        selected_item_ids=chosen,
        selected_store_ids=list(STORES),
        selected_department_ids=sorted({r["dept_id"] for r in selected}),
        available_department_ids=sorted(ranked),
        selected_sku_count=len(chosen),
        selected_series_count=len(selected),
        original_series_count=len(records),
        excluded_series_count=len(records) - len(selected),
        historical_day_count=HISTORICAL_DAYS,
        expected_observation_count=len(selected) * HISTORICAL_DAYS,
        selected_series=selected,
    )
    document["selection_digest"] = scope_digest(document)
    return document


def validate_scope(document, *, source_version=None, source_snapshot_id=None, expected_digest=None):
    """Strict v1 sidecar validation, independent of unchanged Contract v1 manifests."""
    if not isinstance(document, dict):
        raise ScopeError("scope evidence must be an object")
    required = {
        "schema_version",
        "dataset_scope",
        "selection_policy",
        "selection_seed",
        "source_version",
        "source_snapshot_id",
        "selected_item_ids",
        "selected_store_ids",
        "selected_department_ids",
        "available_department_ids",
        "selected_sku_count",
        "selected_series_count",
        "original_series_count",
        "excluded_series_count",
        "historical_day_count",
        "expected_observation_count",
        "selected_series",
        "selection_digest",
    }
    if set(document) != required:
        raise ScopeError("scope evidence fields differ from m5-scope-v1")
    try:
        validate_source_version(document["source_version"])
        validate_source_snapshot_id(document["source_snapshot_id"])
    except ValueError as error:
        raise ScopeError("invalid scope lineage") from error
    if (source_version is not None and document["source_version"] != source_version) or (
        source_snapshot_id is not None and document["source_snapshot_id"] != source_snapshot_id
    ):
        raise ScopeError("scope source lineage mismatch")
    if (
        document["schema_version"] != SCOPE_VERSION
        or document["dataset_scope"] not in {DEMO_SAMPLE, FULL_M5}
        or document["selection_policy"] != SELECTION_POLICY
        or type(document["selection_seed"]) is not int
        or document["selection_seed"] != SELECTION_SEED
        or document["selection_digest"] != scope_digest(document)
        or (expected_digest is not None and document["selection_digest"] != expected_digest)
    ):
        raise ScopeError("scope policy or selection digest mismatch")
    for name in (
        "selected_sku_count",
        "selected_series_count",
        "original_series_count",
        "excluded_series_count",
        "historical_day_count",
        "expected_observation_count",
    ):
        if type(document[name]) is not int or document[name] < 0:
            raise ScopeError("invalid scope count: " + name)
    records, items = _identities(document["selected_series"])
    departments = sorted({r["dept_id"] for r in records})
    available = document["available_department_ids"]
    if (
        records != document["selected_series"]
        or sorted(items) != document["selected_item_ids"]
        or document["selected_store_ids"] != list(STORES)
        or departments != document["selected_department_ids"]
        or not isinstance(available, list)
        or available != sorted(set(available))
        or not set(available) <= set(DEPARTMENTS)
        or set(departments) != set(available)
        or len(items) != document["selected_sku_count"]
        or len(records) != document["selected_series_count"]
        or document["historical_day_count"] != HISTORICAL_DAYS
        or document["original_series_count"] != len(records) + document["excluded_series_count"]
        or document["expected_observation_count"] != len(records) * HISTORICAL_DAYS
        or (
            document["dataset_scope"] == DEMO_SAMPLE
            and (len(items) != DEMO_SKUS or len(records) != DEMO_SERIES)
        )
    ):
        raise ScopeError("scope population or historical coverage mismatch")
    if document["dataset_scope"] == FULL_M5 and (
        len(records) != 30490 or document["excluded_series_count"] != 0
    ):
        raise ScopeError("full_m5 must retain the original 30,490 series")
    return document
