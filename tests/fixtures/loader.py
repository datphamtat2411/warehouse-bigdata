"""Load deterministic fixture resources without transforming their values."""

import json
from pathlib import Path
from typing import Any

_FIXTURE_DIR = Path(__file__).parent

_SALES_DATASETS = {"sales_daily", "sales_enriched", "store_daily_sales", "category_monthly_sales"}
_FORECASTING_DATASETS = {"forecast_features", "forecast_daily", "forecast_metrics"}
_INVENTORY_DATASETS = {
    "inventory_scenario",
    "central_supply_scenario",
    "replenishment_recommendations",
    "allocation_recommendations",
    "inventory_policy_evaluation",
}


def _load_json(relative_path: str) -> dict[str, Any]:
    with (_FIXTURE_DIR / relative_path).open(encoding="utf-8") as handle:
        return json.load(handle)


def load_context() -> dict[str, Any]:
    """Load fixture metadata and stable synthetic lineage identities."""

    return _load_json("context.json")


def load_dataset(name: str) -> Any:
    """Load one named dataset from its domain fixture file."""

    if name in _SALES_DATASETS:
        datasets = _load_json("datasets/sales.json")
    elif name in _FORECASTING_DATASETS:
        datasets = _load_json("datasets/forecasting.json")
    elif name in _INVENTORY_DATASETS:
        datasets = _load_json("datasets/inventory.json")
    else:
        raise KeyError(f"Unknown dataset fixture: {name!r}")
    return datasets[name]


def load_manifest(name: str) -> dict[str, Any]:
    """Load one named manifest fixture."""

    manifests = _load_json("manifests.json")
    if name not in manifests:
        raise KeyError(f"Unknown manifest fixture: {name!r}")
    return manifests[name]


def load_publication_fixture() -> dict[str, Any]:
    """Load the publication context and application metadata copy."""

    return _load_json("publication.json")


def load_expected(name: str) -> Any:
    """Load one independently authored golden expected-result mapping."""

    expected = _load_json("expected.json")
    if name not in expected:
        raise KeyError(f"Unknown expected fixture: {name!r}")
    return expected[name]
