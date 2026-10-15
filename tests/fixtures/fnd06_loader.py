"""Load the deterministic FND-06 JSON fixtures without transforming them."""

import json
from pathlib import Path
from typing import Any

_FIXTURE_DIR = Path(__file__).parent


def load_fixture() -> dict[str, Any]:
    """Load a fresh copy of the valid synthetic records and manifests."""

    with (_FIXTURE_DIR / "fnd06_records.json").open(encoding="utf-8") as handle:
        return json.load(handle)


def load_expected() -> dict[str, Any]:
    """Load the independently written FND-06 golden values."""

    with (_FIXTURE_DIR / "fnd06_expected.json").open(encoding="utf-8") as handle:
        return json.load(handle)
