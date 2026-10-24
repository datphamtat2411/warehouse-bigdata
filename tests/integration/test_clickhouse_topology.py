"""Explicit, infrastructure-only SQL acceptance against running ClickHouse nodes."""

import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[2]


def test_clickhouse_topology():
    subprocess.run(
        ["bash", str(ROOT / "scripts/clickhouse-cluster.sh"), "check"],
        check=True, timeout=150,
    )
