"""Offline serving-schema contracts; no running database or sales inputs."""

import re
import subprocess
from pathlib import Path

from warehouse_bigdata.contracts.datasets import SERVING_DATASET_MAPPINGS

ROOT = Path(__file__).resolve().parents[2]
SQL = ROOT / "sql/clickhouse"


def test_migrations_preserve_contract_v1():
    local = (SQL / "001_warehouse_local.sql").read_text()
    distributed = (SQL / "002_warehouse_distributed.sql").read_text()
    tables = dict(re.findall(
        r"CREATE TABLE IF NOT EXISTS warehouse\.(\w+)_local\s*\((.*?)\)\s*ENGINE",
        local, re.S,
    ))
    assert set(tables) == {m.serving_dataset for m in SERVING_DATASET_MAPPINGS}
    assert len(re.findall(r"CREATE TABLE IF NOT EXISTS", distributed)) == 9
    for mapping in SERVING_DATASET_MAPPINGS:
        fields = dict(line.strip().rstrip(",").split(" ", 1)
                      for line in tables[mapping.serving_dataset].strip().splitlines())
        expected = {
            f.serving_field: f"Nullable({f.serving_type})" if f.nullable else f.serving_type
            for f in mapping.field_mappings if f.serving_field
        }
        if mapping.serving_dataset == "sales_daily":
            expected["source_snapshot_id"] = "String"
        if mapping.serving_dataset == "forecast_daily":
            expected["forecast_status"] = "LowCardinality(String)"
        assert fields == expected
        sharding = {
            "store_daily_sales": "cityHash64(store_id, sale_date)",
            "central_supply_scenario": "cityHash64(item_id)",
            "forecast_metrics": "cityHash64(evaluation_run_id, holdout_id, method_id)",
        }.get(mapping.serving_dataset, "cityHash64(item_id, store_id)")
        assert (f"Distributed('warehouse_cluster', 'warehouse', "
                f"'{mapping.serving_dataset}_local', {sharding})") in distributed
    assert local.count("ENGINE = MergeTree()") == 9
    for text in (local, distributed):
        assert not re.search(r"\b(DROP|TRUNCATE|ALTER|ON\s+CLUSTER|ReplicatedMergeTree|Keeper)\b", text, re.I)


def test_runner_rejects_unknown_actions():
    result = subprocess.run(
        ["bash", str(ROOT / "scripts/clickhouse-schema.sh"), "reset"],
        capture_output=True, text=True,
    )
    assert result.returncode == 2
