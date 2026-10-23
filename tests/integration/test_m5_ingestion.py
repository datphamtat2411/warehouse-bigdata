"""Read-only original-source lineage for canonical M5 Small; never reingest."""

import json

import pytest

from tests.integration.feature_snapshot_support import owner
from warehouse_bigdata.common.small_artifacts import load_small_artifacts

pytestmark = [pytest.mark.integration, pytest.mark.m5_small]


def test_existing_small_raw_lineage_is_validated():
    selector = load_small_artifacts()
    result = json.loads(
        owner(
            """
import json, sys
from warehouse_bigdata.common.config import load_hdfs_config
from warehouse_bigdata.common.hdfs_client import HdfsCliClient
from warehouse_bigdata.ingestion.hdfs_raw import validate_hdfs_raw_snapshot
snapshot = validate_hdfs_raw_snapshot(sys.argv[1], client=HdfsCliClient(), config=load_hdfs_config())
print(json.dumps(dict(status=snapshot.status, source_version=snapshot.source_version,
    raw_path=snapshot.raw_path, source_files=sorted(m['source_file'] for m in snapshot.manifests))))
""",
            selector["source_version"],
        )
    )
    assert result["status"] == "VALIDATED"
    assert result["source_version"] == selector["source_version"]
    assert {"calendar.csv", "sell_prices.csv", "sales_train_evaluation.csv"} <= set(
        result["source_files"]
    )
