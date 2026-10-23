"""Explicitly opt-in read-only publication acceptance and fresh-code J04 lifecycle.

Published-only (no J04 submission or HDFS writes)::

    uv run pytest -m integration tests/integration/test_feature_snapshot.py \
        --run-m05-published -k published

Fresh version, current packaged revision, potentially expensive::

    uv run pytest -m integration tests/integration/test_feature_snapshot.py \
        --run-m05-lifecycle -k lifecycle

Both workflows consume WAREHOUSE_SMALL_ARTIFACTS. Legacy M04 smoke IDs and
scattered M05 environment selectors are not accepted.
"""

import json
from uuid import uuid4

import pytest

from tests.integration.feature_snapshot_support import (
    command,
    independent_probe,
    owner,
    submit,
)
from warehouse_bigdata.common.small_artifacts import load_small_artifacts

pytestmark = pytest.mark.integration


def source_selection():
    selector = load_small_artifacts()
    return tuple(
        selector[key] for key in ("source_version", "source_snapshot_id", "j03_pipeline_run_id")
    )


@pytest.mark.m05_published
def test_published_feature_snapshot(tmp_path):
    """Verify the old publication independently, without attempting current-code reuse."""
    source, snapshot, upstream = source_selection()
    selector = load_small_artifacts()
    version = selector["feature_version"]
    producer = selector["j04_pipeline_run_id"]
    result = independent_probe(source, snapshot, upstream, version, producer)
    assert result["producer"] == producer, "Published selector must identify the original producer"
    evidence = {
        "publication": result,
        "live_reuse_exercised": False,
        "same_run_recovery_exercised": False,
    }
    path = tmp_path / "j04-published-evidence.json"
    path.write_text(json.dumps(evidence, sort_keys=True, indent=2))
    print(f"J04 published-only evidence: {path}")


@pytest.mark.m05_lifecycle
def test_live_feature_snapshot_lifecycle(tmp_path):
    """Fresh publication → full readback → same-code live reuse → owner-only recovery."""
    source, snapshot, upstream = source_selection()
    token = uuid4().hex
    version, run, reuse = "m05-fix02-" + token, "j04-" + token, "j04-reuse-" + token
    path = tmp_path / "j04-lifecycle-evidence.json"
    evidence = {
        "source_version": source,
        "source_snapshot_id": snapshot,
        "upstream_pipeline_run_id": upstream,
        "feature_version": version,
        "producing_run": run,
        "reuse_run": reuse,
        "phase": "prepared",
        "live_reuse_exercised": False,
        "same_run_recovery_exercised": False,
    }

    def checkpoint(phase):
        evidence["phase"] = phase
        path.write_text(json.dumps(evidence, sort_keys=True, indent=2))

    checkpoint("preflight")  # Record recoverable identities before creating anything remotely.
    print(f"J04 lifecycle evidence / recovery identities: {path}", flush=True)
    try:
        command(["bash", "scripts/spark-cluster.sh", "check"], operation="metadata")
        owner(
            """
import json, sys
from warehouse_bigdata.common.hdfs_client import HdfsCliClient
from warehouse_bigdata.common.config import load_hdfs_config
from warehouse_bigdata.common.hdfs_namespace import validate_publication_namespace
from warehouse_bigdata.common.hdfs_permissions import prepare_spark_run_permissions
from warehouse_bigdata.common.hdfs_paths import render_staging_path, render_run_manifest_path
c, config = HdfsCliClient(), load_hdfs_config()
validate_publication_namespace(c, config)
source, snapshot, upstream_run = sys.argv[1:4]
upstream = json.loads(c.read_remote(render_run_manifest_path(config.base_path, upstream_run)))
expected = {'status': 'VALIDATED', 'source_version': source,
    'source_snapshot_id': snapshot, 'pipeline_run_id': upstream_run, 'row_count': 194100,
    'producer': 'warehouse-bigdata:enrichment-analytics-snapshot-v1'}
assert all(upstream.get(k) == v for k, v in expected.items()), ('M04 fixture', upstream)
from warehouse_bigdata.common.scope_evidence import read_scope, validate_scope_binding
from warehouse_bigdata.common.hdfs_paths import render_curated_sales_enriched_path
scope = read_scope(c, render_curated_sales_enriched_path(config.base_path, snapshot))
assert scope['dataset_scope'] == 'demo_sample' and scope['selected_series_count'] == 100
validate_scope_binding(upstream, scope)
for run in sys.argv[4:]:
    base = render_staging_path(config.base_path, run)
    c.mkdir_exclusive(base)
    c.mkdir_exclusive(base + '/events')
    prepare_spark_run_permissions(c, config, pipeline_run_id=run,
        event_log_dir=config.uri + base + '/events', verify_effective_access=False)
""",
            source,
            snapshot,
            upstream,
            run,
            reuse,
        )
        checkpoint("initial_publication")
        evidence["submission"] = submit(source, snapshot, upstream, version, run)
        before = independent_probe(source, snapshot, upstream, version, run)
        assert before["producer"] == run
        evidence["initial"] = before
        checkpoint("live_reuse")
        evidence["reuse_submission"] = submit(
            source, snapshot, upstream, version, reuse, reuse=True
        )
        after = independent_probe(source, snapshot, upstream, version, reuse, full=False)
        assert after["execution_run"] == reuse
        assert after["execution_runtime"]["application_id"] != before["runtime"]["application_id"]
        for key in (
            "file_integrity",
            "sidecar_sha256",
            "producer_completion_sha256",
            "producer",
            "code_revision",
            "runtime",
        ):
            assert after[key] == before[key], f"Live reuse changed {key}"
        evidence["reuse"] = after
        evidence["live_reuse_exercised"] = True
        checkpoint("frozen_same_run_recovery")
        evidence["recovery_submission"] = submit(
            source,
            snapshot,
            upstream,
            version,
            run,
            recovery=True,
        )
        recovery = independent_probe(source, snapshot, upstream, version, run, full=False)
        for key in (
            "file_integrity",
            "sidecar_sha256",
            "completion_sha256",
            "producer_completion_sha256",
            "producer",
            "binding_digest",
            "quality_digest",
            "request_digest",
            "frozen_metadata_sha256",
            "code_revision",
            "runtime",
        ):
            assert recovery[key] == before[key], f"Frozen same-run recovery changed {key}"
        evidence["recovery"] = recovery
        evidence["same_run_recovery_exercised"] = True
        checkpoint("complete")
    except BaseException as error:
        evidence["failure"] = f"{type(error).__name__}: {str(error)[-4096:]}"
        checkpoint(evidence["phase"] + "_interrupted_or_failed")
        raise
