"""Opt-in bounded J04 Standalone publication, independent readback and immutable reuse."""

import json
import os
from uuid import uuid4

import pytest

from tests.integration.feature_snapshot_support import independent_probe, owner, submit

pytestmark = pytest.mark.integration


def test_live_feature_snapshot_publication_and_reuse(tmp_path):
    keys = (
        "WAREHOUSE_M05_SOURCE_VERSION",
        "WAREHOUSE_M05_SOURCE_SNAPSHOT_ID",
        "WAREHOUSE_M05_UPSTREAM_PIPELINE_RUN_ID",
    )
    missing = [key for key in keys if not os.environ.get(key)]
    if missing:
        pytest.fail("Select the validated M04 30-day smoke fixture with: " + ", ".join(missing))
    source, snapshot, upstream = (os.environ[key] for key in keys)
    token = uuid4().hex
    version, run, reuse = "m05-t03-" + token, "j04-" + token, "j04-reuse-" + token
    prepare = """
import json
import sys
from warehouse_bigdata.common.hdfs_client import HdfsCliClient
from warehouse_bigdata.common.config import load_hdfs_config
from warehouse_bigdata.common.hdfs_namespace import validate_publication_namespace
from warehouse_bigdata.common.hdfs_permissions import prepare_spark_run_permissions
from warehouse_bigdata.common.hdfs_paths import render_staging_path
c = HdfsCliClient()
config = load_hdfs_config()
validate_publication_namespace(c, config)
source, snapshot, upstream_run = sys.argv[1:4]
upstream = json.loads(c.read_remote('/data/warehouse/manifests/runs/' + upstream_run + '.json'))
expected = {'status': 'VALIDATED', 'source_version': source,
    'source_snapshot_id': snapshot, 'pipeline_run_id': upstream_run, 'row_count': 914700,
    'producer': 'warehouse-bigdata:enrichment-analytics-snapshot-v1'}
assert all(upstream.get(k) == v for k, v in expected.items()), ('M04 smoke J03 completion', upstream)
for run in sys.argv[4:]:
    path = render_staging_path(config.base_path, run)
    c.mkdir_exclusive(path)
    c.mkdir_exclusive(path + '/events')
    prepare_spark_run_permissions(c, config, pipeline_run_id=run,
        event_log_dir=config.uri + path + '/events', verify_effective_access=False)
"""
    owner(prepare, source, snapshot, upstream, run, reuse)
    output = submit(source, snapshot, upstream, version, run)
    before = independent_probe(source, snapshot, upstream, version, run)
    reuse_output = submit(source, snapshot, upstream, version, reuse)
    # Full source/oracle verification is performed once. Identical persisted bytes
    # plus independently rechecked completion evidence bind reuse and recovery to it.
    after = independent_probe(source, snapshot, upstream, version, reuse, full=False)
    assert before["file_integrity"] == after["file_integrity"]
    assert before["sidecar_sha256"] == after["sidecar_sha256"]
    assert after["producer"] == before["producer"] == run
    # Completed frozen recovery must not recompute or alter any published bytes.
    recovered = submit(source, snapshot, upstream, version, run)
    recovery = independent_probe(source, snapshot, upstream, version, run, full=False)
    for key in ("file_integrity", "sidecar_sha256", "completion_sha256", "producer"):
        assert recovery[key] == before[key], f"Frozen recovery changed {key}"
    evidence = {
        "initial": before,
        "reuse": after,
        "recovery": recovery,
        "submission": output,
        "reuse_submission": reuse_output,
        "recovery_submission": recovered,
    }
    report = owner(
        """
import json, sys
from collections import Counter
from warehouse_bigdata.common.hdfs_client import HdfsCliClient
c = HdfsCliClient(metadata_limit=64 * 1024 * 1024)
run = sys.argv[1]
base = '/data/warehouse/staging/pipeline_run_id=' + run
quality = json.loads(c.read_remote(base + '/quality_report.json'))
counts = Counter()
metrics = Counter()
for path in c.list_children(base + '/events'):
    for line in c.read_remote(path).splitlines():
        event = json.loads(line)
        if (event.get('Event') == 'SparkListenerTaskEnd'
                and event.get('Task End Reason', {}).get('Reason') == 'Success'
                and not event['Task Info'].get('Failed', False)
                and not event['Task Info'].get('Killed', False)):
            counts[event['Task Info']['Host']] += 1
            for key in ('Memory Bytes Spilled', 'Disk Bytes Spilled', 'JVM GC Time'):
                metrics[key] += event.get('Task Metrics', {}).get(key, 0)
            task = event.get('Task Metrics', {})
            metrics['Input Bytes'] += task.get('Input Metrics', {}).get('Bytes Read', 0)
            metrics['Output Bytes'] += task.get('Output Metrics', {}).get('Bytes Written', 0)
assert all(counts[host] > 0 for host in ('spark-worker-1', 'spark-worker-2')), counts
print(json.dumps({'quality': quality, 'worker_tasks': counts, 'task_metrics': metrics}))
""",
        run,
    )
    evidence["runtime"] = json.loads(report)
    path = tmp_path / "j04-evidence.json"
    path.write_text(json.dumps(evidence, sort_keys=True, indent=2))
    print(f"J04 evidence: {path}")
