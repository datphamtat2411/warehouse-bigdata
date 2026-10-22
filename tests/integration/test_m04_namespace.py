"""Tiny live HDFS permission checks: no source profiling, SparkContext, or M5 processing."""

from __future__ import annotations

import json
import subprocess
import uuid
from pathlib import Path
from textwrap import dedent

import pytest

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[2]

_CODE = dedent("""\
    import json
    import sys

    from warehouse_bigdata.common.config import load_hdfs_config
    from warehouse_bigdata.common.hdfs_client import HdfsCliClient, HdfsCommandError
    from warehouse_bigdata.common.hdfs_namespace import (
        publication_parents, require_owner, validate_publication_namespace,
    )
    from warehouse_bigdata.common.hdfs_paths import render_staging_path
    from warehouse_bigdata.common.hdfs_permissions import prepare_spark_run_permissions
    from warehouse_bigdata.common.hdfs_publication import _seal

    action, token = sys.argv[1:]
    assert len(token) == 32 and all(c in '0123456789abcdef' for c in token)
    config = load_hdfs_config()
    client = HdfsCliClient()
    run_id = 'm04-namespace-' + token
    staging = render_staging_path(config.base_path, run_id)
    fixtures = [parent + '/.warehouse-permission-probe-' + token
                for parent in publication_parents(config)]
    if action == 'prepare':
        require_owner(client)
        assert client.safe_mode() == 'Safe mode is OFF'
        client.report()
        validate_publication_namespace(client, config, probe_writes=True)
        client.mkdir_exclusive(staging)
        prepare_spark_run_permissions(client, config, pipeline_run_id=run_id,
                                      verify_effective_access=False)
        for path in fixtures:
            client.mkdir_exclusive(path)
            client.upload_bytes(b'namespace-readiness-only-not-M5\\n', path + '/evidence')
            _seal(client, path)
    elif action == 'spark':
        assert client.effective_user() == 'spark'
        validate_publication_namespace(client, config)
        prepare_spark_run_permissions(client, config, pipeline_run_id=run_id)
        first, second = staging + '/candidate', staging + '/promoted-candidate'
        client.mkdir_exclusive(first)
        client.upload_bytes(b'run-scoped-write\\n', first + '/evidence')
        client.rename_exclusive(first, second)
        client.remove(second)
        denied = 0
        for path in fixtures:
            for operation in (
                lambda: client.remove(path),
                lambda: client.rename_exclusive(path, path + '-stolen'),
                lambda: client.upload_bytes(b'forbidden', path + '/forbidden'),
                lambda: client.set_permission(path + '/evidence', '644'),
            ):
                try:
                    operation()
                except HdfsCommandError as error:
                    if not any(text in str(error).lower() for text in (
                        'permission denied', 'accesscontrolexception', 'http 403', 'upload failed'
                    )):
                        raise
                    denied += 1
                else:
                    raise AssertionError('Spark mutated an owner-owned immutable fixture: ' + path)
        print(json.dumps({'status': 'READY', 'hdfs_user': client.effective_user(),
                          'denied_mutations': denied, 'staging_write_rename_delete': True}))
    elif action == 'cleanup':
        require_owner(client)
        for path in fixtures:
            if client.exists(path):
                assert client.read_remote(path + '/evidence') == b'namespace-readiness-only-not-M5\\n'
                client.remove(path)
            if client.exists(path + '-stolen'):
                client.remove(path + '-stolen')
        if client.exists(staging):
            client.remove(staging)
    else:
        raise AssertionError(action)
    """)


def _run(action: str, token: str, *, owner: bool):
    command = [
        "docker",
        "compose",
        "-f",
        str(ROOT / "infrastructure/compose.yaml"),
        "--profile",
        "spark-submit",
        "run",
        "--rm",
        "--no-deps",
        "-T",
        "--pull",
        "never",
        "--user",
        "1000:100" if owner else "185:185",
        "--entrypoint",
        "/opt/warehouse-app/.venv/bin/python",
        "spark-submit",
        "-c",
        _CODE,
        action,
        token,
    ]
    result = subprocess.run(command, capture_output=True, text=True, timeout=240, check=False)
    assert result.returncode == 0, result.stdout + result.stderr
    return result.stdout


def test_m04_namespace_permissions_without_spark_or_m5():
    token = uuid.uuid4().hex
    try:
        _run("prepare", token, owner=True)
        report = json.loads(_run("spark", token, owner=False))
        assert report == {
            "status": "READY",
            "hdfs_user": "spark",
            "denied_mutations": 20,
            "staging_write_rename_delete": True,
        }
        print(json.dumps(report))
    finally:
        _run("cleanup", token, owner=True)
