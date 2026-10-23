"""Bounded J07 persistence and owner lifecycle using an independent storage double."""

import copy
import json
from pathlib import PurePosixPath
from types import SimpleNamespace

import pytest

from tests.unit.test_decision_inputs import fixture_rows, historical_rows
from tests.unit.test_hdfs_namespace import CONFIG, PermissionHdfs
from tests.unit.test_policy_evaluation import frozen, labels, score
from warehouse_bigdata.common.hdfs_namespace import provision_publication_namespace
from warehouse_bigdata.common.hdfs_publication import _bytes, finalize_publication
from warehouse_bigdata.optimization import inventory_artifacts as artifacts
from warehouse_bigdata.optimization.decision_batch import build_decision_batch
from warehouse_bigdata.optimization.decision_inputs import bind_decision_inputs


def persisted_scope_rows(rows):
    """Extend the pure-policy fixture with a complete, self-digested v1 sidecar."""
    from warehouse_bigdata.contracts.dataset_scope import DEPARTMENTS, STORES, scope_digest, select_scope
    identities = [dict(item_id=f'sku{i}', store_id=s, dept_id=DEPARTMENTS[i % len(DEPARTMENTS)],
        cat_id=DEPARTMENTS[i % len(DEPARTMENTS)].rsplit('_', 1)[0], state_id=s[:2],
        id=f'sku{i}_{s}_evaluation') for i in range(10) for s in STORES]
    scope = select_scope(identities, source_version=rows['selector']['source_version'],
                         source_snapshot_id=rows['selector']['source_snapshot_id'])
    scope.update(original_series_count=30490, excluded_series_count=30390)
    scope['selection_digest'] = scope_digest(scope)
    rows['scope'].clear()
    rows['scope'].update(scope)
    rows['selector']['selection_digest'] = scope['selection_digest']
    if 'cohort_rows' in rows:
        for row in rows['cohort_rows']:
            row['selection_digest'] = scope['selection_digest']
    if 'binding' in rows:
        rows['binding']['scope'] = copy.deepcopy(scope)
    return rows


@pytest.fixture
def storage(monkeypatch):
    client = PermissionHdfs()
    provision_publication_namespace(client, CONFIG)
    staging = CONFIG.base_path + '/staging/pipeline_run_id=inventory-attempt'
    client.mkdir_exclusive(staging)
    client.list_children = lambda p: tuple(s['path'] for s in client.list_status(p))
    client.list_status = lambda p: tuple(dict(path=k, type=v['type'], length=len(client.files.get(k, b'')))
        for k, v in sorted(client.entries.items()) if str(PurePosixPath(k).parent) == p)
    client.checksums = lambda paths: {p: artifacts.document_digest({'bytes': client.files[p].hex()}) for p in paths}

    # Persist bytes and deserialize on every read, including after every rename.
    def write(spark, uri, contract, rows):
        path = uri.removeprefix(CONFIG.uri)
        client.mkdir_exclusive(path)
        client.upload_bytes(_bytes(dict(schema=artifacts.physical_descriptor(contract), rows=list(rows))),
                            path + '/part-00000.parquet')

    def read(spark, uri, contract):
        path = uri.removeprefix(CONFIG.uri)
        data = json.loads(client.read_remote(path + '/part-00000.parquet'))
        artifacts.validate_physical_descriptor(contract, data['schema'])
        return data['rows'], data['schema']

    monkeypatch.setattr(artifacts, '_write_dataset', write)
    monkeypatch.setattr(artifacts, '_read_dataset', read)
    monkeypatch.setattr(artifacts, '_owner_spark', lambda: SimpleNamespace(stop=lambda: None))
    return client, staging


def decision(storage):
    client, _ = storage
    inputs = bind_decision_inputs(**persisted_scope_rows(fixture_rows()))
    result = build_decision_batch(inputs, decision_run_id='decision')
    return artifacts.persist_decision_bundle(object(), client=client, config=CONFIG,
        pipeline_run_id='inventory-attempt', inputs=inputs, result=result,
        code_revision='test-revision', effective_configuration={'hdfs_uri': CONFIG.uri})


def evaluation(storage, units=10):
    batch = frozen(persisted_scope_rows(historical_rows()))
    observed = labels(batch, units)
    return artifacts.persist_policy_evaluation_bundle(object(), client=storage[0], config=CONFIG,
        pipeline_run_id='inventory-attempt', frozen=batch, labels=observed, result=score(batch, observed),
        code_revision='test-revision', effective_configuration={'hdfs_uri': CONFIG.uri})


@pytest.mark.parametrize('kind', ['decision', 'evaluation'])
def test_persistence_reload_promotion_and_no_write_reuse(storage, kind):
    manifest = decision(storage) if kind == 'decision' else evaluation(storage, 0)
    client, staging = storage
    assert manifest['status'] == 'VALIDATED'
    binding = json.loads(client.files[staging + '/input_binding.json'])
    assert binding['pipeline_run_id'] == 'inventory-attempt'
    assert finalize_publication(client, CONFIG, pipeline_run_id='inventory-attempt') == 'VALIDATED'
    final = manifest['artifact_uri'].removeprefix(CONFIG.uri)
    artifacts.verify_inventory_bundle(object(), client=client, config=CONFIG, path=final)
    client.calls.clear()
    assert finalize_publication(client, CONFIG, pipeline_run_id='inventory-attempt') == 'REUSED'
    assert client.calls == []
    if kind == 'evaluation':
        evidence = json.loads(client.files[final + '/evidence/audit.json'])
        assert evidence['metrics']['equal_share']['fulfillment_rate'] is None
        assert len(evidence['daily_rows']) == 28
        assert evidence['metadata']['label_provenance'] == 'observed_m5_unit_sales_demand_proxy'
    else:
        assert len(json.loads(manifest['decision_artifact_uris'])) == 4


@pytest.mark.parametrize('change', ['missing', 'extra', 'scope', 'duplicate', 'schema', 'lineage',
                                    'exclusion', 'capacity', 'checksum'])
def test_tampered_decision_cannot_publish(storage, change):
    manifest = decision(storage)
    client, staging = storage
    root = staging + '/candidates/inventory'
    path = root + '/allocation_recommendations/part-00000.parquet'
    if change == 'missing':
        client.remove(path)
    elif change == 'extra':
        client.upload_bytes(b'bad', root + '/extra.json')
    elif change in {'scope', 'exclusion'}:
        path = root + ('/_scope.json' if change == 'scope' else '/evidence/audit.json')
        data = json.loads(client.files[path])
        if change == 'scope':
            data['selection_digest'] = 'sha256:' + 'f' * 64
        else:
            data['decision_metadata']['exclusions'] = []
        client.files[path] = _bytes(data)
    else:
        data = json.loads(client.files[path])
        if change == 'duplicate':
            data['rows'].append(data['rows'][0])
        elif change == 'schema':
            data['schema'][0]['type'] = 'bigint'
        elif change == 'lineage':
            data['rows'][0]['forecast_run_id'] = 'wrong'
        elif change == 'capacity':
            data['rows'][0]['allocated_units'] += 100
        else:
            data['unrecognized'] = True
        client.files[path] = _bytes(data)
    with pytest.raises((ValueError, RuntimeError, KeyError, AssertionError)):
        finalize_publication(client, CONFIG, pipeline_run_id='inventory-attempt')
    assert not client.exists(manifest['artifact_uri'].removeprefix(CONFIG.uri))


@pytest.mark.parametrize('boundary', ['claim', 'seal', 'final', 'prepare', 'manifest'])
def test_interruption_recovers_without_driver_recomputation(storage, boundary):
    manifest = decision(storage)
    client, _ = storage
    rename, chmod = client.rename_exclusive, client.set_permission
    hit = False
    def interrupted_rename(source, destination):
        nonlocal hit
        if boundary == 'prepare' and destination.endswith('/decision_manifest.json') and not hit:
            hit = True
            raise RuntimeError('interrupted')
        rename(source, destination)
        if not hit and ((boundary == 'claim' and destination.endswith('/candidate'))
                or (boundary == 'final' and destination == manifest['artifact_uri'].removeprefix(CONFIG.uri))
                or (boundary == 'manifest' and destination.endswith('/decision_manifest.json'))):
            hit = True
            raise RuntimeError('interrupted')
    def interrupted_seal(path, mode, **kwargs):
        nonlocal hit
        chmod(path, mode, **kwargs)
        if boundary == 'seal' and not hit and path.endswith('/candidate'):
            hit = True
            raise RuntimeError('interrupted')
    client.rename_exclusive, client.set_permission = interrupted_rename, interrupted_seal
    with pytest.raises(RuntimeError, match='interrupted'):
        finalize_publication(client, CONFIG, pipeline_run_id='inventory-attempt')
    assert hit
    client.rename_exclusive, client.set_permission = rename, chmod
    assert finalize_publication(client, CONFIG, pipeline_run_id='inventory-attempt') in {'VALIDATED', 'REUSED'}
    artifacts.verify_inventory_bundle(object(), client=client, config=CONFIG,
        path=manifest['artifact_uri'].removeprefix(CONFIG.uri))


def test_owner_and_conflicting_candidate_gates(storage):
    manifest = decision(storage)
    client, _ = storage
    before = copy.deepcopy((client.entries, client.files))
    client.identity = 'spark'
    with pytest.raises(RuntimeError, match='owner'):
        finalize_publication(client, CONFIG, pipeline_run_id='inventory-attempt')
    assert before == (client.entries, client.files)
    client.identity = 'hadoop'
    finalize_publication(client, CONFIG, pipeline_run_id='inventory-attempt')
    # An immutable identity cannot be used for another candidate.
    client.entries[storage[1] + '/candidates/inventory'] = dict(owner='hadoop', permission='755', type='DIRECTORY')
    with pytest.raises(RuntimeError, match='candidate'):
        finalize_publication(client, CONFIG, pipeline_run_id='inventory-attempt')


@pytest.mark.parametrize('change', ['daily', 'freeze', 'policy', 'labels'])
def test_evaluation_semantics_reject_tampering(storage, change):
    evaluation(storage)
    client, staging = storage
    path = staging + '/candidates/inventory/evidence/audit.json'
    audit = json.loads(client.files[path])
    if change == 'daily':
        audit['daily_rows'][0]['remaining_stock_units'] += 1
    elif change == 'freeze':
        audit['frozen']['supply'][0]['central_available_units'] = 0
    elif change == 'policy':
        audit['metadata']['decision_run_ids']['equal_share'] = 'proposed'
    else:
        audit['labels'][0]['date'] = '2099-01-01'
    client.files[path] = _bytes(audit)
    with pytest.raises((ValueError, RuntimeError)):
        finalize_publication(client, CONFIG, pipeline_run_id='inventory-attempt')


def test_existing_execution_evidence_is_not_overwritten(storage):
    decision(storage)
    before = copy.deepcopy((storage[0].entries, storage[0].files))
    with pytest.raises(RuntimeError, match='staging|evidence'):
        decision(storage)
    assert before == (storage[0].entries, storage[0].files)


@pytest.mark.parametrize('kind', ['decision', 'evaluation'])
def test_recovery_rejects_each_changed_binding(storage, kind):
    decision(storage) if kind == 'decision' else evaluation(storage)
    binding = json.loads(storage[0].files[storage[1] + '/input_binding.json'])
    args = SimpleNamespace(**{**binding['upstream_identities'], **binding,
                             **binding['effective_configuration']})
    artifacts.validate_inventory_recovery(args, binding)
    for key in ('pipeline_run_id', 'source_snapshot_id', 'selection_digest', 'forecast_run_id',
                'model_run_id', 'scenario_id', 'issue_date', 'configuration_digest', 'hdfs_uri'):
        altered = copy.copy(args)
        setattr(altered, key, 'different')
        with pytest.raises(RuntimeError, match='recovery'):
            artifacts.validate_inventory_recovery(altered, binding)


def test_readback_failure_retains_evidence_without_request(storage, monkeypatch):
    def corrupt(*args):
        raise RuntimeError('wrong physical schema')
    monkeypatch.setattr(artifacts, '_read_dataset', corrupt)
    with pytest.raises(RuntimeError, match='physical schema'):
        decision(storage)
    client, staging = storage
    assert client.exists(staging + '/failure_report.json')
    assert client.exists(staging + '/candidates/inventory/evidence/audit.json')
    assert not client.exists(staging + '/publication/request.json')


@pytest.mark.parametrize('change', ['order', 'type', 'nullable', 'extra'])
def test_explicit_physical_schema_is_not_coerced(change):
    contract = artifacts.CONTRACTS[0]
    schema = artifacts.physical_descriptor(contract)
    if change == 'order':
        schema.reverse()
    elif change == 'extra':
        schema.append(dict(name='extra', type='string', nullable=True))
    else:
        schema[0]['nullable' if change == 'nullable' else 'type'] = False if change == 'nullable' else 'bigint'
    with pytest.raises(RuntimeError, match='physical schema'):
        artifacts.validate_physical_descriptor(contract, schema)


def test_reader_checks_each_file_schema_and_disables_missing_corrupt_skips():
    contract = artifacts.CONTRACTS[0]
    descriptor = artifacts.physical_descriptor(contract)
    def schema(values):
        return [SimpleNamespace(name=f['name'], nullable=f['nullable'],
            dataType=SimpleNamespace(simpleString=lambda t=f['type']: t)) for f in values]
    calls = []
    frame = SimpleNamespace(schema=schema(descriptor), inputFiles=lambda: ['/bundle/part-1.parquet'],
                            limit=lambda n: SimpleNamespace(collect=lambda: []))
    reader = SimpleNamespace()
    def option(key, value):
        calls.append((key, value))
        return reader
    def parquet(uri):
        calls.append(('read', uri))
        return frame if uri == '/bundle' else SimpleNamespace(schema=schema(descriptor[:-1]))
    reader.option, reader.parquet = option, parquet
    with pytest.raises(RuntimeError, match='physical schema'):
        artifacts._read_dataset(SimpleNamespace(read=reader), '/bundle', contract)
    assert ('ignoreMissingFiles', 'false') in calls and ('ignoreCorruptFiles', 'false') in calls
    assert ('read', '/bundle/part-1.parquet') in calls
