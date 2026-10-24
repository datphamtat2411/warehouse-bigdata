"""Read-only approved-publication reconciliation and evidence entry point."""
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
from time import perf_counter
from uuid import uuid4

from warehouse_bigdata.application.read_repository import (
    approved_metadata, binding_filter, load_approved_publication_context,
)
from warehouse_bigdata.common.config import load_clickhouse_config, load_runtime_config
from warehouse_bigdata.common.small_artifacts import load_small_artifacts
from warehouse_bigdata.publication.batch import ClickHouseServing, MAPPINGS, _new_stats, _same_stats


@dataclass(frozen=True)
class VerificationEvidence:
    run_id: str
    details: dict


def require(condition, message):
    if not condition:
        raise ValueError(message)


class VerificationReader(ClickHouseServing):
    def __init__(self, context, client):
        super().__init__(client, None, client.database)
        self.context, self.timings, self.parameters = context, [], {}

    def _query(self, sql):
        start = perf_counter()
        result = self.publisher.query(sql, parameters=self.parameters, settings={
            'skip_unavailable_shards': 0, 'readonly': 1, 'max_execution_time': 30,
            'max_result_rows': 10000, 'result_overflow_mode': 'throw'}).result_rows
        self.timings.append(perf_counter() - start)
        return result

    def aggregate(self, name, source, grouped=False):
        where, self.parameters = binding_filter(self.context, name)
        return self._aggregate(name, source + ' WHERE ' + where, grouped=grouped)


def verify_distributed_publication(context, client, environment):
    """Reconcile both physical nodes with immutable approved source statistics."""
    manifest = environment.get('manifest')
    storage = environment.get('storage')
    require(manifest and storage and set(storage) == {'clickhouse-1', 'clickhouse-2'},
            'authoritative metadata and inspected storage required')
    require(storage['clickhouse-1'] != storage['clickhouse-2'], 'shared ClickHouse storage')
    require(manifest['publication_id'] == context.publication_id, 'publication identity differs')
    reader = VerificationReader(context, client)
    version = reader._query('SELECT version()')[0][0]
    require(version.startswith('26.8.'), 'pinned ClickHouse version required')
    topology = reader._query("SELECT shard_num,replica_num,shard_weight FROM system.clusters "
                             "WHERE cluster='warehouse_cluster' ORDER BY shard_num,replica_num")
    require(len(topology) == 2 and [tuple(r[:2]) for r in topology] == [(1, 1), (2, 1)]
            and topology[0][2] == topology[1][2] and topology[0][2] > 0,
            'exactly two equal-weight shards required')
    nodes = reader._query("SELECT hostName() FROM clusterAllReplicas('warehouse_cluster',system.one) ORDER BY hostName()")
    require(nodes == [('clickhouse-1',), ('clickhouse-2',)], 'physical nodes unavailable')
    reports = {}
    for name, mapping in MAPPINGS.items():
        table = mapping.serving_dataset
        reader.parameters = {'db': reader.database, 'table': table + '_local'}
        schemas = reader._query("SELECT hostName(),name,type,position FROM "
            "clusterAllReplicas('warehouse_cluster',system.columns) "
            "WHERE database={db:String} AND table={table:String} ORDER BY hostName(),position")
        by_node = {node: [tuple(r[1:]) for r in schemas if r[0] == node]
                   for node in ('clickhouse-1', 'clickhouse-2')}
        require(by_node['clickhouse-1'] and by_node['clickhouse-1'] == by_node['clickhouse-2'],
                'local schemas differ: ' + name)
        engines = reader._query("SELECT hostName(),engine FROM clusterAllReplicas('warehouse_cluster',system.tables) "
            "WHERE database={db:String} AND name={table:String} ORDER BY hostName()")
        require(engines == [('clickhouse-1', 'MergeTree'), ('clickhouse-2', 'MergeTree')],
                'local engines differ: ' + name)
        physical = f"clusterAllReplicas('warehouse_cluster',{reader.database},{table}_local)"
        shards = reader.aggregate(name, physical, True)
        for shard in (1, 2):
            shards.setdefault(shard, _new_stats(name))
        require(set(shards) == {1, 2}, 'unexpected shard')
        distributed = reader.aggregate(name, f'{reader.database}.{table}')
        combined = _new_stats(name)
        for stats in shards.values():
            combined['rows'] += stats['rows']
            for section in ('measures', 'nulls'):
                for key, value in stats[section].items():
                    combined[section][key] += value
            combined['fingerprint'] = [(a + b) % 2**64 for a, b in
                                      zip(combined['fingerprint'], stats['fingerprint'], strict=True)]
        expected = manifest['source_statistics'][name]
        require(_same_stats(expected, manifest['release_digest_payload']['reconciliation'][name]['distributed']),
                'source statistics differ from checksum-bound approved reconciliation')
        require(expected['rows'] == manifest['datasets'][name]['expected_rows'] ==
                manifest['expected_rows_map'][name], 'approved source count mismatch')
        require(_same_stats(combined, expected) and _same_stats(distributed, expected),
                'local/Distributed/source reconciliation failed: ' + name)
        routing = {'store_daily_sales': 'store_id,sale_date',
                   'forecast_metrics': 'evaluation_run_id,holdout_id,method_id',
                   'central_supply_scenario': 'item_id'}.get(name, 'item_id,store_id')
        where, reader.parameters = binding_filter(context, name)
        errors = reader._query(f'SELECT countIf(_shard_num != 1+modulo(cityHash64({routing}),2)) '
                               f'FROM {physical} WHERE {where}')[0][0]
        require(errors == 0, 'routing mismatch: ' + name)
        reports[name] = dict(shards=shards, distributed=distributed, source=expected, routing_errors=errors)
    require(all(reports['sales_daily']['shards'][s]['rows'] > 0 for s in (1, 2)),
            'both physical shards require observed sales')
    require(reports['sales_daily']['source']['rows'] == 194100, 'frozen M5 Small required')
    # An isolated remote endpoint substitutes a failed shard without stopping services.
    reader.parameters = {}
    try:
        reader._query("SELECT count() FROM remote('127.0.0.1:1',system.one)")
    except Exception as error:
        text = str(error)
        require(any(token in text for token in ('NETWORK_ERROR', 'Connection refused', 'ALL_CONNECTION_TRIES_FAILED')),
                'outage probe failed for a reason other than unreachable shard')
        outage = {'state': 'failed_as_required', 'probe': 'isolated unreachable remote endpoint'}
    else:
        raise ValueError('outage probe returned partial/successful results')
    return VerificationEvidence('verify-' + uuid4().hex, dict(
        timestamp=datetime.now(timezone.utc).isoformat(), git_revision=environment['git_revision'],
        environment=environment['name'], clickhouse_version=version, topology=topology,
        storage=storage, publication_id=context.publication_id, checksum=context.manifest_checksum,
        scope_digest=context.dataset_bindings['forecast_daily']['selection_digest'],
        coverage=asdict(context.coverage), datasets=reports, query_seconds=reader.timings, outage=outage,
        limitations=['demo_sample only; no benchmark or HA claim',
                     'isolated unreachable endpoint; deployed services were not stopped'] ))


def write_verification_evidence(evidence, output_path):
    path = Path(output_path) / evidence.run_id
    path.mkdir(parents=True, exist_ok=False)
    (path / 'evidence.json').write_text(json.dumps(asdict(evidence), indent=2, default=str, allow_nan=False) + '\n')
    return path


def inspect_storage():
    result = {}
    for service in ('clickhouse-1', 'clickhouse-2'):
        container = subprocess.run(['docker', 'compose', '-f', 'infrastructure/compose.yaml',
            '--profile', 'clickhouse', 'ps', '-q', service], check=True, capture_output=True, text=True).stdout.strip()
        require(container and '\n' not in container, 'missing running service: ' + service)
        # Only Mounts are captured: never inspect/export container credential environments.
        mounts = json.loads(subprocess.run(['docker', 'inspect', '--format', '{{json .Mounts}}', container],
            check=True, capture_output=True, text=True).stdout)
        volumes = [m['Name'] for m in mounts if m['Destination'] == '/var/lib/clickhouse' and m['Type'] == 'volume']
        require(len(volumes) == 1, 'missing dedicated storage volume')
        result[service] = volumes[0]
    return result


def run_verification(publication=Path('artifacts/approved_publication.json'), evidence_dir=Path('artifacts/verification')):
    context = load_approved_publication_context(publication)
    _, manifest = approved_metadata(publication)
    selector = load_small_artifacts()
    for key in ('source_snapshot_id', 'selection_digest', 'model_run_id', 'dataset_scope'):
        require(selector[key] == context.dataset_bindings['forecast_daily'][key], 'selector identity differs: ' + key)
    config = load_clickhouse_config()
    import clickhouse_connect
    client = clickhouse_connect.get_client(host=config.host, port=config.port, database=config.database,
        username=config.user, password=config.password)
    try:
        evidence = verify_distributed_publication(context, client, dict(manifest=manifest,
            storage=inspect_storage(), name=load_runtime_config().environment,
            git_revision=subprocess.run(['git', 'rev-parse', 'HEAD'], check=True,
                capture_output=True, text=True).stdout.strip()))
        # Recheck fencing and approval identity after all queries.
        require(load_approved_publication_context(publication) == context, 'approval changed during verification')
        return write_verification_evidence(evidence, evidence_dir)
    finally:
        client.close()
