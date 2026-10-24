"""Publication-pinned, bounded analytical reads with explicit failure states."""
from dataclasses import dataclass
from datetime import date, timedelta
import json
import math
from pathlib import Path
import re

from warehouse_bigdata.common.config import load_hdfs_config
from warehouse_bigdata.common.hdfs_client import HdfsCliClient
from warehouse_bigdata.common.hdfs_publication import document_digest
from warehouse_bigdata.contracts.manifests import PublicationContext, PublicationCoverage
from warehouse_bigdata.contracts.validation import validate_publication
from warehouse_bigdata.publication.batch import IDENTITIES, MAPPINGS, validate_dataset_bindings


def approved_metadata(path):
    """Verify the exact local copy against the authoritative, unfenced HDFS approval."""
    payload = Path(path).read_bytes()
    copy = json.loads(payload)
    config = load_hdfs_config()
    hdfs = HdfsCliClient()
    parent = config.base_path + '/manifests/published/'
    if hdfs.exists(parent + 'unavailable.json'):
        raise ValueError('publication is fenced unavailable')
    if hdfs.read_remote(parent + 'current.json') != payload:
        raise ValueError('authoritative approval differs')
    expected_uri = config.base_path + '/manifests/publications/' + copy['publication_id'] + '.json'
    if copy.get('manifest_uri') != expected_uri:
        raise ValueError('invalid publication manifest URI')
    manifest = json.loads(hdfs.read_remote(expected_uri))
    validate_publication(manifest, context=manifest['publication_context'], application_copy=copy)
    if document_digest(manifest['release_digest_payload']) != copy['manifest_checksum']:
        raise ValueError('publication release checksum differs')
    validate_dataset_bindings(copy['dataset_bindings'])
    if set(copy['dataset_bindings']) != set(MAPPINGS):
        raise ValueError('incomplete dataset bindings')
    coverage = copy['coverage']
    if not (coverage['series_total'] == coverage['series_eligible'] == coverage['series_scored'] == 100
            and len(set(coverage['available_stores'])) == 10
            and coverage['loaded_rows'] == sum(manifest['expected_rows_map'].values())):
        raise ValueError('invalid frozen small coverage')
    return copy, manifest


def load_approved_publication_context(path=Path('artifacts/approved_publication.json')):
    copy, _ = approved_metadata(path)
    fields = {key: copy[key] for key in PublicationContext.__dataclass_fields__}
    fields['issue_date'] = date.fromisoformat(fields['issue_date'])
    fields['policy_ids'] = tuple(fields['policy_ids'])
    fields['coverage'] = PublicationCoverage(**fields['coverage'])
    return PublicationContext(**fields)


@dataclass(frozen=True)
class ReadLimits:
    timeout_seconds: int = 30
    max_rows: int = 1000
    max_history_days: int = 366

    def __post_init__(self):
        if not (1 <= self.timeout_seconds <= 120 and 7 <= self.max_rows <= 10000
                and 1 <= self.max_history_days <= 1941):
            raise ValueError('invalid bounded read limits')


@dataclass(frozen=True)
class ReadResult:
    state: str
    rows: tuple = ()
    provenance: str = ''
    reason: str = ''


def binding_filter(context, dataset):
    binding = (context.evaluation_binding(dataset) if dataset in
               {'forecast_metrics', 'inventory_policy_evaluation'} else
               (context.dataset_bindings or {}).get(dataset, {}))
    if dataset in {'forecast_metrics', 'inventory_policy_evaluation'}:
        primary = (context.dataset_bindings or {}).get('forecast_daily', {})
        for key in ('source_snapshot_id', 'model_run_id', 'selection_digest', 'dataset_scope'):
            if not binding.get(key) or binding[key] != primary.get(key):
                raise ValueError('incompatible historical binding: ' + key)
        if dataset == 'forecast_metrics' and not (binding.get('holdout_id') and binding.get('origin_forecasts')):
            raise ValueError('historical forecast cohort unavailable')
        if dataset == 'inventory_policy_evaluation' and not (
                binding.get('decision_run_ids') and binding.get('policy_ids')
                and binding.get('label_provenance') == 'observed_m5_unit_sales_demand_proxy'):
            raise ValueError('historical policy labels unavailable')
    columns = {f.serving_field for f in MAPPINGS[dataset].field_mappings}
    if dataset == 'sales_daily':
        columns.add('source_snapshot_id')
    params = {k: v for k, v in binding.items() if k in IDENTITIES and k in columns and v is not None}
    if not params:
        raise ValueError('publication dataset binding is unavailable')
    clauses = [f'`{k}` = {{{k}:Date}}' if k == 'issue_date' else
               f'`{k}` = {{{k}:String}}' for k in params]
    if 'decision_run_ids' in binding and 'policy_id' in columns:
        decisions = binding['decision_run_ids']
        clauses.append('(' + ' OR '.join(
            f'(policy_id={{ep{i}:String}} AND decision_run_id={{ed{i}:String}})'
            for i in range(len(decisions))) + ')')
        for i, (policy, decision) in enumerate(decisions.items()):
            params.update({f'ep{i}': policy, f'ed{i}': decision})
    return ' AND '.join(clauses), params


class ClickHouseReadRepository:
    def __init__(self, context, client, limits=ReadLimits()):
        if not isinstance(context, PublicationContext) or not context.publication_id:
            raise ValueError('validated publication context required')
        self.context, self.client, self.limits = context, client, limits
        self.database = client.database
        if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', self.database):
            raise ValueError('unsafe database')

    def _read(self, dataset, filters=None, *, order='', provenance='calculated'):
        try:
            where, params = binding_filter(self.context, dataset)
        except ValueError as error:
            return ReadResult('unavailable', provenance=provenance, reason=str(error))
        for key, value in (filters or {}).items():
            column, operator, kind = {'start_date': ('sale_date', '>=', 'Date'),
                'end_date': ('sale_date', '<=', 'Date')}.get(key, (key, '=', 'String'))
            where += f' AND `{column}` {operator} {{{key}:{kind}}}'
            params[key] = value
        sql = (f'SELECT * FROM {self.database}.{MAPPINGS[dataset].serving_dataset} WHERE {where}'
               + (f' ORDER BY {order}' if order else '') + ' LIMIT {result_limit:UInt32}')
        params['result_limit'] = self.limits.max_rows + 1
        try:
            rows = tuple(self.client.query(sql, parameters=params, settings={
                'skip_unavailable_shards': 0, 'max_execution_time': self.limits.timeout_seconds,
                'max_result_rows': self.limits.max_rows + 1, 'result_overflow_mode': 'throw',
                'readonly': 1}).named_results())
        except Exception:
            return ReadResult('unavailable', provenance=provenance, reason='query failed; no partial results')
        if len(rows) > self.limits.max_rows:
            return ReadResult('incomplete', provenance=provenance, reason='result bound exceeded')
        # Verify all returned lineage columns, even though SQL also pins them.
        for row in rows:
            for key in IDENTITIES:
                if key in row and key in params and str(row[key]) != str(params[key]):
                    return ReadResult('error', provenance=provenance, reason='lineage mismatch')
        return ReadResult('ok' if rows else 'empty', rows, provenance)

    def get_store_trend(self, store_id, start_date, end_date):
        if not (type(start_date) is date and type(end_date) is date and
                0 <= (end_date - start_date).days < self.limits.max_history_days):
            return ReadResult('error', reason='invalid historical range')
        return self._read('store_daily_sales', dict(store_id=store_id,
            start_date=start_date, end_date=end_date), order='sale_date', provenance='observed')

    def get_forecast_series(self, item_id, store_id):
        result = self._read('forecast_daily', dict(item_id=item_id, store_id=store_id),
                            order='horizon', provenance='predicted')
        if result.state not in {'ok', 'empty'}:
            return result
        valid = len(result.rows) == 7 and all(
            row.get('horizon') == i and row.get('forecast_date') == self.context.issue_date + timedelta(days=i)
            and row.get('forecast_status') == 'ok' and isinstance(row.get('predicted_units'), (int, float))
            and math.isfinite(row['predicted_units']) and row['predicted_units'] >= 0
            for i, row in enumerate(result.rows, 1))
        return result if valid else ReadResult('incomplete', provenance='predicted', reason='seven-day coverage missing')

    def get_forecast_metrics(self):
        result = self._read('forecast_metrics', provenance='evaluated')
        return (ReadResult('unavailable', provenance='evaluated', reason='historical metrics missing')
                if result.state == 'empty' else result)

    def _one(self, dataset, filters, provenance):
        result = self._read(dataset, filters, provenance=provenance)
        return (ReadResult('incomplete', provenance=provenance, reason='duplicate product/series rows')
                if len(result.rows) > 1 else result)

    def get_replenishment(self, item_id, store_id):
        return self._one('replenishment_recommendations', dict(item_id=item_id, store_id=store_id), 'calculated')

    def get_central_supply(self, item_id):
        return self._one('central_supply_scenario', dict(item_id=item_id), 'simulated')

    def get_allocation(self, item_id, policy_id):
        if policy_id not in self.context.policy_ids:
            return ReadResult('error', reason='unapproved policy')
        result = self._read('allocation_recommendations', dict(item_id=item_id, policy_id=policy_id))
        if result.state in {'ok', 'empty'} and (len(result.rows) != len(self.context.coverage.available_stores)
                or {r['store_id'] for r in result.rows} != set(self.context.coverage.available_stores)):
            return ReadResult('incomplete', provenance='calculated', reason='store coverage incomplete')
        return result

    def get_policy_evaluation(self, item_id):
        result = self._read('inventory_policy_evaluation', dict(item_id=item_id), provenance='evaluated')
        return (ReadResult('unavailable', provenance='evaluated', reason='historical policy outcomes missing')
                if result.state == 'empty' else result)
