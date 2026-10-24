"""Bounded read contracts use synthetic rows only."""
from datetime import date, timedelta
from types import SimpleNamespace

import pytest
from dataclasses import replace

from warehouse_bigdata.application.read_repository import ClickHouseReadRepository, ReadLimits
from warehouse_bigdata.contracts.manifests import PublicationContext, PublicationCoverage


def context():
    from warehouse_bigdata.publication.batch import MAPPINGS
    bindings = {name: {'source_snapshot_id': 'source'} for name in MAPPINGS}
    bindings['forecast_daily'].update(model_run_id='model', forecast_run_id='forecast', issue_date='2016-05-22')
    bindings['central_supply_scenario'] = {'scenario_id': 'scenario', 'issue_date': '2016-05-22'}
    bindings.pop('forecast_metrics')
    bindings.pop('inventory_policy_evaluation')
    return PublicationContext('1.0', 'approved', 'sha256:x', 'source', 'model', 'forecast',
        'scenario', 'decision', None, None, date(2016, 5, 22), ('p',),
        PublicationCoverage(100, 100, 100, 700, ('S',)),
        bindings)


class Client:
    database = 'warehouse'
    def __init__(self, rows=(), error=None):
        self.rows, self.error, self.calls = rows, error, []
    def query(self, sql, **kwargs):
        self.calls.append((sql, kwargs))
        if self.error:
            raise self.error
        return SimpleNamespace(named_results=lambda: iter(self.rows))


def test_parameters_limits_and_empty_provenance():
    client = Client()
    ctx = replace(context(), coverage=replace(context().coverage, available_stores=("S' OR 1",)))
    repo = ClickHouseReadRepository(ctx, client, ReadLimits())
    result = repo.get_store_trend("S' OR 1", date(2016, 1, 1), date(2016, 1, 7))
    assert result.state == 'empty' and result.provenance == 'observed'
    sql, args = client.calls[0]
    assert "S' OR 1" not in sql and args['parameters']['store_id'] == "S' OR 1"
    assert args['settings']['skip_unavailable_shards'] == 0
    assert 'LIMIT' in sql and 'source_snapshot_id' in args['parameters']


def test_all_seven_operations_and_missing_historical_binding():
    repo = ClickHouseReadRepository(context(), Client(), ReadLimits())
    assert repo.get_forecast_series('I', 'S').state == 'incomplete'
    assert repo.get_forecast_metrics().state == 'unavailable'
    assert repo.get_policy_evaluation('I').state == 'unavailable'
    assert repo.get_replenishment('I', 'S').state == 'empty'
    assert repo.get_central_supply('I').state == 'empty'
    assert repo.get_allocation('I', 'p').state == 'incomplete'
    assert repo.get_allocation('I', 'other').state == 'error'


def test_forecast_requires_exact_dates_status_and_finite_predictions():
    rows = [dict(item_id='I', store_id='S', horizon=i, forecast_date=context().issue_date + timedelta(days=i),
                 forecast_status='ok', predicted_units=1) for i in range(1, 8)]
    client = Client(rows)
    repo = ClickHouseReadRepository(context(), client, ReadLimits())
    assert repo.get_forecast_series('I', 'S').state == 'ok'
    rows[0]['predicted_units'] = float('nan')
    assert repo.get_forecast_series('I', 'S').state == 'incomplete'


def test_supply_is_one_row_and_shard_failure_is_unavailable():
    repo = ClickHouseReadRepository(context(), Client([{'item_id': 'I', 'central_available_units': 4}] * 2), ReadLimits())
    assert repo.get_central_supply('I').state == 'incomplete'
    repo = ClickHouseReadRepository(context(), Client(error=OSError('shard down')), ReadLimits())
    assert repo.get_central_supply('I').state == 'unavailable'


def test_date_and_limits_rejected():
    with pytest.raises(ValueError):
        ReadLimits(max_rows=0)
    repo = ClickHouseReadRepository(context(), Client(), ReadLimits())
    assert repo.get_store_trend('S', date(2011, 1, 1), date(2016, 1, 1)).state == 'error'


def test_incompatible_historical_binding_is_not_queried():
    ctx = context()
    bindings = dict(ctx.dataset_bindings, forecast_metrics={
        'evaluation_run_id': 'history', 'run_type': 'test', 'model_run_id': 'other'})
    client = Client()
    result = ClickHouseReadRepository(replace(ctx, dataset_bindings=bindings), client).get_forecast_metrics()
    assert result.state == 'unavailable' and not client.calls


def test_compatible_metrics_use_historical_identity_and_preserve_null_metric():
    ctx = context()
    common = dict(source_snapshot_id='source', model_run_id='model',
                  selection_digest='frozen', dataset_scope='demo_sample')
    bindings = dict(ctx.dataset_bindings)
    bindings['forecast_daily'] = dict(bindings['forecast_daily'], **common)
    bindings['forecast_metrics'] = dict(common, evaluation_run_id='historical', run_type='test',
        holdout_id='historical-holdout', origin_forecasts={'origin': 'history-forecast'})
    client = Client([{'evaluation_run_id': 'historical', 'metric_value': None}])
    result = ClickHouseReadRepository(replace(ctx, dataset_bindings=bindings), client).get_forecast_metrics()
    assert result.state == 'ok' and result.rows[0]['metric_value'] is None
    params = client.calls[0][1]['parameters']
    assert params['evaluation_run_id'] == 'historical' and 'forecast_run_id' not in params


def test_lineage_mismatch_discards_all_rows():
    repo = ClickHouseReadRepository(context(), Client([{'source_snapshot_id': 'other'}]))
    result = repo.get_store_trend('S', date(2016, 1, 1), date(2016, 1, 2))
    assert result.state == 'error' and result.rows == ()


def forecasts(item='I'):
    return [dict(item_id=item, store_id='S', horizon=i,
                 forecast_date=context().issue_date + timedelta(days=i),
                 forecast_status='ok', predicted_units=0, source_snapshot_id='source')
            for i in range(1, 8)]


def test_catalog_order_literal_search_and_candidate_limit():
    client = Client(forecasts('z') + forecasts('A_%') + forecasts('a'))
    repo = ClickHouseReadRepository(context(), client)
    result = repo.get_eligible_items('S', '', 2)
    assert result.rows == ({'item_id': 'A_%', 'store_id': 'S'}, {'item_id': 'a', 'store_id': 'S'})
    assert result.provenance == 'predicted'
    assert repo.get_eligible_items('S', 'a_%').rows == ({'item_id': 'A_%', 'store_id': 'S'},)
    assert repo.get_eligible_items('S', 'missing').state == 'empty'
    assert client.calls[0][1]['parameters']['result_limit'] == 1001


@pytest.mark.parametrize('change', [
    {'horizon': 2}, {'forecast_date': date(2000, 1, 1)}, {'forecast_status': 'bad'},
    {'predicted_units': -1}, {'predicted_units': float('inf')}, {'predicted_units': True},
])
def test_catalog_rejects_malformed_matching_pair(change):
    rows = forecasts()
    rows[0].update(change)
    result = ClickHouseReadRepository(context(), Client(rows)).get_eligible_items('S', '')
    assert result.state == 'incomplete' and not result.rows


def test_catalog_missing_horizon_and_raw_overflow():
    assert ClickHouseReadRepository(context(), Client(forecasts()[:-1])).get_eligible_items('S', '').state == 'incomplete'
    result = ClickHouseReadRepository(context(), Client(forecasts() + forecasts('J')),
                                     ReadLimits(max_rows=7)).get_eligible_items('S', 'I', 1)
    assert result.state == 'incomplete' and not result.rows


@pytest.mark.parametrize('limit', [True, 0, 51, 1.5, '2'])
def test_catalog_invalid_limit_does_not_query(limit):
    client = Client()
    result = ClickHouseReadRepository(context(), client).get_eligible_items('S', '', limit)
    assert result.state == 'error' and result.provenance == 'predicted' and not client.calls


def test_invalid_inputs_and_unapproved_stores_do_not_query():
    client = Client()
    repo = ClickHouseReadRepository(context(), client)
    results = [repo.get_eligible_items('other', ''), repo.get_eligible_items('S', None),
               repo.get_forecast_series('', 'S'), repo.get_replenishment('I', 'other'),
               repo.get_store_trend('other', date.today(), date.today()),
               repo.get_item_history(None, 'S', date.today(), date.today()),
               repo.get_central_supply(''), repo.get_allocation('I', 'other')]
    assert all(r.state == 'error' and r.provenance and not r.rows for r in results)
    assert not client.calls


def test_item_history_grain_zeros_gaps_and_bindings():
    start = date(2016, 1, 1)
    rows = [dict(item_id='I', store_id='S', sale_date=start + timedelta(days=i),
                 unit_sales=0, source_snapshot_id='source') for i in (0, 2)]
    client = Client(rows)
    result = ClickHouseReadRepository(context(), client).get_item_history('I', 'S', start, start + timedelta(days=365))
    assert result.state == 'ok' and result.rows == tuple(rows) and result.provenance == 'observed'
    sql, args = client.calls[0]
    assert '.sales_daily ' in sql and 'ORDER BY sale_date' in sql
    assert args['parameters']['item_id'] == 'I' and args['parameters']['source_snapshot_id'] == 'source'
    assert args['settings']['readonly'] == 1 and args['settings']['result_overflow_mode'] == 'throw'
    assert ClickHouseReadRepository(context(), Client(rows * 2)).get_item_history('I', 'S', start, start).state == 'incomplete'


@pytest.mark.parametrize('start,end', [('2016-01-01', date.today()),
    (date(2016, 1, 2), date(2016, 1, 1)), (date(2016, 1, 1), date(2017, 1, 1))])
def test_item_history_invalid_ranges(start, end):
    client = Client()
    result = ClickHouseReadRepository(context(), client).get_item_history('I', 'S', start, end)
    assert result.state == 'error' and result.provenance == 'observed' and not client.calls


def test_returned_pair_mismatch_is_error():
    rows = forecasts()
    rows[0]['store_id'] = 'other'
    result = ClickHouseReadRepository(context(), Client(rows)).get_eligible_items('S', '')
    assert result.state == 'error' and not result.rows


def test_partial_iterator_failure_discards_rows():
    def rows():
        yield forecasts()[0]
        raise OSError('lost shard')
    result = ClickHouseReadRepository(context(), Client(rows())).get_eligible_items('S', '')
    assert result.state == 'unavailable' and result.provenance == 'predicted' and not result.rows


def test_allocation_remains_whole_product_and_supply_unique():
    client = Client([{'item_id': 'I', 'store_id': 'S', 'policy_id': 'p'}])
    assert ClickHouseReadRepository(context(), client).get_allocation('I', 'p').state == 'ok'
    assert 'store_id' not in client.calls[0][1]['parameters']
    assert ClickHouseReadRepository(context(), Client([{'item_id': 'I'}])).get_central_supply('I').state == 'ok'


def test_policy_evaluation_approved_policies_and_historical_decisions():
    ctx = context()
    common = dict(source_snapshot_id='source', model_run_id='model', selection_digest='frozen', dataset_scope='demo_sample')
    bindings = dict(ctx.dataset_bindings)
    bindings['forecast_daily'] = dict(bindings['forecast_daily'], **common)
    bindings['inventory_policy_evaluation'] = dict(common, evaluation_run_id='history', run_type='test',
        policy_ids=['p', 'other'], decision_run_ids={'p': 'old-p', 'other': 'old-other'},
        label_provenance='observed_m5_unit_sales_demand_proxy')
    client = Client([{'item_id': 'I', 'policy_id': 'p', 'decision_run_id': 'old-p', 'metric_value': None}])
    repo = ClickHouseReadRepository(replace(ctx, dataset_bindings=bindings), client)
    assert repo.get_policy_evaluation('I').state == 'ok'
    assert 'old-p' in client.calls[0][1]['parameters'].values()
    assert 'approved_policy_ids' in client.calls[0][1]['parameters']
    client.rows = [{'item_id': 'I', 'policy_id': 'other', 'decision_run_id': 'old-other'}]
    assert repo.get_policy_evaluation('I').state == 'error'
