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
    repo = ClickHouseReadRepository(context(), client, ReadLimits())
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
    repo = ClickHouseReadRepository(context(), Client([{'central_available_units': 4}] * 2), ReadLimits())
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
