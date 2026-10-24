"""Synthetic Forecast presentation contracts and rerender checks."""
from dataclasses import replace
from datetime import date, timedelta
from unittest.mock import Mock

import pytest
from streamlit.testing.v1 import AppTest

from tests.unit.test_application_read_repository import context
from warehouse_bigdata.application.read_repository import ReadResult
from warehouse_bigdata.application.session_state import PREFIX


def forecast_rows():
    ctx = context()
    return [dict(item_id='I', store_id='S', forecast_run_id=ctx.forecast_run_id,
                 model_run_id=ctx.model_run_id, source_snapshot_id=ctx.source_snapshot_id,
                 issue_date=ctx.issue_date, horizon=i,
                 forecast_date=ctx.issue_date + timedelta(days=i), forecast_status='ok',
                 predicted_units=0 if i == 1 else 1.23456789) for i in range(1, 8)]


def historical_context():
    ctx = context()
    common = dict(source_snapshot_id='source', model_run_id='model',
                  selection_digest='frozen', dataset_scope='demo_sample')
    bindings = dict(ctx.dataset_bindings)
    bindings['forecast_daily'] = dict(bindings['forecast_daily'], **common)
    bindings['forecast_metrics'] = dict(common, evaluation_run_id='history', run_type='test',
        holdout_id='holdout', origin_forecasts={'origin': 'past'})
    return replace(ctx, dataset_bindings=bindings)


def metric_rows(horizon=0):
    return [dict(evaluation_run_id='history', model_run_id='model', holdout_id='holdout',
                 run_type='test', method_id=method, metric_name=metric, horizon=horizon,
                 metric_value=12.5 if metric == 'WAPE' else 0,
                 observation_count=100, sum_actual_units=200,
                 evaluation_start=date(2016, 4, 1), evaluation_end=date(2016, 4, 7))
            for method in ('random_forest', 'seasonal_naive')
            for metric in ('MAE', 'RMSE', 'WAPE')]


def test_sorted_precision_and_chart_source():
    from app.forecast import normalize_forecast, forecast_figure
    result = normalize_forecast(ReadResult('ok', tuple(reversed(forecast_rows())), 'predicted'),
                                context(), 'I', 'S')
    assert result.state == 'ok'
    assert [r['horizon'] for r in result.rows] == list(range(1, 8))
    assert list(forecast_figure(result.rows).data[0].y) == [r['predicted_units'] for r in result.rows]
    assert sum(r['predicted_units'] for r in result.rows) == pytest.approx(6 * 1.23456789)


@pytest.mark.parametrize('field,value', [
    ('horizon', True), ('horizon', 2), ('forecast_date', date(2000, 1, 1)),
    ('forecast_status', 'failed'), ('item_id', 'other'), ('store_id', 'other'),
    ('forecast_run_id', 'other'), ('model_run_id', 'other'),
    ('predicted_units', -1), ('predicted_units', True),
    ('predicted_units', float('nan')), ('predicted_units', float('inf'))])
def test_invalid_series(field, value):
    from app.forecast import normalize_forecast
    rows = forecast_rows()
    rows[0][field] = value
    assert normalize_forecast(ReadResult('ok', tuple(rows), 'predicted'), context(), 'I', 'S').state != 'ok'


@pytest.mark.parametrize('kind', ['missing', 'extra', 'provenance', 'overflow', 'missing_run'])
def test_incomplete_series(kind):
    from app.forecast import normalize_forecast
    rows, provenance = forecast_rows(), 'predicted'
    if kind == 'missing':
        rows.pop()
    elif kind == 'extra':
        rows.append(rows[0])
    elif kind == 'provenance':
        provenance = 'observed'
    elif kind == 'missing_run':
        rows[0].pop('forecast_run_id')
    else:
        rows = [dict(row, predicted_units=1e308) for row in rows]
    assert normalize_forecast(ReadResult('ok', tuple(rows), provenance), context(), 'I', 'S').state != 'ok'


def test_metrics_pooled_daily_null_and_percent():
    from app.forecast import compare_metrics
    rows = metric_rows(1) + metric_rows()
    result = compare_metrics(ReadResult('ok', tuple(rows), 'evaluated'), historical_context())
    assert result.state == 'ok' and result.rows[0]['horizon'] == 0
    assert result.rows[2]['Random Forest'] == '12,5%'
    assert result.rows[0]['Random Forest'] == '0,0'
    rows = metric_rows(2)
    rows[2]['metric_value'] = None
    rows[5]['metric_value'] = None
    result = compare_metrics(ReadResult('ok', tuple(rows), 'evaluated'), historical_context())
    assert result.rows[0]['horizon'] == 2
    assert result.rows[2]['Random Forest'] == 'Không xác định'


@pytest.mark.parametrize('field,value', [('evaluation_run_id', 'other'), ('holdout_id', 'other'),
    ('evaluation_end', date(2016, 3, 1)), ('horizon', True), ('method_id', 'unknown')])
def test_invalid_historical_identity(field, value):
    from app.forecast import compare_metrics
    rows = metric_rows()
    rows[0][field] = value
    assert compare_metrics(ReadResult('ok', tuple(rows), 'evaluated'), historical_context()).state != 'ok'


@pytest.mark.parametrize('field,value', [('observation_count', 99), ('sum_actual_units', 201),
    ('evaluation_start', date(2016, 4, 2))])
def test_incomparable_cells_unavailable(field, value):
    from app.forecast import compare_metrics
    rows = metric_rows()
    rows[3][field] = value
    result = compare_metrics(ReadResult('ok', tuple(rows), 'evaluated'), historical_context())
    assert result.rows[0]['Random Forest'] == 'Không khả dụng'
    assert result.rows[0]['Seasonal-naïve'] == 'Không khả dụng'


def test_duplicate_missing_and_bad_metrics():
    from app.forecast import compare_metrics
    rows = metric_rows()
    assert compare_metrics(ReadResult('ok', tuple(rows + [rows[0]]), 'evaluated'), historical_context()).state != 'ok'
    rows.pop(0)
    rows[0]['metric_value'] = float('nan')
    result = compare_metrics(ReadResult('ok', tuple(rows), 'evaluated'), historical_context())
    assert result.rows[0]['Random Forest'] == 'Không khả dụng'
    assert result.rows[1]['Random Forest'] == 'Không khả dụng'
    assert compare_metrics(ReadResult('ok', tuple(rows), 'observed'), historical_context()).state != 'ok'
    assert compare_metrics(ReadResult('ok', tuple(rows), 'evaluated'), context()).state != 'ok'


def render_page():
    import streamlit as st
    from app.forecast import render_forecast
    from warehouse_bigdata.application.session_state import selections
    render_forecast(st.session_state['ctx'], st.session_state['repo'], selections(st.session_state))


def page():
    repo = Mock()
    repo.get_eligible_items.return_value = ReadResult('ok', ({'item_id': 'I', 'store_id': 'S'},), 'predicted')
    repo.get_forecast_series.return_value = ReadResult('ok', tuple(forecast_rows()), 'predicted')
    repo.get_forecast_metrics.return_value = ReadResult('ok', tuple(metric_rows()), 'evaluated')
    at = AppTest.from_function(render_page)
    at.session_state['ctx'] = historical_context()
    at.session_state['repo'] = repo
    return at, repo


def test_selection_precision_and_failure_rerenders():
    at, repo = page()
    at.run()
    assert not at.exception and not at.metric
    repo.get_forecast_series.assert_not_called()
    at.selectbox[1].set_value('I').run()
    assert not at.exception and len(at.metric) == 3
    assert at.metric[0].value == '7,4'
    assert at.metric[1].value == '1,1'
    assert at.dataframe[0].value.iloc[1]['Số đơn vị bán dự đoán'] == 1.23456789
    for state in ('empty', 'incomplete', 'unavailable', 'error'):
        repo.get_forecast_series.return_value = ReadResult(state, reason='secret credentials')
        at.run()
        assert not at.exception and not at.metric and not at.dataframe
        assert 'secret' not in str([el.value for el in [*at.warning, *at.error, *at.info]])


def test_unsupported_selection_recovery_and_store_reset():
    at, repo = page()
    at.session_state[PREFIX + 'store_id'] = 'stale'
    at.session_state[PREFIX + 'item_id'] = 'stale'
    at.run()
    assert at.warning and at.selectbox
    at.selectbox[0].set_value('S').run()
    assert at.session_state[PREFIX + 'item_id'] is None
    at.session_state[PREFIX + 'item_id'] = 'stale'
    at.run()
    assert at.warning and len(at.selectbox) == 2 and not at.metric
    at.selectbox[1].set_value('I').run()
    assert at.metric and not at.exception
    repo.get_forecast_metrics.return_value = ReadResult('unavailable', reason='secret')
    at.run()
    assert at.metric and len(at.dataframe) == 1


@pytest.mark.parametrize('kind', ['duplicate', 'wrong_store', 'saturated', 'provenance', 'empty'])
def test_catalog_guards(kind):
    at, repo = page()
    rows = [{'item_id': 'I', 'store_id': 'S'}]
    if kind == 'duplicate':
        rows *= 2
    elif kind == 'wrong_store':
        rows[0]['store_id'] = 'other'
    elif kind == 'saturated':
        rows = [dict(item_id=str(i), store_id='S') for i in range(50)]
    elif kind == 'empty':
        rows = []
    repo.get_eligible_items.return_value = ReadResult('empty' if kind == 'empty' else 'ok', tuple(rows),
                                                     'observed' if kind == 'provenance' else 'predicted')
    at.session_state[PREFIX + 'item_id'] = 'I'
    at.run()
    assert not at.exception and not at.metric
    repo.get_forecast_series.assert_not_called()
