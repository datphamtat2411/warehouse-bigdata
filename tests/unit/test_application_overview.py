"""Synthetic Overview integrity and Streamlit interaction checks."""
from dataclasses import replace
from datetime import date, timedelta
from unittest.mock import Mock

import pytest
from streamlit.testing.v1 import AppTest

from tests.unit.test_application_read_repository import context
from warehouse_bigdata.application.read_repository import ReadLimits, ReadResult


START = date(2016, 1, 1)
END = START + timedelta(days=2)


def daily(item=None):
    return tuple(dict(sale_date=START + timedelta(days=i), store_id='S',
                      **({'item_id': item, 'unit_sales': units} if item else {'total_units': units}))
                 for i, units in [(2, 3), (0, 0), (1, 2)])


def repository():
    repo = Mock(limits=ReadLimits())
    repo.get_store_history_coverage.return_value = ReadResult('ok', (dict(
        store_id='S', start_date=START, end_date=END, day_count=3, row_count=3),), 'observed')
    repo.get_eligible_items.return_value = ReadResult('ok', ({'item_id': 'I', 'store_id': 'S'},), 'predicted')
    repo.get_store_trend.return_value = ReadResult('ok', daily(), 'observed')
    repo.get_item_history.return_value = ReadResult('ok', daily('I'), 'observed')
    return repo


def app():
    import streamlit as st
    from app.views import render_overview
    from warehouse_bigdata.application.session_state import selections, sync_publication_state
    ctx = st.session_state['context']
    sync_publication_state(st.session_state, ctx)
    render_overview(ctx, st.session_state['repository'], selections(st.session_state))


def launch(repo=None, ctx=None):
    at = AppTest.from_function(app)
    at.session_state['context'] = ctx or context()
    at.session_state['repository'] = repo or repository()
    return at.run()


def test_routing_reconciliation_and_vietnamese_meaning():
    repo = repository()
    at = launch(repo)
    assert not at.exception
    repo.get_store_trend.assert_called_once_with('S', START, END)
    repo.get_item_history.assert_not_called()
    assert [metric.value for metric in at.metric] == ['1', '1', '3', '5']
    table = at.dataframe[0].value
    assert list(table['Số đơn vị bán quan sát']) == [0, 2, 3]
    assert list(table['Ngày']) == ['01/01/2016', '02/01/2016', '03/01/2016']
    text = '\n'.join(el.value for el in list(at.markdown) + list(at.info) + list(at.caption))
    for expected in ['Dữ liệu bán hàng lịch sử từ bộ M5 Walmart.', 'không phải', 'approved',
                     'source', '3.049', '30.490', '59.181.090', 'Nguồn']:
        assert expected in text
    assert len(at.get('plotly_chart')) == 1
    repo.reset_mock()
    at.selectbox[1].set_value('I').run()
    assert not at.exception
    repo.get_item_history.assert_called_once_with('I', 'S', START, END)
    repo.get_store_trend.assert_not_called()
    assert at.metric[-1].value == '5'
    assert set(at.dataframe[0].value['Sản phẩm']) == {'I'}


@pytest.mark.parametrize('state', ['empty', 'incomplete', 'unavailable', 'error'])
def test_success_to_failure_removes_all_sales(state):
    repo = repository()
    at = launch(repo)
    repo.get_store_trend.return_value = ReadResult(state, reason='secret')
    at.run()
    assert not at.exception and not at.dataframe and not at.get('plotly_chart')
    assert len(at.metric) == 3
    assert all('secret' not in el.value for el in list(at.warning) + list(at.error) + list(at.info))


@pytest.mark.parametrize('changes', [
    {'sale_date': START - timedelta(days=1)}, {'sale_date': END},
    {'total_units': -1}, {'total_units': float('nan')}, {'total_units': True},
    {'store_id': 'other'}, {'sale_date': '2016-01-01'},
])
def test_malformed_rows_are_not_partially_displayed(changes):
    repo = repository()
    rows = list(daily())
    rows[1] = dict(rows[1], **changes)
    repo.get_store_trend.return_value = ReadResult('ok', tuple(rows), 'observed')
    at = launch(repo)
    assert not at.exception and not at.dataframe and not at.get('plotly_chart')
    assert at.warning or at.error


@pytest.mark.parametrize('rows,provenance', [(daily()[:-1], 'observed'), (daily(), 'predicted'),
                                         ((), 'observed')])
def test_missing_observations_or_wrong_provenance(rows, provenance):
    repo = repository()
    repo.get_store_trend.return_value = ReadResult('ok', rows, provenance)
    at = launch(repo)
    assert not at.dataframe and not at.get('plotly_chart')


@pytest.mark.parametrize('state', ['empty', 'incomplete', 'unavailable', 'error'])
def test_coverage_failure_blocks_history_without_zero_metrics(state):
    repo = repository()
    repo.get_store_history_coverage.return_value = ReadResult(state)
    at = launch(repo)
    assert not at.exception and not at.dataframe
    repo.get_store_trend.assert_not_called()
    assert all(metric.value != '0' for metric in at.metric)


@pytest.mark.parametrize('state', ['empty', 'incomplete', 'unavailable', 'error'])
def test_catalog_failure_is_not_invented_product_coverage(state):
    repo = repository()
    repo.get_eligible_items.return_value = ReadResult(state, provenance='predicted')
    at = launch(repo)
    assert not at.exception
    if state == 'empty':
        assert at.metric[1].value == '0'
    else:
        assert not at.dataframe
        repo.get_store_trend.assert_not_called()


def test_invalid_product_and_store_are_rejected_without_history():
    repo = repository()
    at = launch(repo)
    repo.reset_mock()
    at.session_state['m09.item_id'] = 'unsupported'
    at.run()
    assert at.warning and not at.dataframe
    repo.get_store_trend.assert_not_called()
    repo.get_item_history.assert_not_called()
    at.session_state['m09.store_id'] = 'other'
    at.run()
    assert at.warning and not at.dataframe


def test_store_change_clears_item_and_rejects_wrong_item_identity():
    repo = repository()
    ctx = replace(context(), coverage=replace(context().coverage, available_stores=('S', 'T')))
    at = launch(repo, ctx)
    at.selectbox[1].set_value('I').run()
    repo.get_item_history.return_value = ReadResult('ok', daily('other'), 'observed')
    at.run()
    assert not at.dataframe
    at.selectbox[0].set_value('T').run()
    assert at.session_state['m09.item_id'] is None


def test_date_drafts_do_not_read_or_retain_sales_until_applied():
    repo = repository()
    at = launch(repo)
    repo.reset_mock()
    at.date_input[0].set_value((START, START + timedelta(days=1))).run()
    assert not at.dataframe
    repo.get_store_trend.assert_not_called()
    at.button[0].click().run()
    repo.get_store_trend.assert_called_once_with('S', START, START + timedelta(days=1))
    # The synthetic response still contains the old window: reject, never truncate it.
    assert not at.dataframe
    repo.reset_mock()
    at.date_input[0].set_value((START,)).run()
    assert not at.dataframe
    repo.get_store_trend.assert_not_called()


def test_date_validation_bounds_and_inclusive_limits():
    from app.overview import valid_window
    assert valid_window((START, END), START, END, 3)
    for dates in [(START,), (END, START), ('bad', END),
                  (START - timedelta(days=1), END), (START, END + timedelta(days=1))]:
        assert not valid_window(dates, START, END, 3)
    assert not valid_window((START, END), START, END, 2)


def test_default_window_uses_measured_bounds_and_stricter_limit():
    repo = repository()
    repo.limits = ReadLimits(max_rows=7, max_history_days=10)
    end = START + timedelta(days=500)
    repo.get_store_history_coverage.return_value = ReadResult('ok', (dict(
        store_id='S', start_date=START, end_date=end, day_count=501, row_count=501),), 'observed')
    at = launch(repo)
    assert not at.exception
    repo.get_store_trend.assert_called_once_with('S', end - timedelta(days=6), end)
    assert at.metric[2].value == '501'


@pytest.mark.parametrize('field', ['publication_id', 'manifest_checksum'])
def test_publication_change_clears_overview_date_state(field):
    at = launch()
    at.session_state['m09.overview.stale'] = 'stale'
    at.session_state['context'] = replace(context(), **{field: 'next'})
    at.run()
    assert 'm09.overview.stale' not in at.session_state
    assert not at.exception


def test_plot_uses_exact_chronological_numeric_rows():
    from app.overview import daily_figure, normalize_daily
    result = normalize_daily(ReadResult('ok', daily(), 'observed'), 'S', None, START, END)
    fig = daily_figure(result.rows)
    assert list(fig.data[0].y) == [0, 2, 3]
    assert list(fig.data[0].x) == [START, START + timedelta(days=1), END]
    assert sum(fig.data[0].y) == sum(row['units'] for row in result.rows) == 5


def test_missing_detail_binding_and_saturated_catalog_fail_closed():
    repo = repository()
    at = launch(repo)
    repo.get_item_history.return_value = ReadResult('unavailable', provenance='observed')
    at.selectbox[1].set_value('I').run()
    assert not at.dataframe and at.warning
    repo.get_eligible_items.return_value = ReadResult('ok', tuple(
        dict(store_id='S', item_id=f'I{i}') for i in range(50)), 'predicted')
    at.run()
    assert not at.dataframe and at.warning
