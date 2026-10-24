"""Synthetic approved Inventory presentation and fail-closed contracts."""
from datetime import date
from unittest.mock import Mock

import pytest
from streamlit.testing.v1 import AppTest

from tests.unit.test_application_read_repository import context
from warehouse_bigdata.application.read_repository import ReadResult
from warehouse_bigdata.application.session_state import PREFIX


INTEGER_FIELDS = ('planning_demand_units', 'opening_stock_units', 'safety_stock_units',
                  'target_stock_units', 'critical_need_units', 'safety_need_units', 'request_units')
LINEAGE = ('source_snapshot_id', 'forecast_run_id', 'model_run_id', 'scenario_id', 'decision_run_id')


def decision_row():
    ctx = context()
    return dict(item_id='I', store_id='S', issue_date=ctx.issue_date,
                **{key: getattr(ctx, key) for key in LINEAGE}, feature_version='features_v1',
                forecast_7d_units=1234.56789, planning_demand_units=1235,
                opening_stock_units=1000, safety_stock_units=100, target_stock_units=1335,
                critical_need_units=235, safety_need_units=100, request_units=335,
                forecast_shortage_flag=1, policy_version='replenishment_v1',
                origin_label='calculated_simulated_replenishment_recommendation')


def render_page():
    import streamlit as st
    from app.inventory import render_inventory
    from warehouse_bigdata.application.session_state import selections
    render_inventory(st.session_state['ctx'], st.session_state['repo'], selections(st.session_state))


def page(selected=True):
    repo = Mock()
    repo.get_eligible_items.return_value = ReadResult('ok', ({'item_id': 'I', 'store_id': 'S'},), 'predicted')
    repo.get_replenishment.return_value = ReadResult('ok', (decision_row(),), 'calculated')
    at = AppTest.from_function(render_page)
    at.session_state['ctx'] = context()
    at.session_state['repo'] = repo
    if selected:
        at.session_state[PREFIX + 'store_id'] = 'S'
        at.session_state[PREFIX + 'item_id'] = 'I'
    return at, repo


@pytest.mark.parametrize('field', LINEAGE + ('feature_version', 'issue_date', 'origin_label', 'policy_version'))
@pytest.mark.parametrize('kind', ['missing', 'mismatch'])
def test_invalid_lineage(field, kind):
    from app.inventory import normalize_replenishment
    row = decision_row()
    if kind == 'missing':
        row.pop(field)
    else:
        row[field] = date(2000, 1, 1) if field == 'issue_date' else 'wrong'
        if field == 'feature_version':
            row[field] = ''
    result = normalize_replenishment(ReadResult('ok', (row,), 'calculated'), context(), 'I', 'S')
    assert result.state == 'incomplete' and not result.rows


@pytest.mark.parametrize('field', INTEGER_FIELDS + ('forecast_7d_units',))
@pytest.mark.parametrize('value', [None, True, -1, float('nan'), float('inf'), '1'])
def test_invalid_quantities(field, value):
    from app.inventory import normalize_replenishment
    row = dict(decision_row(), **{field: value})
    assert normalize_replenishment(ReadResult('ok', (row,), 'calculated'), context(), 'I', 'S').state == 'incomplete'


@pytest.mark.parametrize('kind', ['absent', 'duplicate', 'provenance', 'item', 'store', 'flag', 'float_flag', 'fraction'])
def test_invalid_payload(kind):
    from app.inventory import normalize_replenishment
    row, provenance = decision_row(), 'calculated'
    rows = (row,)
    if kind == 'absent': rows = ()
    elif kind == 'duplicate': rows *= 2
    elif kind == 'provenance': provenance = 'predicted'
    elif kind in ('item', 'store'): row[kind + '_id'] = 'other'
    elif kind == 'flag': row['forecast_shortage_flag'] = 2
    elif kind == 'float_flag': row['forecast_shortage_flag'] = 1.0
    else: row['request_units'] = 1.5
    assert normalize_replenishment(ReadResult('ok', rows, provenance), context(), 'I', 'S').state == 'incomplete'


def test_precision_chart_details_and_identity():
    from app.inventory import inventory_figure
    row = decision_row()
    fig = inventory_figure(row)
    assert list(fig.data[0].y) == [1234.56789, 1000, 1335]
    assert fig.layout.barmode == 'group'
    at, repo = page()
    at.run()
    assert not at.exception and len(at.metric) == 6
    assert [m.value for m in at.metric] == ['1.234,6 đơn vị', '1.000 đơn vị', '100 đơn vị',
                                           '1.335 đơn vị', '335 đơn vị', 'Có nguy cơ']
    details = at.dataframe[0].value.iloc[0]
    assert details['Tổng dự báo 7 ngày (đơn vị)'] == row['forecast_7d_units']
    assert details['Yêu cầu bổ sung (đơn vị)'] == 335
    text = '\n'.join(el.value for el in [*at.caption, *at.info, *at.markdown])
    for value in ['scenario', '22/05/2016', '23/05/2016', '29/05/2016', 'approved',
                  'decision', 'forecast', 'replenishment_v1', row['origin_label'],
                  '1.234,6', '1.000', '100', '1.335', '335', 'đơn đặt mua', 'phân bổ']:
        assert value in text
    repo.get_replenishment.assert_called_once_with('I', 'S')


@pytest.mark.parametrize('kind', ['safety', 'zero', 'all_zero'])
def test_valid_decision_variants(kind):
    at, repo = page()
    row = decision_row()
    row['forecast_shortage_flag'] = False
    if kind in ('zero', 'all_zero'): row['request_units'] = 0
    if kind == 'all_zero':
        for key in INTEGER_FIELDS + ('forecast_7d_units',): row[key] = 0
    repo.get_replenishment.return_value = ReadResult('ok', (row,), 'calculated')
    at.run()
    assert not at.exception and len(at.metric) == 6 and at.dataframe and at.get('plotly_chart')
    assert at.metric[5].value == 'Không có nguy cơ'
    text = '\n'.join(el.value for el in [*at.info, *at.markdown])
    assert ('Không cần yêu cầu bổ sung theo kết quả đã công bố.' in text) == (kind != 'safety')
    if kind == 'safety': assert 'an toàn' in text


@pytest.mark.parametrize('state', ['empty', 'incomplete', 'unavailable', 'error', 'malformed'])
def test_failure_removes_success(state):
    at, repo = page()
    at.run()
    assert at.metric
    repo.get_replenishment.return_value = (ReadResult('ok', ({},), 'calculated') if state == 'malformed'
                                          else ReadResult(state, reason='secret diagnostics'))
    at.run()
    assert not at.exception and not at.metric and not at.dataframe and not at.get('plotly_chart')
    text = str([el.value for el in [*at.info, *at.warning, *at.error]])
    assert 'secret' not in text
    if state == 'empty': assert 'Chưa có khuyến nghị bổ sung đã công bố' in text


def test_selection_and_recovery():
    at, repo = page(False)
    at.run()
    repo.get_replenishment.assert_not_called()
    at.selectbox[1].set_value('I').run()
    assert at.metric
    at.session_state[PREFIX + 'store_id'] = 'unsupported'
    repo.get_replenishment.reset_mock()
    at.run()
    repo.get_replenishment.assert_not_called()
    at.selectbox[0].set_value('S').run()
    assert at.session_state[PREFIX + 'item_id'] is None and not at.metric
    at.session_state[PREFIX + 'item_id'] = 'unsupported'
    at.run()
    repo.get_replenishment.assert_not_called()
    at.selectbox[1].set_value('I').run()
    assert at.metric and not at.exception


@pytest.mark.parametrize('kind', ['empty', 'unavailable', 'error', 'incomplete', 'duplicate',
                                  'blank', 'store', 'saturated', 'provenance', 'inconsistent'])
def test_bad_catalog_blocks_reads(kind):
    at, repo = page()
    rows, state, provenance = [{'item_id': 'I', 'store_id': 'S'}], 'ok', 'predicted'
    if kind in ('empty', 'unavailable', 'error', 'incomplete'): rows, state = [], kind
    elif kind == 'duplicate': rows *= 2
    elif kind == 'blank': rows[0]['item_id'] = ''
    elif kind == 'store': rows[0]['store_id'] = 'other'
    elif kind == 'saturated': rows = [dict(item_id=str(i), store_id='S') for i in range(50)]
    elif kind == 'provenance': provenance = 'observed'
    else: state = 'empty'
    repo.get_eligible_items.return_value = ReadResult(state, tuple(rows), provenance, 'secret')
    at.run()
    assert not at.exception and not at.metric
    repo.get_replenishment.assert_not_called()
