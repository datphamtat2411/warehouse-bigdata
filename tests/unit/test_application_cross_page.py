"""Complete synthetic workflow through the real shell and page callbacks."""
from contextlib import contextmanager
from dataclasses import replace
from datetime import timedelta
from unittest.mock import Mock

import pytest
from streamlit.testing.v1 import AppTest

from tests.unit.test_application_shell import ENTRYPOINT
from tests.unit.test_application_distribution import fixture
from tests.unit.test_application_forecast import historical_context, metric_rows
from tests.unit.test_application_inventory import decision_row
from warehouse_bigdata.application import runtime
from warehouse_bigdata.application.read_repository import ReadLimits, ReadResult
from warehouse_bigdata.application.session_state import PREFIX


@pytest.fixture
def workflow(monkeypatch):
    ctx, supply, allocations = fixture()
    ctx = replace(ctx, dataset_bindings=historical_context().dataset_bindings)
    current = [ctx]
    repo = Mock(limits=ReadLimits())
    repo.get_eligible_items.side_effect = lambda store, *args, **kwargs: ReadResult(
        'ok', tuple(dict(item_id=i, store_id=store) for i in ('I', store)), 'predicted')
    start = ctx.issue_date - timedelta(days=6)
    repo.get_store_history_coverage.side_effect = lambda store: ReadResult('ok', (
        dict(store_id=store, start_date=start, end_date=ctx.issue_date, day_count=7, row_count=7),), 'observed')
    repo.get_item_history.side_effect = lambda item, store, *args: ReadResult('ok', tuple(
        dict(item_id=item, store_id=store, sale_date=start+timedelta(days=i), unit_sales=i+1)
        for i in range(7)), 'observed')
    repo.get_store_trend.side_effect = lambda store, *args: ReadResult('ok', tuple(
        dict(store_id=store, sale_date=start+timedelta(days=i), total_units=i+1)
        for i in range(7)), 'observed')
    repo.get_forecast_series.side_effect = lambda item, store: ReadResult('ok', tuple(
        dict(item_id=item, store_id=store, horizon=i, forecast_date=ctx.issue_date+timedelta(days=i),
             issue_date=ctx.issue_date, forecast_status='ok', predicted_units=float(i),
             source_snapshot_id=ctx.source_snapshot_id, model_run_id=ctx.model_run_id,
             forecast_run_id=ctx.forecast_run_id) for i in range(1, 8)), 'predicted')
    repo.get_forecast_metrics.return_value = ReadResult('ok', tuple(metric_rows()), 'evaluated')
    repo.get_replenishment.side_effect = lambda item, store: ReadResult('ok', (
        dict(decision_row(), item_id=item, store_id=store, forecast_7d_units=28.,
             planning_demand_units=28, opening_stock_units=25, safety_stock_units=2,
             target_stock_units=30, critical_need_units=3, safety_need_units=2, request_units=5),), 'calculated')
    repo.get_central_supply.return_value = ReadResult('ok', (supply,), 'simulated')
    repo.get_allocation.side_effect = lambda item, policy: ReadResult('ok', tuple(
        dict(r, policy_id=policy) for r in allocations), 'calculated')
    repo.get_policy_evaluation.return_value = ReadResult('unavailable', provenance='evaluated')
    monkeypatch.setattr(runtime, 'load_render_context', lambda: current[0])
    @contextmanager
    def opened(context):
        yield repo
    monkeypatch.setattr(runtime, 'open_render_repository', opened)
    return AppTest.from_file(ENTRYPOINT), repo, current


@pytest.mark.parametrize('page', ['Tổng quan', 'Dự báo nhu cầu', 'Tồn kho', 'Phân bổ hàng'])
def test_every_destination_first(workflow, page):
    at, repo, _ = workflow
    at.session_state[PREFIX+'navigation'] = page
    at.run()
    assert not at.exception and not at.error
    assert at.title[0].value == page
    if page != 'Tổng quan':
        assert not at.metric
        assert at.info


def test_complete_workflow_and_filter_reconciliation(workflow):
    at, repo, _ = workflow
    at.run()
    at.selectbox(key=PREFIX+'widget_item_id').select('I').run()
    assert at.metric[-1].value == '28'
    assert at.dataframe[0].value['Số đơn vị bán quan sát'].sum() == 28
    at.sidebar.radio[0].set_value('Dự báo nhu cầu').run()
    assert at.metric[0].value == '28,0'
    assert at.dataframe[0].value['Số đơn vị bán dự đoán'].sum() == 28
    at.session_state[PREFIX+'results.forecast'] = 'stale'
    at.selectbox(key=PREFIX+'forecast.store').select('B').run()
    assert at.session_state[PREFIX+'item_id'] == 'I'
    assert PREFIX+'results.forecast' not in at.session_state
    repo.get_forecast_series.assert_called_with('I', 'B')
    at.sidebar.radio[0].set_value('Tồn kho').run()
    assert at.metric[0].value == '28,0 đơn vị'
    assert at.metric[4].value == '5 đơn vị'
    at.sidebar.radio[0].set_value('Phân bổ hàng').run()
    assert [m.value for m in at.metric] == ['6 đơn vị', '10 đơn vị', '6 đơn vị', '4 đơn vị']
    assert len(at.dataframe[0].value) == 2
    at.selectbox(key=PREFIX+'widget_policy_id').select('equal_share').run()
    at.sidebar.radio[0].set_value('Dự báo nhu cầu').run()
    assert at.session_state[PREFIX+'policy_id'] == 'equal_share'
    at.selectbox(key=PREFIX+'widget_item_id').select('B').run()
    at.selectbox(key=PREFIX+'forecast.store').select('A').run()
    assert at.session_state[PREFIX+'item_id'] is None
    assert not at.metric and not at.dataframe and at.warning


def test_success_to_failure_and_catalog_failure_stop_reads(workflow):
    at, repo, _ = workflow
    at.run()
    at.session_state[PREFIX+'item_id'] = 'I'
    at.sidebar.radio[0].set_value('Dự báo nhu cầu').run()
    assert at.metric
    repo.get_forecast_series.side_effect = None
    repo.get_forecast_series.return_value = ReadResult('incomplete', provenance='predicted')
    at.run()
    assert not at.metric and not at.dataframe and not at.get('plotly_chart')
    repo.get_forecast_series.reset_mock()
    repo.get_eligible_items.side_effect = None
    repo.get_eligible_items.return_value = ReadResult('unavailable')
    at.run()
    repo.get_forecast_series.assert_not_called()
    assert at.session_state[PREFIX+'item_id'] == 'I'
    assert not at.metric and at.warning


def test_acceptance_gate_classification_and_numeric_reconciliation():
    from tests.integration.test_application_acceptance import enabled, classify, reconcile
    assert not enabled({})
    assert not enabled({'WAREHOUSE_VERIFY_APPLICATION': '1'})
    assert enabled({'WAREHOUSE_VERIFY_APPLICATION': '1', 'WAREHOUSE_VERIFY_APPROVED_DATA': '1'})
    assert classify(FileNotFoundError()) == 'BLOCKED'
    assert classify(ConnectionError()) == 'BLOCKED'
    assert classify(AssertionError()) == 'FAIL'
    assert classify(ValueError()) == 'FAIL'
    assert reconcile('numeric', 1., 1. + 1e-10)['status'] == 'PASS'
    assert reconcile('numeric', 1., 2.)['status'] == 'FAIL'
    assert reconcile('count', 7, 6, tolerance=0)['status'] == 'FAIL'


def test_mismatched_history_keeps_valid_primary_and_date_changes_invalidate(workflow):
    at, repo, _ = workflow
    at.run()
    at.session_state[PREFIX+'results.sales'] = 'stale'
    at.date_input[0].set_value((at.date_input[0].value[0], at.date_input[0].value[0])).run()
    assert PREFIX+'results.sales' not in at.session_state
    at.session_state[PREFIX+'item_id'] = 'I'
    repo.get_forecast_metrics.return_value = ReadResult('ok', tuple(
        dict(r, evaluation_run_id='wrong') for r in metric_rows()), 'evaluated')
    at.sidebar.radio[0].set_value('Dự báo nhu cầu').run()
    assert at.metric[0].value == '28,0'
    assert len(at.dataframe) == 1
    assert any('Chưa có kết quả đánh giá' in el.value for el in at.info)
