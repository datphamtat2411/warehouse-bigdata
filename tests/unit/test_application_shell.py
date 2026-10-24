"""Streamlit shell tests patch only external I/O boundaries."""
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock

import pytest
from streamlit.testing.v1 import AppTest

from app import shell
from tests.unit.test_application_read_repository import context
from warehouse_bigdata.application import runtime
from warehouse_bigdata.application.session_state import PREFIX
from warehouse_bigdata.common.hdfs_client import HdfsCommandError

ENTRYPOINT = Path(__file__).resolve().parents[2] / 'app' / 'main.py'


@pytest.fixture
def harness(monkeypatch):
    ctx = context()
    bindings = dict(ctx.dataset_bindings)
    bindings['forecast_daily'] = dict(bindings['forecast_daily'], dataset_scope='demo_sample',
                                     selection_digest='synthetic-scope')
    current = [replace(ctx, dataset_bindings=bindings)]
    loader = Mock(side_effect=lambda: current[0])
    monkeypatch.setattr(runtime, 'load_render_context', loader)
    resources = []

    @contextmanager
    def repository(ctx):
        resource = Mock(context=ctx)
        resources.append(resource)
        try:
            yield resource
        finally:
            resource.close()

    opener = Mock(side_effect=repository)
    monkeypatch.setattr(runtime, 'open_render_repository', opener)
    return current, loader, opener, resources


def test_four_destinations_summary_navigation_refresh(harness):
    current, loader, opener, resources = harness
    at = AppTest.from_file(ENTRYPOINT).run()
    assert not at.exception
    assert at.sidebar.radio[0].options == ['Tổng quan', 'Dự báo nhu cầu', 'Tồn kho', 'Phân bổ hàng']
    assert len(at.radio) == 1
    at.session_state[PREFIX + 'store_id'] = 'S'
    at.session_state[PREFIX + 'item_id'] = 'I'
    for name in at.sidebar.radio[0].options:
        at.sidebar.radio[0].set_value(name).run()
        assert at.title[0].value == name
        assert at.session_state[PREFIX + 'item_id'] == 'I'
    text = '\n'.join(el.value for el in at.markdown)
    for expected in ['22/05/2016', 'approved', 'sha256:x', 'source', 'model', 'forecast',
                     'scenario', 'decision', 'demo_sample', 'synthetic-scope', '700', '194.100']:
        assert expected in text
    assert not at.metric and not at.dataframe
    before = loader.call_count
    at.button[0].click().run()
    assert loader.call_count == before + 1
    assert opener.call_count == loader.call_count
    for resource in resources:
        resource.close.assert_called_once()
        resource.query.assert_not_called()


def test_direct_first_navigation(harness):
    at = AppTest.from_file(ENTRYPOINT)
    at.session_state[PREFIX + 'navigation'] = 'Phân bổ hàng'
    at.run()
    assert not at.exception
    assert at.title[0].value == 'Phân bổ hàng'


def test_render_pins_context_then_next_rerun_adopts_change(harness, monkeypatch):
    current, loader, _, _ = harness
    original = current[0]
    seen = []

    def page(ctx, repository, selections):
        seen.append((ctx, repository.context, selections))
        current[0] = replace(original, publication_id='next')

    monkeypatch.setitem(shell.PAGES, 'Tổng quan', page)
    at = AppTest.from_file(ENTRYPOINT).run()
    assert seen[0][0] is original and seen[0][1] is original
    assert loader.call_count == 1
    at.session_state[PREFIX + 'results'] = 'stale'
    at.session_state[PREFIX + 'item_id'] = 'I'
    at.run()
    assert seen[1][0].publication_id == 'next'
    assert seen[1][2]['item_id'] is None
    assert PREFIX + 'results' not in at.session_state


def test_checksum_transition_and_recovery(harness):
    current, loader, _, _ = harness
    original = current[0]
    at = AppTest.from_file(ENTRYPOINT).run()
    at.session_state[PREFIX + 'item_id'] = 'I'
    at.session_state[PREFIX + 'results'] = 'stale'
    current[0] = replace(original, manifest_checksum='new-checksum')
    at.run()
    assert at.session_state[PREFIX + 'identity'] == ('approved', 'new-checksum')
    assert at.session_state[PREFIX + 'item_id'] is None
    assert PREFIX + 'results' not in at.session_state
    loader.side_effect = ValueError('revoked')
    at.run()
    assert at.error and at.session_state[PREFIX + 'identity'] is None
    loader.side_effect = lambda: current[0]
    at.run()
    assert not at.error and not at.exception
    assert at.session_state[PREFIX + 'policy_id'] == 'p'


@pytest.mark.parametrize('control', ['stop', 'rerun'])
def test_streamlit_control_flow_closes_resources(harness, monkeypatch, control):
    _, loader, _, resources = harness
    calls = []

    def page(ctx, repository, selections):
        import streamlit as st
        calls.append(ctx)
        if len(calls) == 1:
            if control == 'stop':
                st.stop()
            st.rerun()

    monkeypatch.setitem(shell.PAGES, 'Tổng quan', page)
    at = AppTest.from_file(ENTRYPOINT).run()
    assert not at.exception
    assert loader.call_count == (1 if control == 'stop' else 2)
    for resource in resources:
        resource.close.assert_called_once()


@pytest.mark.parametrize('error', [FileNotFoundError('secret'), ValueError('incomplete secret'),
    ValueError('publication is fenced unavailable'), HdfsCommandError('secret host'),
    RuntimeError('secret')])
def test_validation_failure_stops_client_page_and_stale_state(harness, monkeypatch, error, caplog):
    _, loader, opener, _ = harness
    page = Mock()
    monkeypatch.setitem(shell.PAGES, 'Tổng quan', page)
    at = AppTest.from_file(ENTRYPOINT).run()
    at.session_state[PREFIX + 'results'] = 'stale'
    at.session_state[PREFIX + 'item_id'] = 'I'
    loader.side_effect = error
    opener.reset_mock()
    page.reset_mock()
    at.run()
    assert not at.exception and at.error
    assert 'secret' not in at.error[0].value
    assert 'secret' not in caplog.text
    opener.assert_not_called()
    page.assert_not_called()
    assert PREFIX + 'results' not in at.session_state
    assert at.session_state[PREFIX + 'identity'] is None


def test_client_and_page_failure_are_safe_and_cleanup(harness, monkeypatch):
    _, _, opener, resources = harness
    opener.side_effect = RuntimeError('secret credentials')
    at = AppTest.from_file(ENTRYPOINT).run()
    assert not at.exception and at.error
    assert 'secret' not in at.error[0].value
    opener.side_effect = None
    # Restore a scoped resource for the page exception path.
    @contextmanager
    def scoped(ctx):
        client = Mock(context=ctx)
        resources.append(client)
        try:
            yield client
        finally:
            client.close()
    opener.side_effect = scoped
    monkeypatch.setitem(shell.PAGES, 'Tổng quan', Mock(side_effect=RuntimeError('secret page')))
    at.run()
    assert not at.exception and at.error
    resources[-1].close.assert_called_once()


@pytest.mark.parametrize('state,ready', [('ok', True), ('empty', False), ('incomplete', False),
                                       ('unavailable', False), ('error', False)])
def test_read_state_presenter(state, ready):
    def render():
        import streamlit as st
        from app.views import render_read_state
        from warehouse_bigdata.application.read_repository import ReadResult
        st.session_state['ready'] = render_read_state(ReadResult(
            st.session_state['test_state'], reason='secret technical details'))
    at = AppTest.from_function(render)
    at.session_state['test_state'] = state
    at.run()
    assert not at.exception
    assert at.session_state['ready'] is ready
    messages = list(at.info) + list(at.warning) + list(at.error)
    assert bool(messages) is not ready
    assert all('secret' not in el.value and el.value != '0' for el in messages)
