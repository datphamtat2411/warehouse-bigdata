"""Publication-aware state uses tiny synthetic contexts."""
from dataclasses import replace

import pytest

from tests.unit.test_application_read_repository import context
from warehouse_bigdata.application.session_state import (
    PREFIX, publication_identity, selections, set_selection, sync_publication_state,
)


def test_initial_and_unchanged_identity_preserve_navigation_selections():
    state = {'unrelated': 7}
    ctx = context()
    assert sync_publication_state(state, ctx)
    assert state[PREFIX + 'identity'] == publication_identity(ctx)
    assert selections(state) == dict(store_id=None, item_id=None, policy_id='p')
    set_selection(state, 'store_id', 'S')
    set_selection(state, 'item_id', 'I')
    state[PREFIX + 'results'] = {'old': 1}
    assert not sync_publication_state(state, ctx)
    assert selections(state)['item_id'] == 'I'
    assert state[PREFIX + 'results'] == {'old': 1}
    assert state['unrelated'] == 7


@pytest.mark.parametrize('change', [dict(publication_id='next'), dict(manifest_checksum='new')])
def test_identity_change_resets_owned_state_only(change):
    state = {'other': 'kept', PREFIX + 'navigation': 'Tồn kho'}
    sync_publication_state(state, context())
    set_selection(state, 'store_id', 'S')
    set_selection(state, 'item_id', 'I')
    state[PREFIX + 'widget_item_id'] = 'I'
    state[PREFIX + 'results'] = {'stale': 1}
    assert sync_publication_state(state, replace(context(), **change))
    assert selections(state) == dict(store_id=None, item_id=None, policy_id='p')
    assert PREFIX + 'results' not in state
    assert PREFIX + 'widget_item_id' not in state
    assert state['other'] == 'kept'
    assert state[PREFIX + 'navigation'] == 'Tồn kho'


def test_failure_clears_and_recovery_initializes_approved_policy():
    state = {}
    sync_publication_state(state, context())
    state[PREFIX + 'results'] = 'stale'
    sync_publication_state(state, None)
    assert selections(state) == dict(store_id=None, item_id=None, policy_id=None)
    assert PREFIX + 'results' not in state
    assert state[PREFIX + 'identity'] is None
    assert not sync_publication_state(state, None)
    sync_publication_state(state, replace(context(), policy_ids=('approved-policy',)))
    assert selections(state)['policy_id'] == 'approved-policy'


def test_selection_dependencies_and_noop():
    state = {}
    sync_publication_state(state, context())
    set_selection(state, 'store_id', 'S')
    set_selection(state, 'item_id', 'I')
    state[PREFIX + 'results'] = 1
    set_selection(state, 'item_id', 'I')
    assert state[PREFIX + 'results'] == 1
    set_selection(state, 'policy_id', 'p2')
    assert PREFIX + 'results' not in state
    assert selections(state)['item_id'] == 'I'
    state[PREFIX + 'widget_item_id'] = 'I'
    set_selection(state, 'store_id', 'S2')
    assert selections(state)['item_id'] is None
    assert PREFIX + 'widget_item_id' not in state
    state[PREFIX + 'results'] = 2
    set_selection(state, 'item_id', 'I2')
    assert PREFIX + 'results' not in state
    with pytest.raises(ValueError):
        set_selection(state, 'unknown', 'x')
