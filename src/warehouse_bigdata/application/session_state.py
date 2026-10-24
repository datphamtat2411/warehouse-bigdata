"""Small, publication-aware session state with no cached approval or resources."""

PREFIX = 'm09.'
FIELDS = ('store_id', 'item_id', 'policy_id')


def publication_identity(context):
    return context.publication_id, context.manifest_checksum


def sync_publication_state(state, context):
    """Invalidate owned data before widgets; preserve the navigation destination."""
    identity = publication_identity(context) if context is not None else None
    changed = state.get(PREFIX + 'identity') != identity
    if changed or context is None or PREFIX + 'identity' not in state:
        for key in list(state):
            if key.startswith(PREFIX) and key != PREFIX + 'navigation':
                del state[key]
        state[PREFIX + 'identity'] = identity
        for field in FIELDS:
            state[PREFIX + field] = None
        if context is not None and context.policy_ids:
            state[PREFIX + 'policy_id'] = context.policy_ids[0]
    return changed


def selections(state):
    """Return a detached snapshot of the selections for this render."""
    return {field: state.get(PREFIX + field) for field in FIELDS}


def set_selection(state, field, value):
    """Apply filter dependency resets; callers supply validated selection values."""
    if field not in FIELDS:
        raise ValueError('unknown selection field')
    if state.get(PREFIX + field) == value:
        return
    state[PREFIX + field] = value
    for key in list(state):
        if key.startswith(PREFIX + 'results'):
            del state[key]
    if field == 'store_id':
        state[PREFIX + 'item_id'] = None
        state.pop(PREFIX + 'widget_item_id', None)
