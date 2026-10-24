"""Verification must fail closed before issuing data queries."""
import pytest
from tests.unit.test_application_read_repository import Client, context
from warehouse_bigdata.jobs.verify_clickhouse_distributed import verify_distributed_publication
from types import SimpleNamespace


def test_verification_requires_authoritative_metadata_and_storage():
    with pytest.raises(ValueError):
        verify_distributed_publication(context(), Client(), {})


def test_verification_rejects_shared_storage_before_querying():
    client = Client()
    with pytest.raises(ValueError, match='shared'):
        verify_distributed_publication(context(), client, {
            'manifest': {'publication_id': 'approved'},
            'storage': {'clickhouse-1': 'same', 'clickhouse-2': 'same'}})
    assert not client.calls


def test_verification_rejects_missing_shard():
    class OneShard(Client):
        def query(self, sql, **kwargs):
            assert kwargs['settings']['skip_unavailable_shards'] == 0
            return SimpleNamespace(result_rows=[('26.8.1',)] if 'version()' in sql else [(1, 1, 1)])
    with pytest.raises(ValueError, match='two equal-weight'):
        verify_distributed_publication(context(), OneShard(), {
            'manifest': {'publication_id': 'approved'},
            'storage': {'clickhouse-1': 'one', 'clickhouse-2': 'two'}})
