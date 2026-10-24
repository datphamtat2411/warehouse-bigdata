"""Uncached approval loading and render-scoped read-only repository ownership."""
from contextlib import contextmanager

import clickhouse_connect

from warehouse_bigdata.application.read_repository import (
    ClickHouseReadRepository,
    ReadLimits,
    load_approved_publication_context,
)
from warehouse_bigdata.common.config import load_clickhouse_config, load_paths_config


def load_render_context():
    """Recheck authoritative HDFS approval and fencing on every render."""
    return load_approved_publication_context(
        load_paths_config().artifacts_dir / 'approved_publication.json')


@contextmanager
def open_render_repository(context):
    """Acquire one fresh reader and always release it, including control-flow exits."""
    config = load_clickhouse_config()
    limits = ReadLimits()
    client = clickhouse_connect.get_client(
        host=config.host, port=config.port, database=config.database,
        username=config.user, password=config.password,
        connect_timeout=10, send_receive_timeout=limits.timeout_seconds)
    try:
        yield ClickHouseReadRepository(context, client, limits)
    finally:
        client.close()
