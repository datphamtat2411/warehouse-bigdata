#!/usr/bin/env bash
set +x
set -euo pipefail
[[ $# == 1 ]] && [[ $1 == apply || $1 == check ]] || {
    printf 'Usage: clickhouse-schema.sh apply|check\n' >&2
    exit 2
}
repository=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
source "$repository/scripts/load-env.sh"
for suffix in ADMIN_USER ADMIN_PASSWORD; do
    key=WAREHOUSE_CLICKHOUSE_$suffix
    [[ -n ${!key:-} ]] || { printf 'Run scripts/init-clickhouse-env.sh first.\n' >&2; exit 1; }
done
bash "$repository/scripts/clickhouse-cluster.sh" check
python3 - "$repository" "$1" <<'PY'
import json
import re
import subprocess
import sys
from pathlib import Path

root = Path(sys.argv[1])
apply = sys.argv[2] == 'apply'
compose = ['docker', 'compose', '--env-file', str(root / '.env'), '-f',
           str(root / 'infrastructure/compose.yaml'), '--profile', 'clickhouse']


def query(node, sql, metadata=False):
    result = subprocess.run(
        compose + ['exec', '-T', node, 'bash', '-c',
                   'clickhouse-client --user "$CLICKHOUSE_USER" --connect_timeout 3 '
                   '--receive_timeout 30 --send_timeout 30 --multiquery'],
        input=sql, text=True, capture_output=True, timeout=60,
    )
    if result.returncode:
        raise RuntimeError(f'{node}: SQL failed: {result.stderr.strip()}')
    return json.loads(result.stdout)['data'] if metadata else result.stdout


def canonical(value):
    return re.sub(r"[\s`']", '', value)


local = (root / 'sql/clickhouse/001_warehouse_local.sql').read_text()
distributed = (root / 'sql/clickhouse/002_warehouse_distributed.sql').read_text()
schemas = {}
for name, body, ordering in re.findall(
    r'CREATE TABLE IF NOT EXISTS warehouse\.(\w+)\s*\((.*?)\)\s*'
    r'ENGINE = MergeTree\(\)\s*ORDER BY \((.*?)\);', local, re.S,
):
    schemas[name] = (dict(line.strip().rstrip(',').split(' ', 1)
                          for line in body.strip().splitlines()), ordering)
targets = re.findall(
    r'CREATE TABLE IF NOT EXISTS warehouse\.(\w+) AS warehouse\.(\w+)\s*'
    r'ENGINE = (Distributed\(.*?\));', distributed, re.S,
)
if len(schemas) != 9 or len(targets) != 9:
    raise RuntimeError('Expected exactly nine versioned serving schemas')


def validate(node, name, fields, engine, ordering):
    columns = query(node, "SELECT name, type, default_kind FROM system.columns "
                    f"WHERE database = 'warehouse' AND table = '{name}' ORDER BY position FORMAT JSON", True)
    actual = {row['name']: row['type'] for row in columns}
    if actual != fields or any(row['default_kind'] for row in columns):
        raise RuntimeError(f'{node}: incompatible columns for warehouse.{name}: {actual}')
    rows = query(node, "SELECT engine, engine_full, sorting_key, partition_key, sampling_key FROM system.tables "
                 f"WHERE database = 'warehouse' AND name = '{name}' FORMAT JSON", True)
    actual_engine = rows[0]['engine'] if rows and engine == 'MergeTree' else rows[0]['engine_full'] if rows else ''
    if len(rows) != 1 or canonical(actual_engine) != canonical(engine) or canonical(rows[0]['sorting_key']) != canonical(ordering) or rows[0]['partition_key'] or rows[0]['sampling_key']:
        raise RuntimeError(f'{node}: incompatible engine/order for warehouse.{name}: {rows}')


try:
    for node in ('clickhouse-1', 'clickhouse-2'):
        if apply:
            query(node, local)
        for name, (fields, ordering) in schemas.items():
            validate(node, name, fields, 'MergeTree', ordering)
    if apply:
        query('clickhouse-1', distributed)
    for name, target, engine in targets:
        validate('clickhouse-1', name, schemas[target][0], engine, '')
    print('Validated nine local schemas on both shards and nine coordinator Distributed tables.')
except (RuntimeError, subprocess.TimeoutExpired) as error:
    print(str(error), file=sys.stderr)
    sys.exit(1)
PY
