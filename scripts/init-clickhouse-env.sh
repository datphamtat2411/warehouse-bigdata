#!/usr/bin/env bash
set +x
set -euo pipefail
umask 077
repository=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
python3 - "$repository" <<'PY'
import os
import re
import secrets
import shlex
import sys
from pathlib import Path

root = Path(sys.argv[1])
path = root / '.env'
content = path.read_text() if path.exists() else (root / '.env.example').read_text()
defaults = {
    'HOST': 'localhost', 'PORT': '8123', 'DATABASE': 'warehouse',
    'USER': 'warehouse_reader', 'PUBLISHER_USER': 'warehouse_publisher',
    'ADMIN_USER': 'warehouse_admin',
    'PASSWORD': None, 'PUBLISHER_PASSWORD': None, 'ADMIN_PASSWORD': None,
}
for suffix, default in defaults.items():
    key = 'WAREHOUSE_CLICKHOUSE_' + suffix
    pattern = re.compile(r'^(?:export\s+)?' + key + r'=(.*)$', re.MULTILINE)
    match = pattern.search(content)
    if match:
        parsed = shlex.split(match.group(1), comments=True)
        if parsed and parsed[0]:
            continue
    value = default if default is not None else secrets.token_hex(32)
    line = key + '=' + value
    if match:
        content = pattern.sub(lambda _: line, content)
    else:
        content = content.rstrip('\n') + '\n' + line + '\n'
os.chmod(path, 0o600) if path.exists() else None
with open(path, 'w', opener=lambda p, flags: os.open(p, flags, 0o600)) as stream:
    stream.write(content)
PY
printf 'Private ClickHouse environment initialized.\n'
