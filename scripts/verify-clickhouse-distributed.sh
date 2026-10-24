#!/usr/bin/env bash
set -euo pipefail
uv run --locked warehouse-bigdata verify-clickhouse-distributed "$@"
