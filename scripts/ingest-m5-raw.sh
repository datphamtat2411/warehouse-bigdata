#!/usr/bin/env bash
# The legacy raw-only launcher is retired in favor of the single host workflow.
set -euo pipefail

if [[ $# == 1 && $1 == --help ]]; then
    printf 'The raw-only launcher is retired. Use scripts/ingest-m5.sh, which owns the Hadoop CLI container lifecycle.\n'
    exit 0
fi

printf 'scripts/ingest-m5-raw.sh is retired; use scripts/ingest-m5.sh for the supported workflow.\n' >&2
printf 'The warehouse-bigdata ingest-m5-raw CLI remains available for focused tests or an explicitly configured client.\n' >&2
exit 2
