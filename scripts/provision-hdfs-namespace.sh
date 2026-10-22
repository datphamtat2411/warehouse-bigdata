#!/usr/bin/env bash
# Explicit shared-namespace setup/check through the authorized Hadoop owner.
set -euo pipefail

script_dir=${BASH_SOURCE[0]%/*}
[[ $script_dir == "${BASH_SOURCE[0]}" ]] && script_dir=.
repository=$(cd -- "$script_dir/.." && pwd)
# shellcheck disable=SC1091
source "$repository/scripts/load-env.sh"
if [[ $# != 1 || ! $1 =~ ^(setup|check)$ ]]; then
    printf 'Usage: bash scripts/provision-hdfs-namespace.sh setup|check\n' >&2
    exit 2
fi
exec timeout --foreground --kill-after=15s 240s \
    docker compose -f "$repository/infrastructure/compose.yaml" --profile spark-submit \
    run --rm --no-deps -T --pull never --user 1000:100 \
    --entrypoint /opt/warehouse-app/.venv/bin/python spark-submit \
    -m warehouse_bigdata.common.hdfs_namespace "$1"
