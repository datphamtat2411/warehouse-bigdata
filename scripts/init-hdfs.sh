#!/usr/bin/env bash
# Explicit initialization only. Never call this from a daemon's startup path.
set -euo pipefail

script_dir=${BASH_SOURCE[0]%/*}
[[ $script_dir == "${BASH_SOURCE[0]}" ]] && script_dir=.
repository=$(cd -- "$script_dir/.." && pwd)
# shellcheck disable=SC1091
source "$repository/scripts/load-env.sh"
compose=(docker compose -f "$repository/infrastructure/compose.yaml" --profile init)
services=(hdfs-namenode hdfs-datanode-1 hdfs-datanode-2)

# Include paused/restarting containers: checking only "running" is not sufficient.
containers=$("${compose[@]}" ps --all --quiet "${services[@]}")
for container in $containers; do
    state=$(docker inspect --format '{{.State.Status}}' "$container")
    case $state in
        exited|created) ;;
        *) printf 'Initialization refused: HDFS container %s is %s; stop all daemons first.\n' \
            "$container" "$state" >&2; exit 1 ;;
    esac
done

"${compose[@]}" run --rm --no-deps hdfs-init init
