#!/usr/bin/env bash
# Explicit initialization only. Never call this from a daemon's startup path.
set -euo pipefail

repository=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
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
