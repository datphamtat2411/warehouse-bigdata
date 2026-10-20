#!/usr/bin/env bash
# Allowlisted operations for the opt-in Spark infrastructure only.
set -euo pipefail

if [[ $# != 1 ]]; then
    printf 'Usage: bash scripts/spark-cluster.sh {up|stop|status|logs|check}\n' >&2
    exit 2
fi
case $1 in
    up|stop|status|logs|check) action=$1 ;;
    *) printf 'Unknown Spark operation: %s\n' "$1" >&2; exit 2 ;;
esac

repository=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
compose=(docker compose -f "$repository/infrastructure/compose.yaml" --profile spark)
services=(spark-master spark-worker-1 spark-worker-2)
probe=(exec -T --interactive=false spark-master /opt/python/bin/python3.11 /opt/warehouse-spark/healthcheck.py)

diagnostics() {
    timeout --kill-after=2s 12 "${compose[@]}" ps --all "${services[@]}" >&2 || true
    timeout --kill-after=2s 12 "${compose[@]}" logs --no-color --tail 100 "${services[@]}" >&2 || true
}
trap 'result=$?; trap - ERR; diagnostics; exit "$result"' ERR

case $action in
    up)
        memory=$(timeout 15 docker info --format '{{.MemTotal}}')
        if [[ ! $memory =~ ^[0-9]+$ ]] || (( memory < 10 * 1024 * 1024 * 1024 )); then
            printf 'Spark startup guard is the planned 10 GiB budget (not a measured minimum); reported Docker bytes: %s\n' \
                "${memory:-unknown}" >&2
            diagnostics
            exit 1
        fi
        printf 'Docker memory: %s MiB; HDFS + Spark limits: 7168 MiB.\n' "$((memory / 1024 / 1024))"
        # An existing, initialized HDFS deployment is a prerequisite, never a startup target.
        timeout --kill-after=2s 45 "${compose[@]}" exec -T --interactive=false hdfs-namenode bash \
            /opt/warehouse-hdfs/healthcheck.sh wait-ready 30 </dev/null
        timeout 600 "${compose[@]}" up --detach --build --no-deps "${services[@]}"
        timeout --kill-after=2s 195 "${compose[@]}" "${probe[@]}" wait-ready 180 </dev/null
        ;;
    stop) timeout 200 "${compose[@]}" stop --timeout 60 "${services[@]}" ;;
    status) timeout 15 "${compose[@]}" ps --all "${services[@]}" ;;
    logs) timeout 15 "${compose[@]}" logs --no-color --tail 100 "${services[@]}" ;;
    check) timeout --kill-after=2s 15 "${compose[@]}" "${probe[@]}" ready </dev/null ;;
esac
