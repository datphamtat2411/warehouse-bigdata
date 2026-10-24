#!/usr/bin/env bash
set +x
set -euo pipefail
[[ $# == 1 ]] || { printf 'Usage: clickhouse-cluster.sh up|stop|status|logs|check\n' >&2; exit 2; }
case $1 in up|stop|status|logs|check) ;; *) exit 2 ;; esac
repository=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
source "$repository/scripts/load-env.sh"
for suffix in USER PASSWORD PUBLISHER_USER PUBLISHER_PASSWORD ADMIN_USER ADMIN_PASSWORD; do
    key=WAREHOUSE_CLICKHOUSE_$suffix
    [[ -n ${!key:-} ]] || { printf 'Run scripts/init-clickhouse-env.sh first.\n' >&2; exit 1; }
    export "$key"
done
compose=(docker compose --env-file "$repository/.env" -f "$repository/infrastructure/compose.yaml" --profile clickhouse)
services=(clickhouse-1 clickhouse-2)
case $1 in
    up) timeout 240 "${compose[@]}" up -d --no-deps --wait --wait-timeout 180 "${services[@]}" ;;
    stop) timeout 150 "${compose[@]}" stop --timeout 60 "${services[@]}" ;;
    status) timeout 30 "${compose[@]}" ps "${services[@]}" ;;
    logs) timeout 30 "${compose[@]}" logs --tail 100 "${services[@]}" ;;
    check)
        versions=()
        for service in "${services[@]}"; do
            timeout 30 "${compose[@]}" exec -T "$service" bash /opt/warehouse-clickhouse/healthcheck.sh ready
            versions+=("$(timeout 30 "${compose[@]}" exec -T "$service" bash /opt/warehouse-clickhouse/healthcheck.sh topology)")
        done
        [[ ${versions[0]} == 26.8.* && ${versions[0]} == "${versions[1]}" ]]
        printf 'Both nodes passed SQL readiness, exact topology, distributed reachability and matching %s version.\n' "${versions[0]}"
        ;;
esac
