#!/usr/bin/env bash
# Submit one prepared driver; cluster lifecycle remains owned by T01/M01.
set -euo pipefail

script_dir=${BASH_SOURCE[0]%/*}
[[ $script_dir == "${BASH_SOURCE[0]}" ]] && script_dir=.
repository=$(cd -- "$script_dir/.." && pwd)
# shellcheck disable=SC1091
source "$repository/scripts/load-env.sh"

usage() {
    printf 'Usage: bash scripts/spark-submit.sh [--conf KEY=VALUE ...] -- APPLICATION.py [APPLICATION_ARGS...]\n'
}
if [[ $# == 1 && $1 == --help ]]; then
    usage
    exit 0
fi
arguments=("$@")
while [[ $# -gt 0 && $1 != -- ]]; do
    if [[ $1 != --conf || $# -lt 2 || $2 != *=* ]]; then
        usage >&2
        exit 2
    fi
    shift 2
done
if [[ $# -lt 2 || $1 != -- || -z $2 ]]; then
    usage >&2
    exit 2
fi

image=warehouse-bigdata-spark-submit:3.5.7-python3.11.17
compose=(docker compose -f "$repository/infrastructure/compose.yaml" --profile spark-submit)
for executable in docker timeout; do
    if ! command -v "$executable" >/dev/null; then
        printf 'Spark submission requires executable: %s\n' "$executable" >&2
        exit 127
    fi
done
od_executable=/usr/bin/od
if [[ ! -x $od_executable ]]; then
    printf 'Spark submission requires executable: %s\n' "$od_executable" >&2
    exit 127
fi
if timeout 15 docker image inspect "$image" >/dev/null; then
    :
else
    result=$?
    printf 'Prepare the driver image explicitly: docker compose -f "%s/infrastructure/compose.yaml" --profile spark-submit build spark-submit\n' \
        "$repository" >&2
    exit "$result"
fi
bash "$repository/scripts/spark-cluster.sh" check

token=$("$od_executable" -An -N16 -tx1 /dev/urandom)
token=${token//[[:space:]]/}
driver=warehouse-spark-submit-$token
owner_label=io.warehouse-bigdata.spark-submit.owner
run_pid=
cancelled=0

cleanup_driver() {
    local owner
    owner=$(timeout 5 docker inspect --type container --format \
        '{{ index .Config.Labels "io.warehouse-bigdata.spark-submit.owner" }}' "$driver" 2>/dev/null) || return 1
    [[ $owner == "$token" ]] || return 0
    if (( result != 0 )); then
        timeout 5 docker logs --tail 100 --timestamps -- "$driver" >&2 || true
    fi
    timeout 15 docker stop --timeout 10 "$driver" >/dev/null 2>&1 || true
    timeout 5 docker rm --force "$driver" >/dev/null 2>&1 || true
}

finish() {
    result=$?
    trap - EXIT
    trap '' INT TERM
    # Stop the attached client within a finite bound, including startup cancellation.
    if [[ -n $run_pid ]] && kill -0 "$run_pid" 2>/dev/null; then
        kill -TERM "$run_pid" 2>/dev/null || true
        for ((attempt = 0; attempt < 20; attempt++)); do
            kill -0 "$run_pid" 2>/dev/null || break
            sleep 0.1
        done
        kill -KILL "$run_pid" 2>/dev/null || true
        wait "$run_pid" 2>/dev/null || true
    fi
    # A create request can finish just after the client is interrupted. Recheck only
    # this invocation's name/token; never list or clean shared services or aliases.
    for ((attempt = 0; attempt < 5; attempt++)); do
        if cleanup_driver; then
            break
        fi
        (( cancelled )) || break
        sleep 0.2
    done
    exit "$result"
}
trap finish EXIT
trap 'cancelled=1; exit 130' INT
trap 'cancelled=1; exit 143' TERM

"${compose[@]}" run --rm --no-deps -T --pull never \
    --name "$driver" --label "$owner_label=$token" \
    --env "_WAREHOUSE_SPARK_DRIVER_HOST=$driver" \
    spark-submit "${arguments[@]}" &
run_pid=$!
if wait "$run_pid"; then
    exit 0
else
    exit "$?"
fi
