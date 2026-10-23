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
has_shuffle=0
has_aqe=0
while [[ $# -gt 0 && $1 != -- ]]; do
    if [[ $1 != --conf || $# -lt 2 || $2 != *=* ]]; then
        usage >&2
        exit 2
    fi
    [[ $2 == spark.sql.shuffle.partitions=* ]] && has_shuffle=1
    [[ $2 == spark.sql.adaptive.enabled=* ]] && has_aqe=1
    shift 2
done
if [[ $# -lt 2 || $1 != -- || -z $2 ]]; then
    usage >&2
    exit 2
fi
publication_run_id=
feature_job=0
recovery_binding=()
[[ $2 == warehouse_bigdata/jobs/build_forecast_features.py ]] && feature_job=1
case $2 in
    warehouse_bigdata/jobs/normalize_sales_daily.py|warehouse_bigdata/jobs/enrich_analytics.py|warehouse_bigdata/jobs/build_forecast_features.py)
        shift 2
        while [[ $# -gt 0 ]]; do
            case $1 in
                --pipeline-run-id)
                    [[ $# -ge 2 && -z $publication_run_id ]] || { usage >&2; exit 2; }
                    publication_run_id=$2
                    shift 2 ;;
                --pipeline-run-id=*)
                    [[ -z $publication_run_id ]] || { usage >&2; exit 2; }
                    publication_run_id=${1#*=}
                    shift ;;
                --source-version|--source-snapshot-id|--upstream-pipeline-run-id|--feature-version)
                    [[ $# -ge 2 ]] || { usage >&2; exit 2; }
                    if (( feature_job )); then
                        recovery_binding+=("$1" "$2")
                    fi
                    shift 2 ;;
                --source-version=*|--source-snapshot-id=*|--upstream-pipeline-run-id=*|--feature-version=*)
                    if (( feature_job )); then
                        recovery_binding+=("$1")
                    fi
                    shift ;;
                *) shift ;;
            esac
        done
        [[ $publication_run_id =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]] || {
            printf 'M04 publication requires one safe --pipeline-run-id.\n' >&2; exit 2;
        }
        ;;
esac
if (( feature_job )); then
    defaults=()
    (( has_shuffle )) || defaults+=(--conf spark.sql.shuffle.partitions=16)
    (( has_aqe )) || defaults+=(--conf spark.sql.adaptive.enabled=true)
    arguments=("${defaults[@]}" "${arguments[@]}")
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
for worker in spark-worker-1 spark-worker-2; do
    timeout --kill-after=2s 15 "${compose[@]}" exec -T --interactive=false "$worker" \
        /opt/python/bin/python3.11 /opt/warehouse-spark/scratch.py preflight </dev/null
done

token=$("$od_executable" -An -N16 -tx1 /dev/urandom)
token=${token//[[:space:]]/}
driver=warehouse-spark-submit-$token
owner_label=io.warehouse-bigdata.spark-submit.owner
run_pid=
cancelled=0

# shellcheck disable=SC2329 # Called by finish through the EXIT trap.
cleanup_driver() {
    local owner name=${1:-$driver}
    owner=$(timeout 5 docker inspect --type container --format \
        '{{ index .Config.Labels "io.warehouse-bigdata.spark-submit.owner" }}' "$name" 2>/dev/null) || return 1
    [[ $owner == "$token" ]] || return 0
    if (( result != 0 )); then
        timeout 5 docker logs --tail 100 --timestamps -- "$name" >&2 || true
    fi
    timeout 15 docker stop --timeout 10 "$name" >/dev/null 2>&1 || true
    timeout 5 docker rm --force "$name" >/dev/null 2>&1 || true
}

# shellcheck disable=SC2329 # Registered as the EXIT trap below.
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
    if [[ -n $publication_run_id ]]; then
        cleanup_driver "$driver-publish" || true
    fi
    exit "$result"
}
trap finish EXIT
trap 'cancelled=1; exit 130' INT
trap 'cancelled=1; exit 143' TERM

publish_m04() {
    timeout --foreground --kill-after=15s 240s "${compose[@]}" run --rm --no-deps -T --pull never \
        --name "$driver-publish" --label "$owner_label=$token" --user 1000:100 \
        --entrypoint /opt/warehouse-app/.venv/bin/python spark-submit \
        -m warehouse_bigdata.common.hdfs_publication "$@" --pipeline-run-id "$publication_run_id" &
    run_pid=$!
    wait "$run_pid"
}
# Recover a previously claimed candidate before Spark attempts to reconstruct staging.
if [[ -n $publication_run_id ]]; then
    if (( feature_job )); then
        if publish_m04 --skip-completed-driver "${recovery_binding[@]}" --recover; then
            :
        else
            recovery_status=$?
            if (( recovery_status == 10 )); then
                exit 0
            fi
            exit "$recovery_status"
        fi
    else
        publish_m04 --recover
    fi
fi

"${compose[@]}" run --rm --no-deps -T --pull never \
    --name "$driver" --label "$owner_label=$token" \
    --env "_WAREHOUSE_SPARK_DRIVER_HOST=$driver" \
    spark-submit "${arguments[@]}" &
run_pid=$!
if wait "$run_pid"; then
    if [[ -n $publication_run_id ]]; then
        publish_m04
    fi
    exit 0
else
    exit "$?"
fi
