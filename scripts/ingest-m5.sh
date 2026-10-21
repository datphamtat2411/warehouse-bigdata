#!/usr/bin/env bash
# Run the host-side M5 workflow through one owned Hadoop CLI container.
set -euo pipefail

usage() {
    printf 'Usage: bash scripts/ingest-m5.sh --source-dir PATH --source-url URL [--downloaded-at-utc TIMESTAMP] [--pipeline-run-id ID]\n'
}

if [[ $# == 1 && $1 == --help ]]; then
    usage
    exit 0
fi

source_dir=
source_url=
downloaded_at_utc=
downloaded_at_utc_provided=0
pipeline_run_id=
while [[ $# -gt 0 ]]; do
    case "$1" in
        --source-dir|--source-url|--downloaded-at-utc|--pipeline-run-id)
            [[ $# -ge 2 ]] || { usage >&2; exit 2; }
            case "$1" in
                --source-dir) source_dir=$2 ;;
                --source-url) source_url=$2 ;;
                --downloaded-at-utc)
                    downloaded_at_utc=$2
                    downloaded_at_utc_provided=1
                    ;;
                --pipeline-run-id) pipeline_run_id=$2 ;;
            esac
            shift 2
            ;;
        *)
            usage >&2
            exit 2
            ;;
    esac
done

if [[ -z $source_dir || -z $source_url || $downloaded_at_utc_provided == 1 && -z $downloaded_at_utc ]]; then
    usage >&2
    exit 2
fi

repository=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
image=apache/hadoop:3.4.3@sha256:127774dadab40ce84df7ac668a7a8c99945688b3fe336f1388f4477ca33e1529
network=warehouse-bigdata
core_config=$repository/infrastructure/hadoop/core-site.xml
hdfs_config=$repository/infrastructure/hadoop/hdfs-site.xml
container_user=1000:100
owner_label=io.warehouse-bigdata.hdfs-cli.owner

for executable in docker realpath uv timeout mktemp; do
    if ! command -v "$executable" >/dev/null 2>&1; then
        printf 'M5 ingestion requires executable: %s\n' "$executable" >&2
        exit 127
    fi
done
od_executable=/usr/bin/od
if [[ ! -x $od_executable ]]; then
    printf 'M5 ingestion requires executable: %s\n' "$od_executable" >&2
    exit 127
fi

if ! timeout --kill-after=2s 15 docker info >/dev/null 2>&1; then
    printf 'M5 ingestion requires a reachable Docker daemon.\n' >&2
    exit 125
fi
if ! timeout --kill-after=2s 15 docker image inspect "$image" >/dev/null 2>&1; then
    printf 'Prepare the pinned Hadoop client image before ingestion: %s\n' "$image" >&2
    exit 125
fi
if ! timeout --kill-after=2s 15 docker network inspect "$network" >/dev/null 2>&1; then
    printf 'The existing Docker bridge network is absent: %s\n' "$network" >&2
    exit 125
fi
for configuration in "$core_config" "$hdfs_config"; do
    if [[ ! -r $configuration ]]; then
        printf 'Hadoop client configuration is not readable: %s\n' "$configuration" >&2
        exit 2
    fi
done
if [[ -z ${WAREHOUSE_HDFS_URI:-} ]]; then
    printf 'M5 ingestion requires WAREHOUSE_HDFS_URI matching the mounted Hadoop XML.\n' >&2
    exit 2
fi

if ! source_dir=$(realpath -e -- "$source_dir"); then
    printf 'M5 source directory does not exist: %s\n' "$source_dir" >&2
    exit 2
fi
if [[ ! -d $source_dir ]]; then
    printf 'M5 source path is not a directory: %s\n' "$source_dir" >&2
    exit 2
fi

artifacts_dir=${WAREHOUSE_ARTIFACTS_DIR:-$repository/artifacts}
scratch_dir=${WAREHOUSE_SCRATCH_DIR:-$repository/artifacts/scratch}
artifacts_dir=$(realpath -m -- "$artifacts_dir")
scratch_dir=$(realpath -m -- "$scratch_dir")
if ! mkdir -p -- "$artifacts_dir" "$scratch_dir"; then
    printf 'Cannot create local artifact or scratch directory.\n' >&2
    exit 2
fi
if [[ ! -d $scratch_dir || ! -w $scratch_dir || ! -x $scratch_dir ]]; then
    printf 'Scratch directory must be a writable local directory: %s\n' "$scratch_dir" >&2
    exit 2
fi
scratch_probe=$(mktemp "$scratch_dir/.warehouse-m5-launcher.XXXXXX")
rm -f -- "$scratch_probe"

token=$($od_executable -An -N16 -tx1 /dev/urandom)
token=${token//[[:space:]]/}
client=warehouse-hdfs-cli-$token
client_attempted=0
result=0

cleanup_client() {
    local ownership
    ownership=$(timeout --kill-after=2s 10 docker inspect --type container --format \
        '{{.Name}}|{{ index .Config.Labels "io.warehouse-bigdata.hdfs-cli.owner" }}' \
        "$client" 2>/dev/null) || return 0
    if [[ $ownership == "/$client|$token" ]]; then
        timeout --kill-after=2s 20 docker rm --force "$client" >/dev/null 2>&1 || {
            printf 'Cannot clean up owned Hadoop CLI container: %s\n' "$client" >&2
            return 1
        }
    fi
    return 0
}

finish() {
    result=$?
    trap - EXIT
    trap '' INT TERM
    if (( client_attempted )) && ! cleanup_client; then
        (( result == 0 )) && result=1
    fi
    exit "$result"
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

client_attempted=1
if ! timeout --kill-after=5s 30 docker create \
    --name "$client" \
    --label "$owner_label=$token" \
    --platform linux/amd64 \
    --network "$network" \
    --user "$container_user" \
    --memory 512m \
    --cpus 1 \
    --env HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    --env HADOOP_HEAPSIZE_MIN=128 \
    --env HADOOP_HEAPSIZE_MAX=256 \
    --env TZ=UTC \
    --health-cmd 'hdfs version >/dev/null 2>&1' \
    --health-interval 10s \
    --health-timeout 5s \
    --health-retries 3 \
    --entrypoint /bin/bash \
    --mount "type=bind,source=$core_config,target=/opt/hadoop/etc/hadoop/core-site.xml,readonly" \
    --mount "type=bind,source=$hdfs_config,target=/opt/hadoop/etc/hadoop/hdfs-site.xml,readonly" \
    "$image" -c 'exec sleep infinity' >/dev/null; then
    printf 'Cannot create the isolated Hadoop CLI container: %s\n' "$client" >&2
    exit 125
fi
if ! timeout --kill-after=5s 30 docker start "$client" >/dev/null; then
    printf 'Hadoop CLI container did not start: %s\n' "$client" >&2
    exit 125
fi

export WAREHOUSE_HDFS_CLI_CONTAINER=$client
export WAREHOUSE_HDFS_CLI_OWNER=$token
export WAREHOUSE_ARTIFACTS_DIR=$artifacts_dir
export WAREHOUSE_SCRATCH_DIR=$scratch_dir
export WAREHOUSE_HDFS_BASE_PATH=${WAREHOUSE_HDFS_BASE_PATH:-/data/warehouse}

arguments=(
    --source-dir "$source_dir"
    --source-url "$source_url"
)
if [[ $downloaded_at_utc_provided == 1 ]]; then
    arguments+=(--downloaded-at-utc "$downloaded_at_utc")
fi
if [[ -n $pipeline_run_id ]]; then
    arguments+=(--pipeline-run-id "$pipeline_run_id")
fi

cd -- "$repository"
uv run --locked warehouse-bigdata ingest-m5 "${arguments[@]}"
