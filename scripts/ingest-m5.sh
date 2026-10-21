#!/usr/bin/env bash
# Run the combined M5 workflow in the prepared network-attached Hadoop client image.
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
image=warehouse-bigdata-spark-submit:3.5.7-python3.11.17
compose=(docker compose -f "$repository/infrastructure/compose.yaml" --profile spark-submit)
artifacts_dir=${WAREHOUSE_ARTIFACTS_DIR:-$repository/artifacts}

for executable in docker realpath; do
    if ! command -v "$executable" >/dev/null; then
        printf 'M5 ingestion requires executable: %s\n' "$executable" >&2
        exit 127
    fi
done

source_dir=$(realpath -e -- "$source_dir")
if [[ ! -d $source_dir ]]; then
    printf 'M5 source directory does not exist: %s\n' "$source_dir" >&2
    exit 2
fi
mkdir -p -- "$artifacts_dir"
artifacts_dir=$(realpath -e -- "$artifacts_dir")

if ! docker image inspect "$image" >/dev/null 2>&1; then
    printf 'Prepare the driver image explicitly: docker compose -f "%s/infrastructure/compose.yaml" --profile spark-submit build spark-submit\n' \
        "$repository" >&2
    exit 125
fi

arguments=(
    --source-dir /opt/warehouse-input/source
    --source-url "$source_url"
)
if [[ $downloaded_at_utc_provided == 1 ]]; then
    arguments+=(--downloaded-at-utc "$downloaded_at_utc")
fi
if [[ -n $pipeline_run_id ]]; then
    arguments+=(--pipeline-run-id "$pipeline_run_id")
fi

environment_args=(--env WAREHOUSE_ARTIFACTS_DIR=/opt/warehouse-artifacts)
for variable in WAREHOUSE_HDFS_URI WAREHOUSE_HDFS_BASE_PATH WAREHOUSE_SPARK_MASTER_URL; do
    if [[ -n ${!variable:-} ]]; then
        environment_args+=(--env "$variable=${!variable}")
    fi
done

# --no-deps starts only this short-lived client; it never starts or changes HDFS/Spark services.
exec "${compose[@]}" run --rm --no-deps -T --pull never \
    --entrypoint /usr/bin/tini \
    --user "$(id -u):$(id -g)" \
    "${environment_args[@]}" \
    --mount "type=bind,source=$source_dir,target=/opt/warehouse-input/source,readonly" \
    --mount "type=bind,source=$artifacts_dir,target=/opt/warehouse-artifacts" \
    spark-submit -g -- /opt/warehouse-app/.venv/bin/warehouse-bigdata ingest-m5 "${arguments[@]}"
