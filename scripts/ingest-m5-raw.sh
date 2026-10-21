#!/usr/bin/env bash
# Run raw ingestion in the prepared network-attached Hadoop client image.
set -euo pipefail

usage() {
    printf 'Usage: bash scripts/ingest-m5-raw.sh --source-dir PATH --profile-path PATH --manifest-path PATH --pipeline-run-id ID\n'
}

if [[ $# == 1 && $1 == --help ]]; then
    usage
    exit 0
fi

source_dir=
profile_path=
manifest_path=
pipeline_run_id=
while [[ $# -gt 0 ]]; do
    case "$1" in
        --source-dir)
            [[ $# -ge 2 ]] || { usage >&2; exit 2; }
            source_dir=$2
            shift 2
            ;;
        --profile-path)
            [[ $# -ge 2 ]] || { usage >&2; exit 2; }
            profile_path=$2
            shift 2
            ;;
        --manifest-path)
            [[ $# -ge 2 ]] || { usage >&2; exit 2; }
            manifest_path=$2
            shift 2
            ;;
        --pipeline-run-id)
            [[ $# -ge 2 ]] || { usage >&2; exit 2; }
            pipeline_run_id=$2
            shift 2
            ;;
        *)
            usage >&2
            exit 2
            ;;
    esac
done

if [[ -z $source_dir || -z $profile_path || -z $manifest_path || -z $pipeline_run_id ]]; then
    usage >&2
    exit 2
fi

repository=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
image=warehouse-bigdata-spark-submit:3.5.7-python3.11.17
compose=(docker compose -f "$repository/infrastructure/compose.yaml" --profile spark-submit)

for executable in docker realpath; do
    if ! command -v "$executable" >/dev/null; then
        printf 'Raw ingestion requires executable: %s\n' "$executable" >&2
        exit 127
    fi
done

source_dir=$(realpath -e -- "$source_dir")
profile_path=$(realpath -e -- "$profile_path")
manifest_path=$(realpath -e -- "$manifest_path")
if [[ ! -d $source_dir || ! -f $profile_path || ! -f $manifest_path ]]; then
    printf 'Raw ingestion inputs must be an existing directory and two existing files.\n' >&2
    exit 2
fi

if ! docker image inspect "$image" >/dev/null 2>&1; then
    printf 'Prepare the driver image explicitly: docker compose -f "%s/infrastructure/compose.yaml" --profile spark-submit build spark-submit\n' \
        "$repository" >&2
    exit 125
fi

# --no-deps starts only this short-lived client; it never starts or changes HDFS services.
exec "${compose[@]}" run --rm --no-deps -T --pull never \
    --entrypoint /usr/bin/tini \
    --mount "type=bind,source=$source_dir,target=/opt/warehouse-input/source,readonly" \
    --mount "type=bind,source=$profile_path,target=/opt/warehouse-input/source_profile.json,readonly" \
    --mount "type=bind,source=$manifest_path,target=/opt/warehouse-input/source_manifests.json,readonly" \
    spark-submit -g -- /opt/warehouse-app/.venv/bin/warehouse-bigdata ingest-m5-raw \
    --source-dir /opt/warehouse-input/source \
    --profile-path /opt/warehouse-input/source_profile.json \
    --manifest-path /opt/warehouse-input/source_manifests.json \
    --pipeline-run-id "$pipeline_run_id"
