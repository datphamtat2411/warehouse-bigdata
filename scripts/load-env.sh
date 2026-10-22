#!/usr/bin/env bash
# Source this file from a host-side workflow to load the repository's local environment.

warehouse_load_env() {
    local loader_path loader_dir repository env_file allexport_was_set xtrace_was_set status
    loader_path=${BASH_SOURCE[0]}
    loader_dir=${loader_path%/*}
    [[ $loader_dir == "$loader_path" ]] && loader_dir=.
    repository=$(cd -- "$loader_dir/.." && pwd)
    env_file=$repository/.env
    [[ -f $env_file ]] || return 0

    allexport_was_set=0
    xtrace_was_set=0
    if [[ $- == *a* ]]; then
        allexport_was_set=1
    else
        set -a
    fi
    if [[ $- == *x* ]]; then
        xtrace_was_set=1
        set +x
    fi

    if . "$env_file"; then
        status=0
    else
        status=$?
    fi

    (( allexport_was_set )) || set +a
    (( xtrace_was_set )) && set -x
    if (( status != 0 )); then
        printf 'Cannot load project environment file: %s\n' "$env_file" >&2
    fi
    return "$status"
}

warehouse_load_env
load_status=$?
if (( load_status != 0 )); then
    if [[ ${BASH_SOURCE[0]} == "$0" ]]; then
        exit "$load_status"
    fi
    return "$load_status"
fi

# Direct execution is useful for one-shot host commands, including integration tests.
if [[ ${BASH_SOURCE[0]} == "$0" && $# -gt 0 ]]; then
    command_path=${BASH_SOURCE[0]}
    command_dir=${command_path%/*}
    [[ $command_dir == "$command_path" ]] && command_dir=.
    command_repository=$(cd -- "$command_dir/.." && pwd)
    cd -- "$command_repository"
    exec "$@"
fi
