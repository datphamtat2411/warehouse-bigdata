#!/usr/bin/env bash
# Bounded HTTP/JMX and TCP probes; do not start a JVM for container health.
set -euo pipefail

jmx() {
    curl --fail --silent --show-error --connect-timeout 2 --max-time 4 \
        "http://$1:$2/jmx?qry=Hadoop:service=$3,name=$4"
}

namenode_info() { jmx hdfs-namenode 9870 NameNode NameNodeInfo; }

# LiveNodes is a JSON-encoded string in Hadoop's pretty-printed JMX response.
# Restrict matching to that field, never DeadNodes or unrelated configuration.
live_nodes() {
    sed -n 's/^[[:space:]]*"LiveNodes"[[:space:]]*:[[:space:]]*"\(.*\)"[,]*[[:space:]]*$/\1/p'
}

has_hostname() {
    local decoded=${1//\\\"/\"}
    [[ $decoded == *"\"$2:"* ]]
}

ready() {
    local info live state
    info=$(namenode_info) || return 1
    grep -Eq '"Safemode"[[:space:]]*:[[:space:]]*""' <<< "$info" || return 1
    live=$(live_nodes <<< "$info")
    has_hostname "$live" hdfs-datanode-1 || return 1
    has_hostname "$live" hdfs-datanode-2 || return 1
    state=$(jmx hdfs-namenode 9870 NameNode FSNamesystemState) || return 1
    grep -Eq '"NumLiveDataNodes"[[:space:]]*:[[:space:]]*2[[:space:]]*[,}]' <<< "$state"
}

case ${1:-} in
    namenode)
        response=$(jmx 127.0.0.1 9870 NameNode NameNodeStatus)
        grep -Eq '"State"[[:space:]]*:[[:space:]]*"active"' <<< "$response"
        # No dependency on DataNode registration or safe mode here.
        timeout 2 bash -c 'exec 3<>/dev/tcp/127.0.0.1/8020'
        ;;
    datanode)
        response=$(jmx 127.0.0.1 9864 DataNode DataNodeInfo)
        grep -Eq '"name"[[:space:]]*:[[:space:]]*"Hadoop:service=DataNode,name=DataNodeInfo"' <<< "$response"
        response=$(namenode_info)
        live=$(live_nodes <<< "$response")
        has_hostname "$live" "${HDFS_DATANODE_HOSTNAME:?Missing DataNode hostname}"
        ;;
    ready) ready ;;
    wait-ready)
        wait_seconds=${2:-180}
        [[ $wait_seconds =~ ^[1-9][0-9]*$ && $wait_seconds -le 600 ]] || {
            printf 'Readiness timeout must be between 1 and 600 seconds.\n' >&2; exit 1;
        }
        deadline=$((SECONDS + wait_seconds))
        until ready; do
            if ((SECONDS >= deadline)); then
                printf 'HDFS not write-ready: require two live DataNodes and safe mode off.\n' >&2
                exit 1
            fi
            sleep 2
        done
        printf 'HDFS write-ready: two live DataNodes; safe mode off.\n'
        ;;
    *) printf 'Usage: healthcheck.sh {namenode|datanode|ready|wait-ready [seconds]}\n' >&2; exit 1 ;;
esac
