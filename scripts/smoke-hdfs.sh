#!/usr/bin/env bash
# A small T01 check, not node-failure or separate-client integration tooling.
set -euo pipefail

repository=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
compose=(docker compose -f "$repository/infrastructure/compose.yaml")

"${compose[@]}" exec -T hdfs-namenode bash /opt/warehouse-hdfs/healthcheck.sh wait-ready 180

uuid1=$("${compose[@]}" exec -T hdfs-datanode-1 \
    awk -F= '$1 == "datanodeUuid" {print $2}' /var/lib/hdfs/data/current/VERSION)
uuid2=$("${compose[@]}" exec -T hdfs-datanode-2 \
    awk -F= '$1 == "datanodeUuid" {print $2}' /var/lib/hdfs/data/current/VERSION)
[[ -n $uuid1 && -n $uuid2 && $uuid1 != "$uuid2" ]] || {
    printf 'Expected two distinct, persistent DataNode UUIDs.\n' >&2; exit 1;
}
printf 'DataNode UUIDs: hdfs-datanode-1=%s hdfs-datanode-2=%s\n' "$uuid1" "$uuid2"

path="/t01-smoke-$(date +%s)-$$-$RANDOM"
"${compose[@]}" exec -T hdfs-namenode bash -s -- "$path" <<'HDFS_SMOKE'
set -euo pipefail
path=$1
payload="T01 HDFS round-trip: $path"
report=$(timeout 30 hdfs dfsadmin -report -live)
printf '%s\n' "$report"
grep -Eq '^Live datanodes \(2\):' <<< "$report"
safe_mode=$(timeout 30 hdfs dfsadmin -safemode get)
[[ $safe_mode == 'Safe mode is OFF' ]]

address_for() {
    awk -v host="$1" '/^Name: / {address=$2} /^Hostname: / && $2 == host {print address}' <<< "$report"
}
address1=$(address_for hdfs-datanode-1)
address2=$(address_for hdfs-datanode-2)
[[ -n $address1 && -n $address2 && $address1 != "$address2" ]]

# mkdir is exclusive: install cleanup only after this script creates its own path.
timeout 30 hdfs dfs -mkdir "$path"
cleanup() { timeout 30 hdfs dfs -rm -r -skipTrash "$path"; }
trap cleanup EXIT
printf '%s\n' "$payload" | timeout 30 hdfs dfs -put - "$path/payload"
[[ $(timeout 30 hdfs dfs -stat '%r' "$path/payload") == 2 ]]
timeout 60 hdfs dfs -setrep -w 2 "$path/payload"
expected=$(printf '%s\n' "$payload" | sha256sum | awk '{print $1}')
actual=$(timeout 30 hdfs dfs -cat "$path/payload" | sha256sum | awk '{print $1}')
[[ $actual == "$expected" ]]
timeout 30 hdfs dfs -checksum "$path/payload"

placement=$(timeout 30 hdfs fsck "$path/payload" -files -blocks -locations)
printf '%s\n' "$placement"
replicas=$(grep 'Live_repl=2' <<< "$placement")
grep -Fq "$address1" <<< "$replicas"
grep -Fq "$address2" <<< "$replicas"
printf 'HDFS smoke passed: SHA-256 %s; block replicas on both DataNodes.\n' "$actual"
HDFS_SMOKE
