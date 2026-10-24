#!/usr/bin/env bash
set +x
set -euo pipefail
query() {
    CLICKHOUSE_PASSWORD=$CLICKHOUSE_PASSWORD clickhouse-client --user "$CLICKHOUSE_USER" \
        --connect_timeout 3 --receive_timeout 10 --send_timeout 10 --query "$1"
}
case ${1:-ready} in
    ready)
        [[ $(query 'SELECT 1') == 1 ]]
        query 'SELECT version(), hostName() FORMAT TSV'
        ;;
    topology)
        expected=$'1\t1\tclickhouse-1\n2\t1\tclickhouse-2'
        actual=$(query "SELECT shard_num, replica_num, host_name FROM system.clusters WHERE cluster = 'warehouse_cluster' ORDER BY shard_num, replica_num FORMAT TSV")
        [[ $actual == "$expected" ]]
        hosts=$(query "SELECT hostName() FROM clusterAllReplicas('warehouse_cluster', system.one) ORDER BY hostName() FORMAT TSV")
        [[ $hosts == $'clickhouse-1\nclickhouse-2' ]]
        query 'SELECT version() FORMAT TSV'
        ;;
    *) exit 2 ;;
esac
