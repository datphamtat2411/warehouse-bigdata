#!/usr/bin/env bash
# Deliberately bypass /opt/starter.sh: normal startup must never format storage.
set -euo pipefail
shopt -s nullglob dotglob

name_dir=${HDFS_NAMENODE_DIR:-/var/lib/hdfs/name}
data_dir=${HDFS_DATANODE_DIR:-/var/lib/hdfs/data}
data1_dir=${HDFS_DATANODE_1_DIR:-/var/lib/hdfs/datanode-1}
data2_dir=${HDFS_DATANODE_2_DIR:-/var/lib/hdfs/datanode-2}

die() { printf 'HDFS lifecycle: %s\n' "$*" >&2; exit 1; }

require_directory() {
    [[ -d $1 && ! -L $1 ]] || die "Missing or symlinked storage directory: $1"
}

is_empty() {
    local entries=("$1"/*)
    ((${#entries[@]} == 0))
}

# Hadoop writes these VERSION properties as one unescaped key=value per line.
# Reject missing, empty, or duplicate identity properties rather than guessing.
property() {
    local value
    value=$(awk -F= -v key="$2" '
        $1 == key { count++; value=$2; if (NF != 2) bad=1 }
        END { if (count != 1 || value == "" || bad) exit 1; print value }
    ' "$1") || die "Invalid $2 in $1; storage was not modified."
    printf '%s' "$value"
}

validate_version() {
    local file=$1 expected_type=$2 value
    [[ -s $file && -f $file && ! -L $file ]] || die "Missing/invalid metadata: $file"
    [[ $(property "$file" storageType) == "$expected_type" ]] || die "Wrong storage type: $file"
    value=$(property "$file" clusterID)
    [[ $value =~ ^CID-[A-Za-z0-9-]+$ ]] || die "Malformed cluster ID: $file"
    value=$(property "$file" layoutVersion)
    [[ $value =~ ^-[0-9]+$ ]] || die "Malformed layout version: $file"
    value=$(property "$file" cTime)
    [[ $value =~ ^[0-9]+$ ]] || die "Malformed creation time: $file"
}

validate_namenode() {
    local current=$name_dir/current value image valid_image=false
    [[ -d $current && ! -L $current ]] || die "NameNode is not initialized: $name_dir"
    validate_version "$current/VERSION" NAME_NODE
    value=$(property "$current/VERSION" namespaceID)
    [[ $value =~ ^[0-9]+$ ]] || die "Malformed namespace ID: $current/VERSION"
    value=$(property "$current/VERSION" blockpoolID)
    [[ $value =~ ^BP-[A-Za-z0-9.:-]+$ ]] || die "Malformed block pool ID: $current/VERSION"
    [[ -s $current/seen_txid && ! -L $current/seen_txid ]] || die "Missing seen_txid: $current"
    value=$(cat "$current/seen_txid")
    [[ $value =~ ^[0-9]+$ ]] || die "Malformed seen_txid: $current"
    for image in "$current"/fsimage_*; do
        [[ ${image##*/} =~ ^fsimage_[0-9]{19}$ ]] || continue
        if [[ -f $image && -s $image && ! -L $image && -s $image.md5 && ! -L $image.md5 ]]; then
            valid_image=true
        fi
    done
    [[ $valid_image == true ]] || die "No complete fsimage/checksum pair: $current"
}

validate_datanode() {
    local directory=$1 expected_cluster=$2 file value
    is_empty "$directory" && return 0
    [[ -d $directory/current && ! -L $directory/current ]] || die "Unrecognized DataNode storage: $directory"
    file=$directory/current/VERSION
    validate_version "$file" DATA_NODE
    value=$(property "$file" storageID)
    [[ $value =~ ^DS-[A-Za-z0-9-]+$ ]] || die "Malformed storage ID: $file"
    value=$(property "$file" datanodeUuid)
    [[ $value =~ ^[A-Za-z0-9-]+$ ]] || die "Malformed DataNode UUID: $file"
    if [[ -n $expected_cluster && $(property "$file" clusterID) != "$expected_cluster" ]]; then
        die "Cluster ID mismatch: $file; retain all volumes and inspect metadata."
    fi
    # Hadoop validates block-pool contents, edit logs, checksums, and deeper integrity.
}

case ${1:-} in
    init)
        for directory in "$name_dir" "$data1_dir" "$data2_dir"; do
            require_directory "$directory"
        done
        if is_empty "$name_dir" && is_empty "$data1_dir" && is_empty "$data2_dir"; then
            [[ $(id -u) == 0 ]] || die "Fresh-volume ownership preparation requires root."
            # Only the three empty volume roots, never recursive ownership changes.
            for directory in "$name_dir" "$data1_dir" "$data2_dir"; do
                chown 1000:100 "$directory"
            done
            runuser -u hadoop -- hdfs namenode -format -nonInteractive
            printf 'HDFS NameNode initialized; all persistent volumes retained.\n'
        else
            validate_namenode
            cluster_id=$(property "$name_dir/current/VERSION" clusterID)
            validate_datanode "$data1_dir" "$cluster_id"
            validate_datanode "$data2_dir" "$cluster_id"
            printf 'HDFS already initialized (%s); no storage changes made.\n' "$cluster_id"
        fi
        ;;
    namenode)
        [[ $(id -u) == 1000 ]] || die "Daemons must run as the image's hadoop user (UID 1000)."
        require_directory "$name_dir"
        validate_namenode
        exec hdfs namenode
        ;;
    datanode)
        [[ $(id -u) == 1000 ]] || die "Daemons must run as the image's hadoop user (UID 1000)."
        [[ -n ${HDFS_DATANODE_HOSTNAME:-} ]] || die "HDFS_DATANODE_HOSTNAME is required."
        require_directory "$data_dir"
        validate_datanode "$data_dir" ""
        exec hdfs datanode
        ;;
    *) die "Usage: lifecycle.sh {init|namenode|datanode}" ;;
esac
