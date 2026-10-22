#!/usr/bin/env bash
# Host-only provisioning. Never format an existing image or touch HDFS volumes.
set -euo pipefail
[[ $# == 0 && $EUID == 0 ]] || {
    printf 'Run as root on the Linux Docker host: sudo bash scripts/provision-spark-scratch.sh\n' >&2
    exit 2
}
script_dir=${BASH_SOURCE[0]%/*}
repository=$(cd -- "$script_dir/.." && pwd)
base=$repository/artifacts/spark-scratch
mkdir -p "$base"
[[ ! -L $base ]] || exit 1
exec 9>"$base/.provision.lock"
flock -n 9
for worker in 1 2; do
    directory=$base/worker-$worker
    image=$directory/scratch.ext4
    target=$directory/mount
    label=warehouse-sp-$worker
    mkdir -p "$directory" "$target"
    [[ ! -L $directory && ! -L $image && ! -L $target ]] || exit 1
    if [[ ! -e $image ]]; then
        [[ -z $(ls -A "$target") ]] || { printf 'Nonempty scratch mountpoint: %s\n' "$target" >&2; exit 1; }
        available=$(df --output=avail -B1 "$base" | tr -dc '0-9\n' | tr -d '\n')
        # Fully reserve 8 GiB plus 4 GiB host headroom before allocation (not sparse).
        (( available >= 12 * 1024**3 )) || { printf 'Insufficient host disk for scratch allocation\n' >&2; exit 1; }
        (set -o noclobber; : > "$image")
        fallocate -l 8589934592 "$image"
        mkfs.ext4 -q -m 0 -L "$label" "$image"
        chmod 0600 "$image"
    fi
    [[ $(stat -c %s "$image") == 8589934592 ]]
    [[ $(blkid -p -s TYPE -o value "$image") == ext4 ]]
    [[ $(blkid -p -s LABEL -o value "$image") == "$label" ]]
    if mountpoint -q "$target"; then
        device=$(findmnt -n -o SOURCE --target "$target")
        [[ $(losetup -n -O BACK-FILE "$device") == "$image" ]]
    else
        [[ -z $(ls -A "$target") ]] || { printf 'Refusing to cover nonempty path: %s\n' "$target" >&2; exit 1; }
        mount -t ext4 -o loop,rw,nosuid,nodev,noexec "$image" "$target"
    fi
    # Only provision these two exact directories; never recursively chown app data.
    for name in local work; do
        [[ ! -L $target/$name ]] || exit 1
        mkdir -p "$target/$name"
        chown 185:185 "$target/$name"
        chmod 0700 "$target/$name"
    done
    findmnt --target "$target"
done
