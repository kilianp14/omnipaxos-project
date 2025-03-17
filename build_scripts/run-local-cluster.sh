#!/bin/bash

usage="Usage: run-local-cluster.sh"
cluster_size=3
n_shards=2
rust_log="info"

# Clean up child processes
interrupt() {
    pkill -P $$
    ./clear_db.sh
}
trap "interrupt" SIGINT

# Servers' output is saved into logs dir
local_experiment_dir="./logs"
mkdir -p "${local_experiment_dir}"

# Run coordinators and shards
cluster_config_path="./coordinator-cluster-config.toml"
for ((i = 1; i <= cluster_size; i++)); do
    server_config_path="./coordinator-${i}-config.toml"
    RUST_LOG=$rust_log SERVER_CONFIG_FILE=$server_config_path CLUSTER_CONFIG_FILE=$cluster_config_path cargo run --manifest-path="../Cargo.toml" --bin coordinator &
    for ((j = 1; j <= n_shards; j++)); do
        shard_cluster_config_path="./shard${j}-cluster-config.toml"
        shard_server_config_path="./shard${j}-${i}-config.toml"
        RUST_LOG=$rust_log SERVER_CONFIG_FILE=$shard_server_config_path CLUSTER_CONFIG_FILE=$shard_cluster_config_path cargo run --manifest-path="../Cargo.toml" --bin shard &
    done
done
wait

