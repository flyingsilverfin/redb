#!/bin/bash

set -ux

git_version=$(git rev-parse HEAD)
DIR="$1"
THREAD_COUNT=(1)

for thread_count in "${THREAD_COUNT[@]}"; do
  child_dir_name="${@:2}-$thread_count"
  child_dir="$DIR/$child_dir_name"
  rm -rf "$child_dir"
  mkdir -p "$child_dir"
  child_log_file="$child_dir/log.md"
  echo "Benchmark version: $(git rev-parse HEAD)" >> "$child_log_file"
  echo >> "$child_log_file"
  for op_size in "${@:2}"; do
    echo "Operation: {op_size}"
    date >> "$child_log_file"
    RUST_BACKTRACE=full cargo bench --bench storage_benchmark -- "$op_size" "$thread_count" "$child_dir" >> "$child_log_file"
    echo >> "$child_log_file"
  done
done
