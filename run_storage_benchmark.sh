#!/bin/bash

set -ux

RUN_BASE_DIR="$1"
OP_SIZES="${@:2}"

THREAD_COUNTS=(1)

for thread_count in "${THREAD_COUNTS[@]}"; do
  run_dir_name="${OP_SIZES// /-}-$thread_count"
  run_dir="$RUN_BASE_DIR/$run_dir_name"
  rm -rf "$run_dir"
  mkdir -p "$run_dir"
  child_log_file="$run_dir/log.md"
  echo "Benchmark version: $(git rev-parse HEAD)" >> "$child_log_file"
  echo >> "$child_log_file"
  for op_size in "${@:2}"; do
    echo "Operation: {op_size}"
    date >> "$child_log_file"
    RUST_BACKTRACE=full cargo bench --bench storage_benchmark -- "$op_size" "$thread_count" "$run_dir" >> "$child_log_file"
    echo >> "$child_log_file"
  done
done
