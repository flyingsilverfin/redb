#!/bin/bash

set -u

RUN_BASE_DIR="$1"

OP_SIZES="${@:2}"
THREAD_COUNTS=(1)

COMMIT="$(git rev-parse HEAD)"

for thread_count in "${THREAD_COUNTS[@]}"; do
  run_dir_name="${OP_SIZES// /-}-$thread_count"
  run_dir="$RUN_BASE_DIR/$run_dir_name"
  rm -rf "$run_dir"
  mkdir -p "$run_dir"
  run_log_output="$run_dir/log.md"
  echo "Benchmark version: $COMMIT" >> "$run_log_output"
  echo >> "$run_log_output"
  for op_size in "${@:2}"; do
    echo "Operation: $op_size"
    date >> "$run_log_output"
    RUST_BACKTRACE=full cargo bench --bench storage_benchmark -- "$op_size" "$thread_count" "$run_dir" >> "$run_log_output"
    echo >> "$run_log_output"
  done
done
