#!/bin/bash

set -ux

DIR=$1
THREAD_COUNT=(1)

mkdir -p $DIR

for thread_count in "${THREAD_COUNT[@]}"; do
  out_file="${@:2}-$thread_count.md"
  echo "Deleting $out_file if it exists, then starting benchmarks"
  rm "$out_file"
  echo "Benchmark version: $(git rev-parse HEAD)" >> "$out_file"
  echo >> "$out_file"
  for op_size in "${@:2}"; do
    echo "Operation: {op_size}"
    date >> "$out_file"
    RUST_BACKTRACE=full cargo bench --bench storage_benchmark -- "$op_size" "$thread_count" "$DIR" >> "$out_file"
    echo >> "$out_file"
  done
done
