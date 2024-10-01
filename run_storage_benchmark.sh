#!/bin/bash

set -ux

TMP_DIR=$1
OP_SIZE=('s' 'm' 'b')
THREAD_COUNT=(1 4 16)

for op_size in "${OP_SIZE[@]}"; do
  for thread_count in "${THREAD_COUNT[@]}"; do
    echo "Benchmark version: $(git rev-parse HEAD)" > "$op_size-$thread_count.md"
    RUST_BACKTRACE=full cargo bench --bench storage_benchmark -- "$op_size" "$thread_count" "$TMP_DIR" >> "$op_size-$thread_count.md"
  done
done
