#!/bin/bash

set -ux

cargo bench --bench storage_benchmark -- s 1 $TMP_DIR > s-1.md
cargo bench --bench storage_benchmark -- s 4 $TMP_DIR > s-4.md
cargo bench --bench storage_benchmark -- s 16 $TMP_DIR > s-16.md

cargo bench --bench storage_benchmark -- m 1 $TMP_DIR > m-1.md
cargo bench --bench storage_benchmark -- m 4 $TMP_DIR > m-4.md
cargo bench --bench storage_benchmark -- m 16 $TMP_DIR > m-16.md