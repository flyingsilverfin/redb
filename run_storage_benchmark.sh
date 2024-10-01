#!/bin/bash

set -x

cargo bench --bench storage_benchmark -- s 1 /tmp/ > s-1.md
cargo bench --bench storage_benchmark -- s 4 /tmp/ > s-4.md
cargo bench --bench storage_benchmark -- s 16 /tmp/ > s-16.md

cargo bench --bench storage_benchmark -- m 1 /tmp/ > m-1.md
cargo bench --bench storage_benchmark -- m 4 /tmp/ > m-4.md
cargo bench --bench storage_benchmark -- m 16 /tmp/ > m-16.md