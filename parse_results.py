#! /bin/bash

import os
import argparse
import re

args_parser = argparse.ArgumentParser()
args_parser.add_argument(
    "--dir",
    required=True,
    help="Directory to recursively search for 'log.md' files",
)

args = args_parser.parse_args()

OP_SIZE_REGEX = re.compile("op size: OpSize { insert_key_total_count: (.*), insert_key_per_tx_count: (.*), scan_total_count: (.*), scan_per_tx_count: (.*), iter_per_scan_count: (.*) }")
LOAD_REGEX = re.compile("Preload done: loaded (.*) keys in (.*)ms")
SCAN_REGEX = re.compile("Scan done: (.*) scan ops in (.*)ms")
KEYS_REGEX = re.compile("Database keys: (.*) keys")

def ops_per_milli_to_per_sec(ops, millis):
    return int(float(ops) / (float(millis) / 1000.0))

def print_summaries(log_file_path):
    with open(log_file_path) as log:
        data = log.read()
        op_sizes = OP_SIZE_REGEX.findall(data)
        load_times = LOAD_REGEX.findall(data)
        scan_times = SCAN_REGEX.findall(data)
        key_counts = KEYS_REGEX.findall(data)
        if len(op_sizes) == len(load_times) and len(op_sizes) == len(scan_times) and len(op_sizes) == len(key_counts):
            for index in range(0, len(op_sizes)):
                (keys_inserted, keys_per_tx, scans, scans_per_tx, iter_per_scan) = op_sizes[index]
                (loaded, loaded_millis) = load_times[index]
                (scans, scans_millis) = scan_times[index]
                key_count = key_counts[index]

                print(f"### {log_file_path}")
                print(f"Keys added: {keys_inserted}, keys per txn: {keys_per_tx}, scans opened: {scans}, scan advances: {iter_per_scan}")
                # print(f"Load keys/sec: \t\t\t {ops_per_milli_to_per_sec(loaded, loaded_millis)}")
                # print(f"Scan (open + advances) / sec: \t {ops_per_milli_to_per_sec(scans, scans_millis)}")
                # print(f"Total keys:\t\t\t{key_count}")

                # Load keys / sec
                print(f"L {ops_per_milli_to_per_sec(loaded, loaded_millis)}")
                # Scans ( = opened, but time includes advances) / sec
                print(f"S {ops_per_milli_to_per_sec(scans, scans_millis)}")
                print(f"T {key_count}")
                print("")

        else:
            print(f"Skipping this file {log_file_path}, since the op_sizes, load_times, scan_times, keys count don't match lengths: {len(op_sizes)}, {len(load_times)}, {len(scan_times)}, {len(key_counts)}")


walk = os.walk(args.dir)
for path in walk:
    (path, _subdirs, files) = path
    for file in files:
        if file == "log.md":
            log_file_path = os.path.join(path, file)
            print_summaries(log_file_path)

