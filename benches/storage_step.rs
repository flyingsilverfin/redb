use byte_unit::rust_decimal::prelude::ToPrimitive;
use std::fmt::Display;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

use crate::common::*;

use crate::storage_common::*;
use crate::storage_op_size::OpSize;

const PRINT_FREQUENCY_SEC: Duration = Duration::new(2, 0);

pub fn preload<T: BenchDatabase + Send + Sync>(driver: &T, op_size: &OpSize, thread_count: usize) {
    let start = Instant::now();
    thread::scope(|scope| {
        for thread_id in 0..thread_count {
            scope.spawn(move || {
                let mut rng = create_rng();
                let mut last_printed = Instant::now();
                let mut transactions = 0;
                for _ in 0..(op_size.insert_key_total_count / op_size.insert_key_per_tx_count / thread_count) {
                    let mut tx = driver.write_transaction();
                    {
                        let mut inserter = tx.get_inserter();
                        for _ in 0..op_size.insert_key_per_tx_count {
                            let key = gen_key(&mut rng);
                            let value = Vec::new();
                            match inserter.insert(&key, &value) {
                                Ok(()) => {}
                                Err(()) => {}
                            }
                        }
                    }
                    tx.commit().unwrap();
                    transactions += 1;
                    let time_since_last_print = Instant::now() - last_printed;
                    if time_since_last_print > PRINT_FREQUENCY_SEC {
                        print_insertion_speed(op_size, thread_id, transactions, time_since_last_print);
                        last_printed = Instant::now();
                        transactions = 0;
                    }
                }
            });
        }
    });

    let end = Instant::now();
    let duration = end - start;
    println!(
        "{}: Preload done: loaded {} keys in {}ms",
        T::db_type_name(),
        op_size.insert_key_total_count,
        duration.as_millis()
    );
}

pub fn benchmark<T: BenchDatabase + Send + Sync>(driver: &T, op_size: &OpSize, thread_count: usize) {
    let start = Instant::now();
    thread::scope(|s| {
        for thread_id in 0..thread_count {
            s.spawn(move || {
                let mut last_printed = Instant::now();
                let mut transactions = 0;
                let mut rng = create_rng();
                for _ in 0..(op_size.scan_total_count / op_size.scan_per_tx_count / thread_count) {
                    let tx = driver.read_transaction();
                    {
                        let reader = tx.get_reader();
                        for _ in 0..op_size.scan_per_tx_count {
                            let key = gen_key(&mut rng); // TODO: should be a prefix of some value, not the value itself
                            let mut scanned_key = 0;
                            let mut iter = reader.range_from(&key);
                            for i in 0..op_size.iter_per_scan_count {
                                scanned_key += 1;
                                match iter.next() {
                                    Some((_, value)) => {}
                                    None => { break; }
                                }
                            }
                        }
                    }
                    drop(tx);
                    transactions += 1;
                    let time_since_last_print = Instant::now() - last_printed;
                    if time_since_last_print > PRINT_FREQUENCY_SEC {
                        print_scan_speed(op_size, thread_id, transactions, time_since_last_print);
                        last_printed = Instant::now();
                        transactions = 0;
                    }
                }
            });
        }
    });
    let end = Instant::now();
    let duration = end - start;
    println!(
        "{}: Scan done: {} scan ops in {}ms",
        T::db_type_name(),
        op_size.scan_total_count,
        duration.as_millis()
    );
}

fn print_insertion_speed(op_size: &OpSize, thread_id: usize, mut transactions: usize, time_since_last_print: Duration) {
    let keys = transactions * op_size.insert_key_per_tx_count;
    let key_per_sec = keys.to_f64().unwrap() / (time_since_last_print.as_nanos().to_f64().unwrap() / 1000_000_000.0);
    println!(
        "  thread {}: insertion of {} keys took {}ms ({} key/s)",
        thread_id,
        keys,
        time_since_last_print.as_millis(),
        key_per_sec as u64
    );
}

fn print_scan_speed(op_size: &OpSize, thread_id: usize, mut transactions: usize, time_since_last_print: Duration) {
    let keys = transactions * op_size.scan_per_tx_count * op_size.iter_per_scan_count;
    let key_per_sec = keys.to_f64().unwrap() / (time_since_last_print.as_nanos().to_f64().unwrap() / 1000_000_000.0);
    println!(
        "  thread {}: scanning of {} keys took {}ms ({} key/s)",
        thread_id,
        keys,
        time_since_last_print.as_millis(),
        key_per_sec as u64
    );
}

pub fn print_data_size<T: BenchDatabase + Send + Sync>(path: &Path, _: &T) {
    let size = database_size(path);
    println!("{}: Database size: {} bytes", T::db_type_name(), size);
}
