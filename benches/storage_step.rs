use byte_unit::rust_decimal::prelude::ToPrimitive;
use std::fmt::Display;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};
use std::thread;
use std::time::Instant;
use tempfile::TempDir;

use crate::common::*;

use crate::storage_common::*;
use crate::storage_op_size::OpSize;

pub fn preload<T: BenchDatabase + Send + Sync>(driver: &T, op_size: &OpSize, thread_count: usize) {
    let start = Instant::now();
    thread::scope(|scope| {
        for thread_id in 0..thread_count {
            scope.spawn(move || {
                let mut rng = create_rng(thread_id.to_u64().unwrap());
                for i in 0..(op_size.insert_key_total_count / op_size.insert_key_per_tx_count / thread_count) {
                    let mut tx = driver.write_transaction();
                    {
                        let mut inserter = tx.get_inserter();
                        for k in 0..op_size.insert_key_per_tx_count {
                            let key = gen_key(&mut rng);
                            let value = Vec::new();
                            match inserter.insert(&key, &value) {
                                Ok(()) => {}
                                Err(()) => {}
                            }
                        }
                    }
                    tx.commit().unwrap();
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
    let mut total_scanned_key = Arc::new(AtomicI32::new(0));
    let start = Instant::now();
    thread::scope(|s| {
        for thread_id in 0..thread_count {
            let scanned_key_ref = total_scanned_key.clone();
            s.spawn(move || {
                let mut rng = create_rng(thread_id.to_u64().unwrap());
                for i in 0..(op_size.scan_total_count / op_size.scan_per_tx_count / thread_count) {
                    let tx = driver.read_transaction();
                    {
                        let reader = tx.get_reader();
                        for k in 0..op_size.scan_per_tx_count {
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
                            scanned_key_ref.fetch_add(scanned_key, Ordering::SeqCst);
                        }
                    }
                    drop(tx);
                }
            });
        }
    });
    let end = Instant::now();
    let duration = end - start;
    println!(
        "{}: Scan done: scanned {} total entries from {} scan ops, in {}ms",
        T::db_type_name(),
        total_scanned_key.load(Ordering::SeqCst),
        op_size.scan_total_count,
        duration.as_millis()
    );
}

pub fn print_data_size<T: BenchDatabase + Send + Sync>(tmpdir: &TempDir, _: &T) {
    let size = database_size(tmpdir.path());
    println!("{}: Database size: {} bytes", T::db_type_name(), size);
}
