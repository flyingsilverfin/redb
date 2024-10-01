use byte_unit::rust_decimal::prelude::ToPrimitive;
use std::fmt::Display;
use std::mem::size_of;
use std::path::Path;
use std::time::Instant;
use std::thread;
use tempfile::TempDir;

use crate::common::*;

use crate::storage_op_size::OpSize;

const KEY_SIZE: usize = 48;

pub fn preload<T: BenchDatabase + Send + Sync>(driver: &T, op_size: &OpSize, thread_count: usize) {
    let start = Instant::now();
    thread::scope(|scope| {
        for thread_id in 0..thread_count {
            scope.spawn(move || {
                let mut rng = create_rng(thread_id.to_u64().unwrap());
                for i in 0..(op_size.preload_key_count / op_size.preload_key_per_tx_count / thread_count) {
                    let mut tx = driver.write_transaction();
                    {
                        let mut inserter = tx.get_inserter();
                        for k in 0..op_size.preload_key_per_tx_count {
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
        op_size.preload_key_count,
        duration.as_millis()
    );
}

pub fn benchmark<T: BenchDatabase + Send + Sync>(driver: &T, op_size: &OpSize, thread_count: usize) {
    let mut total_scanned_key = 0;
    let start = Instant::now();
    thread::scope(|s| {
        for thread_id in 0..thread_count {
            s.spawn(move || {
                let mut rng = create_rng(thread_id.to_u64().unwrap());
                for i in 0..(op_size.benchmark_op_count / op_size.benchmark_op_per_tx_count / thread_count) {
                    let tx = driver.read_transaction();
                    {
                        let reader = tx.get_reader();
                        for k in 0..op_size.benchmark_op_per_tx_count {
                            let key = gen_key(&mut rng); // TODO: should be a prefix of some value, not the value itself
                            let mut scanned_key = 0;
                            let mut iter = reader.range_from(&key);
                            for i in 0..op_size.benchmark_iter_per_op_count {
                                scanned_key += 1;
                                match iter.next() {
                                    Some((_, value)) => {}
                                    None => { break; }
                                }
                            }
                            total_scanned_key += scanned_key;
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
        total_scanned_key,
        op_size.benchmark_op_count,
        duration.as_millis()
    );
}

pub fn print_data_size<T: BenchDatabase + Send + Sync>(tmpdir: &TempDir, _: &T) {
    let size = database_size(tmpdir.path());
    println!("{}: Database size: {} bytes", T::db_type_name(), size);
}

fn create_rng(seed: u64) -> fastrand::Rng {
    fastrand::Rng::with_seed(seed)
}

fn gen_key(rng: &mut fastrand::Rng) -> [u8; KEY_SIZE] {
    let mut key = [0u8; KEY_SIZE];
    fill_slice(&mut key, rng);
    key
}

fn fill_slice(slice: &mut [u8], rng: &mut fastrand::Rng) {
    let mut i = 0;
    while i + size_of::<u128>() < slice.len() {
        let tmp = rng.u128(..);
        slice[i..(i + size_of::<u128>())].copy_from_slice(&tmp.to_le_bytes());
        i += size_of::<u128>()
    }
    if i + size_of::<u64>() < slice.len() {
        let tmp = rng.u64(..);
        slice[i..(i + size_of::<u64>())].copy_from_slice(&tmp.to_le_bytes());
        i += size_of::<u64>()
    }
    if i + size_of::<u32>() < slice.len() {
        let tmp = rng.u32(..);
        slice[i..(i + size_of::<u32>())].copy_from_slice(&tmp.to_le_bytes());
        i += size_of::<u32>()
    }
    if i + size_of::<u16>() < slice.len() {
        let tmp = rng.u16(..);
        slice[i..(i + size_of::<u16>())].copy_from_slice(&tmp.to_le_bytes());
        i += size_of::<u16>()
    }
    if i + size_of::<u8>() < slice.len() {
        slice[i] = rng.u8(..);
    }
}

fn database_size(path: &Path) -> u64 {
    let mut size = 0u64;
    for result in walkdir::WalkDir::new(path) {
        let entry = result.unwrap();
        size += entry.metadata().unwrap().len();
    }
    size
}
