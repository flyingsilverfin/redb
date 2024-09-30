use std::env::current_dir;
use std::mem::size_of;
use std::path::Path;
use std::sync::Arc;
use std::{fs, process, thread};
use heed::EnvFlags;
use tempfile::{NamedTempFile, TempDir};

mod common;
use common::*;

use std::time::{Duration, Instant};
use byte_unit::rust_decimal::prelude::ToPrimitive;
use fastrand::Rng;
use rocksdb::TransactionDB;

const DATA_DIR: &str = "/tmp"; //"/mnt/balanced-pd/tmp";
const PROFILE: Params = MEDIUM;
const THREAD_COUNT: usize = 8;

const SMALL: Params = Params {
    preload_key_count: 1_000_000,
    preload_key_per_tx_count: 1_000,
    benchmark_op_count: 100_000,
    benchmark_op_per_tx_count: 100,
    benchmark_iter_per_op_count: 1_000,
};

const MEDIUM: Params = Params {
    preload_key_count: 10_000_000,
    preload_key_per_tx_count: 1_000,
    benchmark_op_count: 1_00_000,
    benchmark_op_per_tx_count: 100,
    benchmark_iter_per_op_count: 1_000,
};

const KEY_SIZE: usize = 48;

fn main() {
    let tmpdir = TempDir::new_in(DATA_DIR).unwrap();
    dbg!("Using benchmark dir: {}", &tmpdir);

    // instantiate rocksdb
    let mut rocksdb_opts = rocksdb::Options::default();
    rocksdb_opts.create_if_missing(true);
    let rocksdb_db = rocksdb::OptimisticTransactionDB::open(&rocksdb_opts, tmpdir.path()).unwrap();
    let rocksdb_driver = OptimisticRocksdbBenchDatabase::new(&rocksdb_db);
    preload(&rocksdb_driver);
    benchmark(&rocksdb_driver);
    print_size(&tmpdir, &rocksdb_driver);

    // instantiate lmdb
    let lmdb_env = unsafe {
        let mut options = heed::EnvOpenOptions::new();
        options.map_size(4096 * 1024 * 1024);
        options.flags(EnvFlags::NO_TLS | EnvFlags::NO_SYNC | EnvFlags::NO_READ_AHEAD);
        options.open(tmpdir.path()).unwrap()
    };
    let lmdb_driver = HeedBenchDatabase::new(&lmdb_env);
    preload(&lmdb_driver);
    benchmark(&lmdb_driver);
    print_size(&tmpdir, &lmdb_driver);

    fs::remove_dir_all(&tmpdir).unwrap();
}

fn preload<T: BenchDatabase + Send + Sync>(driver: &T) {
    let start = Instant::now();
        thread::scope(|scope| {
            for thread_id in 0..THREAD_COUNT {
                scope.spawn(move || {
                    let mut rng = create_rng(thread_id.to_u64().unwrap());
                    for i in 0..(PROFILE.preload_key_count / PROFILE.preload_key_per_tx_count / THREAD_COUNT) {
                        let mut tx = driver.write_transaction();
                        {
                            let mut inserter = tx.get_inserter();
                            for k in 0..PROFILE.preload_key_per_tx_count {
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
        PROFILE.preload_key_count,
        duration.as_millis()
    );
}

fn benchmark<T: BenchDatabase + Send + Sync>(driver: &T) {
    let mut total_scanned_key = 0;
    let start = Instant::now();
    thread::scope(|s| {
        for thread_id in 0..THREAD_COUNT {
            s.spawn(move || {
                let mut rng = create_rng(thread_id.to_u64().unwrap());
                for i in 0..(PROFILE.benchmark_op_count / PROFILE.benchmark_op_per_tx_count / THREAD_COUNT) {
                    let tx = driver.read_transaction();
                    {
                        let reader = tx.get_reader();
                        for k in 0..PROFILE.benchmark_op_per_tx_count {
                            let key = gen_key(&mut rng); // TODO: should be a prefix of some value, not the value itself
                            let mut scanned_key = 0;
                            let mut iter = reader.range_from(&key);
                            for i in 0..PROFILE.benchmark_iter_per_op_count {
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
        PROFILE.benchmark_op_count,
        duration.as_millis()
    );
}

fn print_size<T: BenchDatabase + Send + Sync>(tmpdir: &TempDir, _: &T) {
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

struct Params {
    preload_key_count: usize,
    preload_key_per_tx_count: usize,
    benchmark_op_count: usize,
    benchmark_op_per_tx_count: usize,
    benchmark_iter_per_op_count: usize,
}
