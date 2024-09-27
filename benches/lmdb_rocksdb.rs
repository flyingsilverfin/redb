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
use rocksdb::TransactionDB;

const DATA_DIR: &str = "/tmp"; //"/mnt/balanced-pd/tmp";
const RNG_SEED: u64 = 3;

const PRELOAD_KEY_COUNT: usize = 1_000_000;
const PRELOAD_KEY_PER_TX_COUNT: usize = 10_000;
const BENCH_OP_COUNT: usize = 1_000_000;
const BENCH_OP_PER_TX_COUNT: usize = 1_000;
const KEY_SIZE: usize = 48;
const VALUE_SIZE: usize = 0;

fn main() {
    let tmpdir = TempDir::new_in(DATA_DIR).unwrap();
    dbg!("Using benchmark dir: {}", &tmpdir);
    let mut rng = make_rng();

    // instantiate rocksdb
    let mut rocksdb_bb_opts = rocksdb::BlockBasedOptions::default();
    rocksdb_bb_opts.set_block_cache(&rocksdb::Cache::new_lru_cache(4 * 1_024 * 1_024 * 1_024));
    let mut rocksdb_opts = rocksdb::Options::default();
    rocksdb_opts.set_block_based_table_factory(&rocksdb_bb_opts);
    rocksdb_opts.create_if_missing(true);
    let rocksdb_tx_opts = Default::default();
    let rocksdb_db: TransactionDB = rocksdb::TransactionDB::open(&rocksdb_opts, &rocksdb_tx_opts, tmpdir.path()).unwrap();
    let rocksdb_bench = RocksdbBenchDatabase::new(&rocksdb_db);

    for i in 0..(PRELOAD_KEY_COUNT/ PRELOAD_KEY_PER_TX_COUNT) {
        let mut rocksdb_tx = rocksdb_bench.write_transaction();
        let mut rocksdb_inserter = rocksdb_tx.get_inserter();
        for i in 0..PRELOAD_KEY_PER_TX_COUNT {
            let key = gen_key(&mut rng);
            let value = Vec::new();
            rocksdb_inserter.insert(&key, &value).unwrap()
        }
        drop(rocksdb_inserter);
        rocksdb_tx.commit().unwrap();
    }

    for i in 0..(BENCH_OP_COUNT/BENCH_OP_PER_TX_COUNT) {
        for i in 0..BENCH_OP_PER_TX_COUNT {
            // seek and iterate
        }
    }

    fs::remove_dir_all(&tmpdir).unwrap();
}

fn make_rng() -> fastrand::Rng {
    fastrand::Rng::with_seed(RNG_SEED)
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
