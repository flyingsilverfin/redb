use byte_unit::rust_decimal::prelude::ToPrimitive;
use heed::EnvFlags;
use std::fmt::Display;
use std::fs;
use tempfile::TempDir;

mod common;
use common::*;

mod storage_common;
use storage_common::*;

const DATA_DIR: &str = "/tmp"; //"/mnt/balanced-pd/tmp";
const PROFILE: OpConfig = SMALL;
const THREAD_COUNT: usize = 8;

fn main() {
    let tmpdir = TempDir::new_in(DATA_DIR).unwrap();
    println!("Using benchmark dir: {:?}", &tmpdir);

    // instantiate rocksdb
    let mut rocksdb_opts = rocksdb::Options::default();
    rocksdb_opts.create_if_missing(true);
    let rocksdb_db = rocksdb::OptimisticTransactionDB::open(&rocksdb_opts, tmpdir.path()).unwrap();
    let rocksdb_driver = OptimisticRocksdbBenchDatabase::new(&rocksdb_db);
    preload(&rocksdb_driver, PROFILE, THREAD_COUNT);
    benchmark(&rocksdb_driver, PROFILE, THREAD_COUNT);
    print_data_size(&tmpdir, &rocksdb_driver);

    // instantiate lmdb
    let lmdb_env = unsafe {
        let mut options = heed::EnvOpenOptions::new();
        options.map_size(4096 * 1024 * 1024);
        options.flags(EnvFlags::NO_TLS | EnvFlags::NO_SYNC | EnvFlags::NO_READ_AHEAD);
        options.open(tmpdir.path()).unwrap()
    };
    let lmdb_driver = HeedBenchDatabase::new(&lmdb_env);
    preload(&lmdb_driver, PROFILE, THREAD_COUNT);
    benchmark(&lmdb_driver, PROFILE, THREAD_COUNT);
    print_data_size(&tmpdir, &lmdb_driver);

    fs::remove_dir_all(&tmpdir).unwrap();
}

//
// predefined profiles
//
const SMALL: OpConfig = OpConfig {
    preload_key_count: 1_000_000,
    preload_key_per_tx_count: 1_000,
    benchmark_op_count: 100_000,
    benchmark_op_per_tx_count: 100,
    benchmark_iter_per_op_count: 1_000,
};

const MEDIUM: OpConfig = OpConfig {
    preload_key_count: 10_000_000,
    preload_key_per_tx_count: 1_000,
    benchmark_op_count: 1_00_000,
    benchmark_op_per_tx_count: 100,
    benchmark_iter_per_op_count: 1_000,
};

const BIG: OpConfig = OpConfig {
    preload_key_count: 1000_000_000,
    preload_key_per_tx_count: 1_000,
    benchmark_op_count: 10_000_000,
    benchmark_op_per_tx_count: 100,
    benchmark_iter_per_op_count: 1_000,
};
