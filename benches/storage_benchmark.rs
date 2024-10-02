use byte_unit::rust_decimal::prelude::ToPrimitive;
use heed::EnvFlags;
use std::env;
use std::fmt::Display;
use std::fs;
use tempfile::TempDir;

mod common;
mod storage_common;
mod storage_op_size;
mod storage_step;

use common::*;
use storage_step::*;
use crate::storage_op_size::OpSize;

fn main() {
    let mut args: Vec<String> = env::args().collect();
    args.pop().unwrap(); // pop '--bench'
    let tmpdir_path = args.pop().unwrap();
    let thread_count = args.pop().unwrap().parse::<usize>().unwrap();
    let op_size = OpSize::from_str(args.pop().unwrap().as_str());

    println!("op size: {:?}\nthread count: {}\ntmp dir: {:?}", &op_size, thread_count, &tmpdir_path);

    rocksdb_benchmark(&op_size, thread_count, &tmpdir_path);
    lmdb_benchmark(&op_size, thread_count, &tmpdir_path);

}

fn rocksdb_benchmark(op_size: &OpSize, thread_count: usize, tmpdir_path: &String) {
    let tmpdir = TempDir::new_in(tmpdir_path).unwrap();
    let mut rocksdb_opts = rocksdb::Options::default();
    rocksdb_opts.create_if_missing(true);
    let rocksdb_db = rocksdb::OptimisticTransactionDB::open(&rocksdb_opts, tmpdir.path()).unwrap();
    let rocksdb_driver = OptimisticRocksdbBenchDatabase::new(&rocksdb_db);
    preload(&rocksdb_driver, &op_size, thread_count);
    benchmark(&rocksdb_driver, &op_size, thread_count);
    print_data_size(&tmpdir, &rocksdb_driver);
    fs::remove_dir_all(&tmpdir).unwrap();
}

fn lmdb_benchmark(op_size: &OpSize, thread_count: usize, tmpdir_path: &String) {
    let tmpdir = TempDir::new_in(tmpdir_path).unwrap();
    let lmdb_env = unsafe {
        let mut options = heed::EnvOpenOptions::new();
        options.map_size(500 * 1024 * 1024 * 1024); // 500GB
        options.flags(EnvFlags::NO_TLS | EnvFlags::NO_SYNC | EnvFlags::NO_READ_AHEAD);
        options.open(tmpdir.path()).unwrap()
    };
    let lmdb_driver = HeedBenchDatabase::new(&lmdb_env);
    preload(&lmdb_driver, &op_size, thread_count);
    benchmark(&lmdb_driver, &op_size, thread_count);
    print_data_size(&tmpdir, &lmdb_driver);
    fs::remove_dir_all(&tmpdir).unwrap();
}
