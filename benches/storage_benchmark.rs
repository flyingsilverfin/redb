use byte_unit::rust_decimal::prelude::ToPrimitive;
use heed::EnvFlags;
use std::env;
use std::fmt::Display;
use std::fs;
use std::path::Path;
use std::time::Instant;
use tempfile::TempDir;

mod common;
mod storage_common;
mod storage_op_size;
mod storage_step;

use common::*;
use storage_step::*;
use crate::storage_common::available_disk;
use crate::storage_op_size::OpSize;

fn main() {
    let mut args: Vec<String> = env::args().collect();
    args.pop().unwrap(); // pop '--bench'
    let tmpdir_path = args.pop().unwrap();
    let thread_count = args.pop().unwrap().parse::<usize>().unwrap();
    let op_size = OpSize::from_str(args.pop().unwrap().as_str());

    println!("op size: {:?}\nthread count: {}\ndir: {:?}", &op_size, thread_count, &tmpdir_path);

    // rocksdb_benchmark(&op_size, thread_count, &tmpdir_path);
    lmdb_benchmark(&op_size, thread_count, &tmpdir_path);
    // redb_benchmark(&op_size, thread_count, &tmpdir_path);

}

fn rocksdb_benchmark(op_size: &OpSize, thread_count: usize, tmpdir_path: &String) {
    // let tmpdir = TempDir::new_in(tmpdir_path).unwrap();
    let dir = Path::new(tmpdir_path);

    let mut rocksdb_opts = rocksdb::Options::default();
    rocksdb_opts.create_if_missing(true);
    let rocksdb_db = rocksdb::OptimisticTransactionDB::open(&rocksdb_opts, dir).unwrap();
    let rocksdb_driver = OptimisticRocksdbBenchDatabase::new(&rocksdb_db);
    preload_step(&rocksdb_driver, &op_size, thread_count);
    scan_step(&rocksdb_driver, &op_size, thread_count);
    print_data_size(&dir, &rocksdb_driver);
}

fn lmdb_benchmark(op_size: &OpSize, thread_count: usize, tmpdir_path: &String) {
    let dir = Path::new(tmpdir_path);
    let lmdb_env = unsafe {
        let mut options = heed::EnvOpenOptions::new();
        options.map_size(available_disk() as usize); // 125GB (size of the benchmark VM memory)
        options.flags(EnvFlags::NO_TLS | EnvFlags::NO_SYNC | EnvFlags::NO_READ_AHEAD);
        // options.open(tmpdir.path()).unwrap()
        options.open(dir).unwrap()
    };
    let lmdb_driver = HeedBenchDatabase::new(&lmdb_env);
    preload_step(&lmdb_driver, &op_size, thread_count);
    scan_step(&lmdb_driver, &op_size, thread_count);
    print_data_size(&dir, &lmdb_driver);
}


fn redb_benchmark(op_size: &OpSize, thread_count: usize, path: &String) {
    let dir = Path::new(path).join("redb");
    let mut db = redb::Database::builder()
        .set_cache_size(4 * 1024 * 1024 * 1024)
        .create(&dir)
        .unwrap();
    let table = RedbBenchDatabase::new(&db);
    preload_step(&table, &op_size, thread_count);
    println!("Reading without compacting...");
    scan_step(&table, &op_size, thread_count);

    let start = Instant::now();
    db.compact().unwrap();
    let end = Instant::now();
    let duration = end - start;
    println!("redb: Compacted in {}ms", duration.as_millis());

    let table = RedbBenchDatabase::new(&db);
    println!("Reading after compacting...");
    scan_step(&table, &op_size, thread_count);
    print_data_size(&dir, &table);
}
