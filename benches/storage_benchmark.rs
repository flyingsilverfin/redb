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

    let mut bb = rocksdb::BlockBasedOptions::default();
    bb.set_block_cache(&rocksdb::Cache::new_lru_cache(4 * 1_024 * 1_024 * 1_024)); // TODO
    // setFilterPolicy non-existent
    bb.set_partition_filters(true);
    bb.set_index_type(rocksdb::BlockBasedIndexType::TwoLevelIndexSearch);
    bb.set_optimize_filters_for_memory(true);
    bb.set_pin_top_level_index_and_filter(true);
    bb.set_pin_l0_filter_and_index_blocks_in_cache(true);
    bb.set_cache_index_and_filter_blocks(true);
    // setCacheIndexAndFilterBlocksWithHighPriority non-existent

    let mut rocksdb_opts = rocksdb::Options::default();
    rocksdb_opts.create_if_missing(true);
    rocksdb_opts.set_max_subcompactions(thread_count.to_u32().unwrap());
    rocksdb_opts.set_max_background_jobs(thread_count.to_i32().unwrap());
    rocksdb_opts.set_enable_write_thread_adaptive_yield(true);
    rocksdb_opts.set_allow_concurrent_memtable_write(true);
    rocksdb_opts.set_block_based_table_factory(&bb);
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
