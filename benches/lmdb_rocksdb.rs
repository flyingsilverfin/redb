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

const DATA_DIR: &str = "/mnt/balanced-pd/tmp";
const SEED_DATA: usize = 1_000_000;
const KEY_SIZE: usize = 48;
const VALUE_SIZE: usize = 0;

fn main() {
    let tmpdir = TempDir::new_in(DATA_DIR).unwrap();
    dbg!("Using benchmark dir: {}", &tmpdir);

    for i in 0..SEED_DATA {
        // insert data
    }

    for i in 0..(SEED_DATA/1000) {
        // seek and iterate
    }

    fs::remove_dir_all(&tmpdir).unwrap();
}
