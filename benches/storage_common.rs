use std::mem::size_of;
use std::path::Path;

pub const PREFIX_SIZE: usize = 16;
const KEY_SIZE: usize = 48;

pub fn create_rng() -> fastrand::Rng {
    // fastrand::Rng::with_seed(seed)
    fastrand::Rng::new()
}

pub fn gen_prefix(rng: &mut fastrand::Rng) -> [u8; PREFIX_SIZE]  {
    let mut prefix = [0u8; PREFIX_SIZE];
    fill_slice(&mut prefix, rng);
    prefix
}

pub fn gen_key(rng: &mut fastrand::Rng) -> [u8; KEY_SIZE] {
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

pub fn database_size(path: &Path) -> u64 {
    let mut size = 0u64;
    for result in walkdir::WalkDir::new(path) {
        let entry = result.unwrap();
        size += entry.metadata().unwrap().len();
    }
    size
}

use sysinfo::{
    Components, Disks, Networks, System,
};

pub fn available_disk() -> u64 {
    let mut sys = System::new_all();
    sys.refresh_all();
    let disks = Disks::new_with_refreshed_list();
    let disk = &disks.list()[0];
    let bytes = disk.available_space();

    // round to 16k multiple for convenience
    const KB_16: u64 = 4096 * 4;
    let multiples = bytes / KB_16;
    multiples * KB_16
}
