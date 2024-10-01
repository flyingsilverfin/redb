use std::mem::size_of;
use std::path::Path;

const KEY_SIZE: usize = 48;

pub fn create_rng(seed: u64) -> fastrand::Rng {
    fastrand::Rng::with_seed(seed)
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
