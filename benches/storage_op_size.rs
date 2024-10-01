#[derive(Debug)]
pub struct OpSize {
    pub preload_key_count: usize,
    pub preload_key_per_tx_count: usize,
    pub benchmark_op_count: usize,
    pub benchmark_op_per_tx_count: usize,
    pub benchmark_iter_per_op_count: usize,
}

impl OpSize {
    pub fn from_str(str: &str) -> Self {
        match str {
            "s" => SMALL,
            "m" => MEDIUM,
            "b" => BIG,
            x => panic!("must be either 's', 'm', or 'b', got {}", x)
        }
    }
}




//
// predefined profiles
//
const SMALL: OpSize = OpSize {
    preload_key_count: 1_000_000,
    preload_key_per_tx_count: 1_000,
    benchmark_op_count: 100_000,
    benchmark_op_per_tx_count: 100,
    benchmark_iter_per_op_count: 1_000,
};

const MEDIUM: OpSize = OpSize {
    preload_key_count: 10_000_000,
    preload_key_per_tx_count: 1_000,
    benchmark_op_count: 1_00_000,
    benchmark_op_per_tx_count: 100,
    benchmark_iter_per_op_count: 1_000,
};

const BIG: OpSize = OpSize {
    preload_key_count: 1000_000_000,
    preload_key_per_tx_count: 1_000,
    benchmark_op_count: 10_000_000,
    benchmark_op_per_tx_count: 100,
    benchmark_iter_per_op_count: 1_000,
};

