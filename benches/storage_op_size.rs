#[derive(Debug)]
pub struct OpSize {
    pub insert_key_total_count: usize,
    pub insert_key_per_tx_count: usize,
    pub scan_total_count: usize,
    pub scan_per_tx_count: usize,
    pub iter_per_scan_count: usize,
}

impl OpSize {
    pub fn from_str(str: &str) -> Self {
        match str {
            "s" => SMALL,
            "m" => MEDIUM,
            "m2" => MEDIUM2,
            "m3" => MEDIUM3,
            "b" => BIG,
            x => panic!("must be either 's', 'm', 'm2', 'm3', or 'b', got {}", x)
        }
    }
}

//
// predefined profiles
//
const SMALL: OpSize = OpSize {
    insert_key_total_count: 1_000_000,
    insert_key_per_tx_count: 1_000,
    scan_total_count: 100_000,
    scan_per_tx_count: 100,
    iter_per_scan_count: 1_000,
};

const MEDIUM: OpSize = OpSize {
    insert_key_total_count: 10_000_000,
    insert_key_per_tx_count: 1_000,
    scan_total_count: 1_00_000,
    scan_per_tx_count: 100,
    iter_per_scan_count: 1_000,
};

const MEDIUM2: OpSize = OpSize {
    insert_key_total_count: 100_000_000,
    insert_key_per_tx_count: 1_000,
    scan_total_count: 1_000_000,
    scan_per_tx_count: 100,
    iter_per_scan_count: 1_000,
};

const MEDIUM3: OpSize = OpSize {
    insert_key_total_count: 300_000_000,
    insert_key_per_tx_count: 1_000,
    scan_total_count: 3_000_000,
    scan_per_tx_count: 100,
    iter_per_scan_count: 1_000,
};


const BIG: OpSize = OpSize {
    insert_key_total_count: 1000_000_000,
    insert_key_per_tx_count: 1_000,
    scan_total_count: 10_000_000,
    scan_per_tx_count: 100,
    iter_per_scan_count: 1_000,
};
