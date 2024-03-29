use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;

use crate::data_loader::DataLoader;

mod data_loader;

#[derive(Serialize, Deserialize)]
struct RunData {
    chunk: usize,
    rows_processed: usize,
    n_blacklisted: usize,
    max_block: u64,
    processed_after: String,
    ram_usage_gb: f32,
}

// block_number,transaction_index,trace_address,from_address,to_address,value,gas_used,status
// 19397712,2,"0,1,2",0x7a250d5630b4cf539739df2c5dacb4c659f2488d,0xa11243ac5a171a86ba64a2e9c3683ae1ffb58659,0,642.0,1
// 19397712,95,"1,0,3",0x74de5d4fcbf63e00296fd95d33236b9794016631,0xbb3d7f42c58abd83616ad7c8c72473ee46df2678,0,962.0,0

// address,name,account_type,entity,legitimacy,tags
// 0x8ab7404063ec4dbcfd4598215992dc3f8ec853d7, Akropolis (AKRO),contract,defi,1,defi
// 0x1c74cff0376fb4031cd7492cd6db2d66c3f2c6b9, bZx Protocol Token (BZRX),contract,defi,1,token contract

const TRACES_CSV: &str = "../data/raw/traces/traces-sorted.csv";
const KNOWN_ADDRESSES_CSV: &str = "../data/known-addresses.csv";
const TORNADO_CSV: &str = "../data/tornado.csv";
const OUTPUT_CSV: &str = "../data/tmp/blacklisted-addresses.txt";

fn main() -> Result<()> {
    let data_loader = DataLoader::new(
        KNOWN_ADDRESSES_CSV.to_string(),
        TORNADO_CSV.to_string(),
        TRACES_CSV.to_string(),
        OUTPUT_CSV.to_string(),
    );

    let mut blacklisted_addresses = data_loader.load_known_addresses();
    println!(
        "Initially blacklisted addresses: {}",
        blacklisted_addresses.len()
    );

    for line in data_loader.traces_iter() {
        let line = line?;
        let parts: Vec<&str> = line.split(',').collect();
        if (parts[7] == "0") || (parts[5] == "0") {
            continue;
        }
        let from_address = parts[3];
        let to_address = parts[4];
        if blacklisted_addresses.contains(from_address) {
            blacklisted_addresses.insert(to_address.to_string());
        }
    }

    println!("Blacklisted addresses: {}", blacklisted_addresses.len());

    // Save the blacklisted addresses to a txt file
    let mut file = fs::File::create("../data/tmp/blacklisted-addresses.txt")?;
    for address in blacklisted_addresses.iter() {
        writeln!(file, "{}", address)?;
    }

    Ok(())
}
