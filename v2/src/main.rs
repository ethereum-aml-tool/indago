use anyhow::Result;
use fxhash::FxHashSet as HashSet;
use polars::datatypes::DataType;
use polars::lazy::dsl::col;
use polars::lazy::frame::{LazyCsvReader, LazyFileListReader, LazyFrame};
use polars::prelude::Schema;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::sync::Arc;

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

fn main() -> Result<()> {
    let blacklist_content = fs::read_to_string("../data/known-addresses.csv")?;
    let mut blacklisted_addresses: HashSet<String> = blacklist_content
        .lines()
        .filter(|line| line.split(',').nth(4).unwrap() == "0")
        .map(|line| line.split(',').next().unwrap().to_string())
        .collect();

    println!(
        "Initially blacklisted addresses: {}",
        blacklisted_addresses.len()
    );

    let file = fs::File::open("../data/raw/traces/traces-sorted.csv")?;
    let reader = BufReader::with_capacity(4096, file);
    for line in reader.lines() {
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

fn with_polars() -> Result<HashSet<String>> {
    let blacklist_content = fs::read_to_string("../data/known-addresses.csv")?;
    let mut blacklisted_addresses: HashSet<String> = blacklist_content
        .lines()
        .filter(|line| line.split(',').nth(4).unwrap() == "0")
        .map(|line| line.split(',').next().unwrap().to_string())
        .collect();

    let mut schema = Schema::new();
    schema.with_column("value".into(), DataType::Decimal(None, None));

    let df = LazyCsvReader::new("../data/raw/traces/traces-sorted.csv")
        .has_header(true)
        .with_dtype_overwrite(Some(&schema))
        .finish()?;

    let valid_rows = df
        .filter(col("status").eq(1))
        .filter(col("value").gt(0))
        .collect()?;

    for row in valid_rows.iter() {
        let from_address = row.get(3).unwrap().to_string();
        let to_address = row.get(4).unwrap().to_string();
        if blacklisted_addresses.contains(&from_address) {
            blacklisted_addresses.insert(to_address);
        }
    }

    println!("Blacklisted addresses: {}", blacklisted_addresses.len());

    Ok(blacklisted_addresses)
}
