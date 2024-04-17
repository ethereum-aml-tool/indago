use anyhow::Result;
use fxhash::FxHashMap as HashMap;
use std::fmt;
use std::io::Write;
use std::{fs, time::Duration};
use thousands::Separable;

use serde::{Deserialize, Serialize};
use sysinfo::System;

use crate::algos::Balance;

#[derive(Serialize, Deserialize)]
pub struct RunData {
    rows_processed: usize,
    n_blacklisted: usize,
    processed_after: Duration,
    current_block: u64,
    ram_usage_gb: f32,
}

#[derive(Serialize, Deserialize)]
pub struct RunDataExt {
    pub rows_processed: usize,
    pub n_blacklisted: usize,
    pub n_blacklisted_0_01: usize,
    pub n_blacklisted_0_1: usize,
    pub n_blacklisted_1: usize,
    pub n_blacklisted_10: usize,
    pub n_blacklisted_100: usize,
    pub processed_after: Duration,
    pub current_block: u64,
    pub ram_usage_gb: f32,
}

impl RunData {
    pub fn new(
        system: &mut System,
        rows_processed: usize,
        n_blacklisted: usize,
        processed_after: Duration,
        current_block: u64,
    ) -> Self {
        system.refresh_memory();
        let ram_usage_gb = system.used_memory() as f32 / 1024.0 / 1024.0 / 1024.0;

        let rundata = Self {
            rows_processed,
            n_blacklisted,
            processed_after,
            current_block,
            ram_usage_gb,
        };
        println!("\n{}", rundata);

        rundata
    }
}

impl RunDataExt {
    pub fn new(
        system: &mut System,
        rows_processed: usize,
        balances: &HashMap<String, Balance>,
        processed_after: Duration,
        current_block: u64,
    ) -> Self {
        system.refresh_memory();
        let ram_usage_gb = system.used_memory() as f32 / 1024.0 / 1024.0 / 1024.0;

        let mut n_blacklisted = 0;
        let mut n_blacklisted_0_01 = 0;
        let mut n_blacklisted_0_1 = 0;
        let mut n_blacklisted_1 = 0;
        let mut n_blacklisted_10 = 0;
        let mut n_blacklisted_100 = 0;
        for balance in balances.values() {
            if balance.tainted > 0 {
                n_blacklisted += 1;

                // 0.01, 0.1, 1, 10, and 100 ETH
                if balance.tainted <= eth_to_wei(0.01) {
                    n_blacklisted_0_01 += 1;
                } else if balance.tainted <= eth_to_wei(0.1) {
                    n_blacklisted_0_1 += 1;
                } else if balance.tainted <= eth_to_wei(1.0) {
                    n_blacklisted_1 += 1;
                } else if balance.tainted <= eth_to_wei(10.0) {
                    n_blacklisted_10 += 1;
                } else if balance.tainted <= eth_to_wei(100.0) {
                    n_blacklisted_100 += 1;
                }
            }
        }

        let rundata = Self {
            rows_processed,
            n_blacklisted,
            n_blacklisted_0_01,
            n_blacklisted_0_1,
            n_blacklisted_1,
            n_blacklisted_10,
            n_blacklisted_100,
            processed_after,
            current_block,
            ram_usage_gb,
        };
        println!("\n{}", rundata);

        rundata
    }
}

impl fmt::Display for RunData {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let percentage_complete = self.rows_processed as f32 / 1_754_936_568.0;
        let estimated_remaining = 1.0 / percentage_complete * self.processed_after.as_secs_f32();

        write!(
            f,
            "Rows processed: {}\nBlacklisted addresses: {}\nProcessed after: {:.2} minutes\nBlock: {}\nRAM usage: {:.2} GB\nProgress: {:.2}%, estimated remaining: {:.2} minutes\n",
            self.rows_processed.separate_with_commas(),
            self.n_blacklisted.separate_with_commas(),
            self.processed_after.as_secs_f32() / 60.0,
            self.current_block,
            self.ram_usage_gb,
            percentage_complete * 100.0,
            estimated_remaining / 60.0
        )
    }
}

impl fmt::Display for RunDataExt {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let percentage_complete = self.rows_processed as f32 / 1_754_936_568.0;
        let estimated_remaining = 1.0 / percentage_complete * self.processed_after.as_secs_f32();

        write!(
            f,
            "Rows processed: {}\nBlacklisted addresses: {}\nProcessed after: {:.2} minutes\nBlock: {}\nRAM usage: {:.2} GB\nProgress: {:.2}%, estimated remaining: {:.2} minutes\n",
            self.rows_processed.separate_with_commas(),
            self.n_blacklisted.separate_with_commas(),
            self.processed_after.as_secs_f32() / 60.0,
            self.current_block,
            self.ram_usage_gb,
            percentage_complete * 100.0,
            estimated_remaining / 60.0
        )
    }
}

pub fn save_run_data(run_data: Vec<RunData>, path: &str) -> Result<()> {
    let mut file = fs::File::create(path)?;

    writeln!(
        file,
        "rows_processed,n_blacklisted,processed_after_mins,current_block,ram_usage_gb"
    )?;
    for data in run_data {
        writeln!(
            file,
            "{},{},{:.2},{},{:.2}",
            data.rows_processed,
            data.n_blacklisted,
            data.processed_after.as_secs_f32() / 60.0,
            data.current_block,
            data.ram_usage_gb
        )?;
    }

    Ok(())
}

pub fn save_run_data_ext(run_data: Vec<RunDataExt>, path: &str) -> Result<()> {
    let mut file = fs::File::create(path)?;

    writeln!(
        file,
        "rows_processed,n_blacklisted,n_blacklisted_0_01,n_blacklisted_0_1,n_blacklisted_1,n_blacklisted_10,n_blacklisted_100,processed_after_mins,current_block,ram_usage_gb"
    )?;
    for data in run_data {
        writeln!(
            file,
            "{},{},{},{},{},{},{},{:.2},{},{:.2}",
            data.rows_processed,
            data.n_blacklisted,
            data.n_blacklisted_0_01,
            data.n_blacklisted_0_1,
            data.n_blacklisted_1,
            data.n_blacklisted_10,
            data.n_blacklisted_100,
            data.processed_after.as_secs_f32() / 60.0,
            data.current_block,
            data.ram_usage_gb
        )?;
    }

    Ok(())
}

fn eth_to_wei(eth: f64) -> u128 {
    (eth * 1_000_000_000_000_000_000.0) as u128
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_eth_to_wei() {
        assert_eq!(eth_to_wei(1.0), 1_000_000_000_000_000_000);
        assert_eq!(eth_to_wei(0.1), 100_000_000_000_000_000);
        assert_eq!(eth_to_wei(0.01), 10_000_000_000_000_000);
        assert_eq!(eth_to_wei(0.001), 1_000_000_000_000_000);
        assert_eq!(eth_to_wei(0.0001), 100_000_000_000_000);
        assert_eq!(eth_to_wei(0.00001), 10_000_000_000_000);
        assert_eq!(eth_to_wei(0.000001), 1_000_000_000_000);
        assert_eq!(eth_to_wei(0.0000001), 100_000_000_000);
        assert_eq!(eth_to_wei(0.00000001), 10_000_000_000);
        assert_eq!(eth_to_wei(0.000000001), 1_000_000_000);
        assert_eq!(eth_to_wei(0.0000000001), 100_000_000);
        assert_eq!(eth_to_wei(0.00000000001), 10_000_000);
        assert_eq!(eth_to_wei(0.000000000001), 1_000_000);
        assert_eq!(eth_to_wei(0.0000000000001), 100_000);
        assert_eq!(eth_to_wei(0.00000000000001), 10_000);
        assert_eq!(eth_to_wei(0.000000000000001), 1_000);
        assert_eq!(eth_to_wei(0.0000000000000001), 100);
        assert_eq!(eth_to_wei(0.00000000000000001), 10);
        assert_eq!(eth_to_wei(0.000000000000000001), 1);
    }
}
