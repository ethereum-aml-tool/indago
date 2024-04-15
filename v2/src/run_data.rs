use anyhow::Result;
use std::fmt;
use std::io::Write;
use std::{fs, time::Duration};
use thousands::Separable;

use serde::{Deserialize, Serialize};
use sysinfo::System;

#[derive(Serialize, Deserialize)]
pub struct RunData {
    rows_processed: usize,
    n_blacklisted: usize,
    processed_after: Duration,
    current_block: u64,
    ram_usage_gb: f32,
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
