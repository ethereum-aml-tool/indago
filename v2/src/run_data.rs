use anyhow::Result;
use std::io::Write;
use std::{fs, time::Duration};

use serde::{Deserialize, Serialize};
use sysinfo::System;

#[derive(Serialize, Deserialize)]
pub struct RunData {
    rows_processed: usize,
    n_blacklisted: usize,
    processed_after: Duration,
    ram_usage_gb: f32,
}

impl RunData {
    pub fn new(
        system: &mut System,
        rows_processed: usize,
        n_blacklisted: usize,
        processed_after: Duration,
    ) -> Self {
        system.refresh_memory();
        let ram_usage_gb = system.used_memory() as f32 / 1024.0 / 1024.0 / 1024.0;

        Self {
            rows_processed,
            n_blacklisted,
            processed_after,
            ram_usage_gb,
        }
    }
}

pub fn save_run_data(run_data: Vec<RunData>, path: &str) -> Result<()> {
    let mut file = fs::File::create(path)?;

    writeln!(
        file,
        "rows_processed,n_blacklisted,processed_after,ram_usage_gb"
    )?;
    for data in run_data {
        writeln!(
            file,
            "{},{},{},{}",
            data.rows_processed,
            data.n_blacklisted,
            data.processed_after.as_secs_f32() / 60.0,
            data.ram_usage_gb
        )?;
    }

    Ok(())
}
