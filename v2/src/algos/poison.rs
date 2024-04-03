use std::fs;
use std::io::Write;

use crate::{
    data_loader::{DataLoader, TraceColumn},
    run_data::{save_run_data, RunData},
    BlacklistingAlgorithm, Dataset,
};
use anyhow::Result;
use sysinfo::System;

pub struct Poison {}

impl BlacklistingAlgorithm for Poison {
    fn run(
        &self,
        data_loader: &DataLoader,
        dataset: Dataset,
        n_between_stats: usize,
    ) -> Result<()> {
        let mut blacklisted_addresses = data_loader.load_dataset(&dataset);

        println!(
            "Initially blacklisted addresses: {}",
            blacklisted_addresses.len()
        );

        let mut run_data: Vec<RunData> = Vec::new();
        let mut system = System::new();
        let start_time = std::time::Instant::now();
        let mut n_processed = 0;

        // let whitelist = data_loader.load_whitelisted_addresses();
        for line in data_loader.traces_iter() {
            let line = line?;
            let parts: Vec<&str> = line.split(',').collect();

            let is_miner_reward = parts.len() < 7;
            if is_miner_reward {
                continue;
            }

            let status = TraceColumn::Status.extract_from_parts(&parts);
            let value = TraceColumn::Value.extract_from_parts(&parts);
            // if (status == "0") || value == "0" || whitelist.contains(from_address) {
            if (status == "0") || value == "0" {
                continue;
            }

            let from_address = TraceColumn::FromAddress.extract_from_parts(&parts);
            let to_address = TraceColumn::ToAddress.extract_from_parts(&parts);
            if blacklisted_addresses.contains(from_address) {
                blacklisted_addresses.insert(to_address.to_string());
            }

            n_processed += 1;
            if n_processed % n_between_stats == 0 {
                run_data.push(RunData::new(
                    &mut system,
                    n_processed,
                    blacklisted_addresses.len(),
                    start_time.elapsed(),
                    TraceColumn::BlockNumber
                        .extract_from_parts(&parts)
                        .parse()
                        .unwrap(),
                ));
            }
        }
        // final run data
        run_data.push(RunData::new(
            &mut system,
            n_processed,
            blacklisted_addresses.len(),
            start_time.elapsed(),
            0,
        ));

        // Save the blacklisted addresses to a txt file
        let mut file = fs::File::create(format!(
            "{}/poison-addresses-{}.txt",
            data_loader.output_dir,
            dataset.to_str()
        ))?;
        for address in blacklisted_addresses.iter() {
            writeln!(file, "{}", address)?;
        }
        // Save run data to a csv file
        let run_data_path = format!(
            "{}/poison-rundata-{}.csv",
            data_loader.output_dir,
            dataset.to_str()
        );
        save_run_data(run_data, run_data_path.as_str())?;

        Ok(())
    }
}
