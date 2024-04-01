use std::fs;
use std::io::Write;

use crate::{
    data_loader::{DataLoader, TraceColumn},
    run_data::RunData,
    BlacklistingAlgorithm, Dataset,
};
use anyhow::Result;
use sysinfo::System;
use thousands::Separable;

pub struct Poison {}

impl BlacklistingAlgorithm for Poison {
    fn run(
        &self,
        data_loader: &DataLoader,
        dataset: Dataset,
        n_between_stats: usize,
    ) -> Result<Vec<RunData>> {
        let mut blacklisted_addresses = data_loader.load_dataset(dataset);

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

            let status = TraceColumn::Status.extract_from_parts(&parts);
            let value = TraceColumn::Value.extract_from_parts(&parts);
            let from_address = TraceColumn::FromAddress.extract_from_parts(&parts);
            // if (status == "0") || value == "0" || whitelist.contains(from_address) {
            if (status == "0") || value == "0" {
                continue;
            }

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
                ));
            }
        }

        println!(
            "Blacklisted addresses: {}",
            blacklisted_addresses.len().separate_with_commas()
        );
        println!("Processed {} rows", n_processed.separate_with_commas());

        // Save the blacklisted addresses to a txt file
        let mut file = fs::File::create(format!(
            "{}/blacklisted-addresses.txt",
            data_loader.output_dir
        ))?;
        for address in blacklisted_addresses.iter() {
            writeln!(file, "{}", address)?;
        }

        Ok(run_data)
    }
}
