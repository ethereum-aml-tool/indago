use std::fs;
use std::io::Write;

use crate::{
    data_loader::{DataLoader, TraceColumn},
    run_data::{save_run_data, RunData},
    BlacklistingAlgorithm, Dataset,
};
use anyhow::Result;
use fxhash::FxHashMap as HashMap;
use sysinfo::System;

pub struct Haircut {}

struct Balance {
    total: u64,
    tainted: u64,
}

impl BlacklistingAlgorithm for Haircut {
    fn run(
        &self,
        data_loader: &DataLoader,
        dataset: Dataset,
        n_between_stats: usize,
    ) -> Result<()> {
        let blacklisted_addresses = data_loader.load_dataset(&dataset);

        let mut balances: HashMap<String, Balance> = HashMap::default();
        balances.extend(blacklisted_addresses.iter().map(|address| {
            (
                address.clone(),
                Balance {
                    total: u64::max_value() / 2,
                    tainted: u64::max_value() / 2,
                },
            )
        }));

        let mut run_data: Vec<RunData> = Vec::new();
        let mut system = System::new();
        let start_time = std::time::Instant::now();
        let mut n_processed = 0;

        for line in data_loader.traces_iter() {
            let line = line?;
            let parts: Vec<&str> = line.split(',').collect();

            let status = TraceColumn::Status.extract_from_parts(&parts);
            let value = TraceColumn::Value.extract_from_parts(&parts);
            if (status == "0") || value == "0" {
                continue;
            }

            let from_address = TraceColumn::FromAddress.extract_from_parts(&parts);
            let to_address = TraceColumn::ToAddress.extract_from_parts(&parts);
            let gas_used = TraceColumn::GasUsed
                .extract_from_parts(&parts)
                .parse::<u64>()
                .unwrap_or(0);
            let value = match value.parse::<u64>() {
                Ok(value) => value,
                Err(_) => {
                    eprintln!("Failed to parse value: {}", value);
                    continue;
                }
            };

            let mut tainted_percent = 0.0;
            if let Some(from_balance) = balances.get_mut(from_address) {
                tainted_percent = from_balance.tainted as f64 / from_balance.total as f64;
                from_balance.total -= value + gas_used;
                from_balance.tainted -= (value as f64 * tainted_percent) as u64;
            }

            if let Some(to_balance) = balances.get_mut(to_address) {
                to_balance.total += value;
                to_balance.tainted += (value as f64 * tainted_percent) as u64;
            } else {
                balances.insert(
                    to_address.to_string(),
                    Balance {
                        total: value,
                        tainted: (value as f64 * tainted_percent) as u64,
                    },
                );
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
        // final run data
        run_data.push(RunData::new(
            &mut system,
            n_processed,
            blacklisted_addresses.len(),
            start_time.elapsed(),
        ));

        // Save the blacklisted addresses to a txt file
        let mut file = fs::File::create(format!(
            "{}/haircut-addresses-{}.txt",
            data_loader.output_dir,
            dataset.to_str()
        ))?;
        for address in blacklisted_addresses.iter() {
            writeln!(file, "{}", address)?;
        }
        // Save run data to a csv file
        let run_data_path = format!(
            "{}/haircut-rundata-{}.csv",
            data_loader.output_dir,
            dataset.to_str()
        );
        save_run_data(run_data, run_data_path.as_str())?;

        Ok(())
    }
}
