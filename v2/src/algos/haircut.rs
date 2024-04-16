use std::fs;
use std::io::Write;

use crate::{
    data_loader::{DataLoader, Trace},
    run_data::{save_run_data, RunData},
    BlacklistingAlgorithm, Dataset,
};
use anyhow::Result;
use fxhash::FxHashMap as HashMap;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use sysinfo::System;

pub struct Haircut {}

struct Balance {
    total: u128,
    tainted: u128,
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
                    total: u128::max_value() / 2,
                    tainted: u128::max_value() / 2,
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
            let trace = Trace::from_parts(&parts);
            if !trace.is_valid() {
                continue;
            }

            if trace.is_miner_reward() {
                balances
                    .entry(trace.miner_address().to_string())
                    .and_modify(|balance| {
                        balance.total += trace.miner_reward().parse::<u128>().unwrap_or(0);
                    })
                    .or_insert_with(|| Balance {
                        total: trace.miner_reward().parse::<u128>().unwrap_or(0),
                        tainted: 0,
                    });
                continue;
            }

            let from_address = trace.from_address();
            let to_address = trace.to_address();

            let mut parsed_value: Option<u128> = None;
            let mut tainted_percent = 0.0;
            if let Some(from_balance) = balances.get_mut(from_address) {
                tainted_percent = from_balance.tainted as f64 / from_balance.total as f64;

                let gas_used = trace.gas_used().parse::<u128>().unwrap_or(0);
                parsed_value = trace.value().parse::<u128>().ok();
                let value = parsed_value.unwrap_or(0);
                from_balance.total = from_balance.total.saturating_sub(value + gas_used);

                if from_balance.total == 0 {
                    balances.remove(from_address);
                } else {
                    from_balance.tainted = from_balance
                        .tainted
                        .saturating_sub((value as f64 * tainted_percent) as u128);
                }
            }

            if let Some(to_balance) = balances.get_mut(to_address) {
                if let Some(parsed_value) = parsed_value {
                    to_balance.total += parsed_value;
                    to_balance.tainted += (parsed_value as f64 * tainted_percent) as u128;
                }
            } else {
                let value = parsed_value.unwrap_or(0);
                balances.insert(
                    to_address.to_string(),
                    Balance {
                        total: value,
                        tainted: (value as f64 * tainted_percent) as u128,
                    },
                );
            }

            n_processed += 1;
            if n_processed % n_between_stats == 0 {
                run_data.push(RunData::new(
                    &mut system,
                    n_processed,
                    n_tainted_addresses(&balances) - blacklisted_addresses.len(),
                    start_time.elapsed(),
                    trace.block_number().parse().unwrap(),
                ));
            }
        }
        // final run data
        run_data.push(RunData::new(
            &mut system,
            n_processed,
            n_tainted_addresses(&balances) - blacklisted_addresses.len(),
            start_time.elapsed(),
            0,
        ));

        // Save the blacklisted addresses to a .csv
        let mut file = fs::File::create(format!(
            "{}/haircut-addresses-{}.csv",
            data_loader.output_dir,
            dataset.to_str()
        ))?;
        writeln!(file, "address,total,tainted")?;
        clean_balances_without_taint(&mut balances);
        for (address, balance) in balances.iter() {
            writeln!(file, "{},{},{}", address, balance.total, balance.tainted)?;
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

fn n_tainted_addresses(balances: &HashMap<String, Balance>) -> usize {
    balances
        .par_iter()
        .filter(|(_, balance)| balance.tainted > 0)
        .count()
}

fn clean_balances_without_taint(balances: &mut HashMap<String, Balance>) {
    balances.retain(|_, balance| balance.tainted > 0);
}
