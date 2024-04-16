use std::io::Write;
use std::{cmp, fs};

use crate::run_data::{save_run_data_ext, RunDataExt};
use crate::{
    data_loader::{DataLoader, Trace},
    BlacklistingAlgorithm, Dataset,
};
use anyhow::Result;
use fxhash::FxHashMap as HashMap;
use sysinfo::System;

use super::Balance;

pub struct Seniority {}

impl BlacklistingAlgorithm for Seniority {
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

        let mut run_data: Vec<RunDataExt> = Vec::new();
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
            let mut taint_to_move = 0;
            if let Some(from_balance) = balances.get_mut(from_address) {
                parsed_value = trace.value().parse::<u128>().ok();
                let gas_used = trace.gas_used().parse::<u128>().unwrap_or(0);
                let value = parsed_value.unwrap_or(0);
                from_balance.total = from_balance.total.saturating_sub(value + gas_used);

                taint_to_move = cmp::min(value, from_balance.tainted);
                if from_balance.total == 0 {
                    balances.remove(from_address);
                } else {
                    from_balance.tainted = from_balance.tainted.saturating_sub(taint_to_move);
                }
            }

            if let Some(to_balance) = balances.get_mut(to_address) {
                if let Some(parsed_value) = parsed_value {
                    to_balance.total += parsed_value;
                    to_balance.tainted += taint_to_move;
                }
            } else {
                let value = parsed_value.unwrap_or(0);
                balances.insert(
                    to_address.to_string(),
                    Balance {
                        total: value,
                        tainted: taint_to_move,
                    },
                );
            }

            n_processed += 1;
            if n_processed % n_between_stats == 0 {
                run_data.push(RunDataExt::new(
                    &mut system,
                    n_processed,
                    &balances,
                    start_time.elapsed(),
                    trace.block_number().parse().unwrap(),
                ));
            }
        }
        // final run data
        run_data.push(RunDataExt::new(
            &mut system,
            n_processed,
            &balances,
            start_time.elapsed(),
            0,
        ));

        // Save the blacklisted addresses to a .csv
        let mut file = fs::File::create(format!(
            "{}/seniority-addresses-{}.csv",
            data_loader.output_dir,
            dataset.to_str()
        ))?;
        let mut lines_to_write = Vec::new();
        lines_to_write.push("address,total,tainted".to_string());
        clean_balances_without_taint(&mut balances);
        for (address, balance) in balances.iter() {
            lines_to_write.push(format!("{},{},{}", address, balance.total, balance.tainted));
            if lines_to_write.len() % 1_000_000 == 0 {
                writeln!(file, "{}", lines_to_write.join("\n")).unwrap();
                lines_to_write.clear();
            }
        }
        writeln!(file, "{}", lines_to_write.join("\n")).unwrap();
        // Save run data to a csv file
        let run_data_path = format!(
            "{}/seniority-rundata-{}.csv",
            data_loader.output_dir,
            dataset.to_str()
        );
        save_run_data_ext(run_data, run_data_path.as_str())?;

        Ok(())
    }
}

fn clean_balances_without_taint(balances: &mut HashMap<String, Balance>) {
    balances.retain(|_, balance| balance.tainted > 0);
}
