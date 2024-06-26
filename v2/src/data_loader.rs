use fxhash::FxHashSet as HashSet;
use std::io::Write;
use std::{
    fs::{self, File},
    io::{BufRead, BufReader, Lines},
};
use thousands::Separable;

use crate::Dataset;

#[derive(Debug, Clone, Copy)]
pub enum TraceColumn {
    BlockNumber = 0,
    TransactionIndex = 1,
    FromAddress = 2,
    ToAddress = 3,
    Value = 4,
    GasUsed = 5,
    Status = 6,
}

impl TraceColumn {
    pub fn extract_from_parts<'a>(&'a self, line_parts: &[&'a str]) -> &str {
        return line_parts.get(*self as usize).unwrap_or_else(|| {
            panic!(
                "Could not extract column {} from line parts: {:?}",
                *self as usize, line_parts
            )
        });
    }
}

pub struct Trace<'a>(&'a [&'a str]);

impl<'a> Trace<'a> {
    pub fn from_parts(parts: &'a [&'a str]) -> Self {
        Self(parts)
    }

    pub fn is_valid(&self) -> bool {
        match self.0.len() {
            6 => true,
            7 => self.status() != "0" && self.value() != "0",
            _ => false,
        }
    }

    pub fn block_number(&self) -> &str {
        TraceColumn::BlockNumber.extract_from_parts(self.0)
    }

    pub fn transaction_index(&self) -> &str {
        TraceColumn::TransactionIndex.extract_from_parts(self.0)
    }

    #[allow(clippy::wrong_self_convention)]
    pub fn from_address(&self) -> &str {
        TraceColumn::FromAddress.extract_from_parts(self.0)
    }

    pub fn to_address(&self) -> &str {
        TraceColumn::ToAddress.extract_from_parts(self.0)
    }

    pub fn value(&self) -> &str {
        TraceColumn::Value.extract_from_parts(self.0)
    }

    pub fn gas_used(&self) -> &str {
        TraceColumn::GasUsed.extract_from_parts(self.0)
    }

    pub fn status(&self) -> &str {
        TraceColumn::Status.extract_from_parts(self.0)
    }

    pub fn is_miner_reward(&self) -> bool {
        self.0.len() == 6
    }

    pub fn miner_address(&self) -> &str {
        self.0[2]
    }

    pub fn miner_reward(&self) -> &str {
        self.0[3]
    }
}

pub struct DataLoader {
    known_addresses_csv: String,
    tornado_csv: String,
    traces_csv: String,
    pub output_dir: String,
}

impl DataLoader {
    pub fn new(
        known_addresses_csv: String,
        tornado_csv: String,
        traces_csv: String,
        output_dir: String,
    ) -> Self {
        for file in [&known_addresses_csv, &tornado_csv, &traces_csv, &output_dir] {
            if fs::metadata(file).is_err() {
                panic!("File {} does not exist", file);
            }
        }

        Self {
            known_addresses_csv,
            tornado_csv,
            traces_csv,
            output_dir,
        }
    }

    pub fn load_known_addresses(&self) -> HashSet<String> {
        let known_addresses = fs::read_to_string(&self.known_addresses_csv).unwrap();
        known_addresses
            .lines()
            .skip(1)
            .filter(|line| line.split(',').nth(4).unwrap() == "0")
            .map(|line| line.split(',').next().unwrap().to_string())
            .collect()
    }

    pub fn load_whitelisted_addresses(&self) -> HashSet<String> {
        let known_addresses = fs::read_to_string(&self.known_addresses_csv).unwrap();
        known_addresses
            .lines()
            .skip(1)
            .filter(|line| line.split(',').nth(4).unwrap() == "1")
            .map(|line| line.split(',').next().unwrap().to_string())
            .collect()
    }

    pub fn load_tornado_addresses(&self) -> HashSet<String> {
        let tornado_content = fs::read_to_string(&self.tornado_csv).unwrap();
        tornado_content
            .lines()
            .skip(1)
            .map(|line| line.split(',').next().unwrap().to_string())
            .collect()
    }

    pub fn load_dataset(&self, dataset: &Dataset) -> HashSet<String> {
        match dataset {
            Dataset::KnownAddresses => self.load_known_addresses(),
            Dataset::Tornado => self.load_tornado_addresses(),
            Dataset::Combined => self
                .load_known_addresses()
                .into_iter()
                .chain(self.load_tornado_addresses())
                .collect(),
        }
    }

    pub fn traces_iter(&self) -> Lines<BufReader<File>> {
        let file = fs::File::open(&self.traces_csv).unwrap();
        let reader = BufReader::with_capacity(8192, file);
        reader.lines()
    }

    pub fn n_unique_addresses(&self) -> usize {
        let mut addresses = HashSet::default();
        for line in self.traces_iter() {
            let line = line.unwrap();
            let parts: Vec<&str> = line.split(',').collect();
            addresses.insert(
                TraceColumn::FromAddress
                    .extract_from_parts(&parts)
                    .to_string(),
            );
            addresses.insert(
                TraceColumn::ToAddress
                    .extract_from_parts(&parts)
                    .to_string(),
            );
        }

        addresses.len()
    }

    pub fn scientific_notation_to_int(&self) {
        let output_file = format!("{}/traces_no_sciene.csv", self.output_dir);
        let mut file = File::create(output_file).unwrap();

        let mut lines_to_write = Vec::new();
        lines_to_write.push(
            "block_number,transaction_index,from_address,to_address,value,gas_used,status"
                .to_string(),
        );

        for (n_processed, line) in self.traces_iter().skip(1).enumerate() {
            if lines_to_write.len() % 10_000_000 == 0 {
                writeln!(file, "{}", lines_to_write.join("\n")).unwrap();
                lines_to_write.clear();
                println!(
                    "Processed {} lines, {}%",
                    n_processed.separate_with_commas(),
                    (n_processed as f32 / 8_035_989_413.0 * 100.0).round()
                );
            }

            let line = line.unwrap();
            let mut parts: Vec<String> = line.split(',').map(|s| s.to_string()).collect();
            let is_miner_reward = parts.len() < 7;

            parts
                .iter_mut()
                .skip(if is_miner_reward { 3 } else { 4 })
                .for_each(|part| {
                    if part.contains('e') {
                        let (mantissa, exponent) = part.split_once('e').unwrap();
                        let mantissa = mantissa.parse::<f64>().unwrap();
                        let exponent = exponent.parse::<i32>().unwrap();
                        let new_value = mantissa * 10_f64.powi(exponent);
                        *part = new_value.to_string();
                    } else if part.contains('.') {
                        *part = part.split('.').next().unwrap().to_string();
                    }
                });

            lines_to_write.push(parts.join(","));
        }

        writeln!(file, "{}", lines_to_write.join("\n")).unwrap();
    }

    // have to handle that trace address can be empty, in quotes and contain commas
    // ,
    // "0,1,2",
    // 2,
    // pub fn remove_trace_address_column(&self) {
    //     let output_file = format!("{}/traces_no_trace_address.csv", self.output_dir);
    //     let mut file = File::create(output_file).unwrap();

    //     let mut lines_to_write = Vec::new();
    //     lines_to_write.push(
    //         "block_number,transaction_index,from_address,to_address,value,gas_used,status"
    //             .to_string(),
    //     );

    //     for line in self.traces_iter().skip(1) {
    //         if lines_to_write.len() % 1_000_000 == 0 {
    //             writeln!(file, "{}", lines_to_write.join("\n")).unwrap();
    //             lines_to_write.clear();
    //         }

    //         let line = line.unwrap();

    //         let mut current_column = 0;
    //         let mut parts = line.split(',').peekable();
    //         let mut new_line = String::new();
    //         while let Some(part) = parts.next() {
    //             if current_column == TraceColumn::TraceAddress as usize {
    //                 while parts
    //                     .peek()
    //                     .map_or(false, |next_part| !next_part.starts_with("0x"))
    //                 {
    //                     parts.next(); // skip over continued parts of `trace_address`
    //                 }
    //                 current_column += 1;
    //                 continue;
    //             } else if current_column == TraceColumn::FromAddress as usize {
    //                 // write all the remaining parts
    //                 let remaining_parts =
    //                     format!("{},{}", part, parts.collect::<Vec<&str>>().join(","));
    //                 new_line.push_str(&remaining_parts);
    //                 break;
    //             }

    //             new_line.push_str(&format!("{},", part));
    //             current_column += 1;
    //         }

    //         lines_to_write.push(new_line);
    //     }

    //     writeln!(file, "{}", lines_to_write.join("\n")).unwrap();
    // }
}
