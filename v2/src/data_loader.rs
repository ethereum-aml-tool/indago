use fxhash::FxHashSet as HashSet;
use std::io::Write;
use std::{
    fs::{self, File},
    io::{BufRead, BufReader, Lines},
};

use crate::Dataset;

#[derive(Debug, Clone, Copy)]
pub enum TraceColumn {
    BlockNumber = 0,
    TransactionIndex = 1,
    TraceAddress = 2,
    FromAddress = 3,
    ToAddress = 4,
    Value = 5,
    GasUsed = 6,
    Status = 7,
}

impl TraceColumn {
    pub fn extract_from_parts<'a>(&'a self, line_parts: &[&'a str]) -> &str {
        line_parts[*self as usize]
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

    // have to handle that trace address can be empty, in quotes and contain commas
    // ,
    // "0,1,2",
    // 2,
    pub fn remove_trace_address_column(&self) {
        let output_file = format!("{}/traces_no_trace_address.csv", self.output_dir);
        let mut file = File::create(output_file).unwrap();

        writeln!(
            file,
            "block_number,transaction_index,from_address,to_address,value,gas_used,status"
        )
        .unwrap();
        for line in self.traces_iter().skip(1) {
            let line = line.unwrap();

            let mut current_column = 0;
            let mut parts = line.split(',').peekable();
            while let Some(part) = parts.next() {
                if current_column == TraceColumn::TraceAddress as usize {
                    while parts
                        .peek()
                        .map_or(false, |next_part| !next_part.starts_with("0x"))
                    {
                        parts.next(); // Skip over continued parts of `trace_address`
                    }
                }

                write!(file, "{}", part).unwrap();
                if current_column < TraceColumn::Status as usize {
                    write!(file, ",").unwrap();
                }

                current_column += 1;
                if current_column > TraceColumn::Status as usize {
                    writeln!(file).unwrap();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const OUTPUT_DIR: &str = "/home/ponbac/dev/indago/data/tmp";
    const TRACES_CSV: &str = "/home/ponbac/dev/indago/data/raw/test-traces.csv";
    const KNOWN_ADDRESSES_CSV: &str = "/home/ponbac/dev/indago/data/known-addresses.csv";
    const TORNADO_CSV: &str = "/home/ponbac/dev/indago/data/tornado.csv";

    #[test]
    fn remove_trace_address_column() {
        let data_loader = DataLoader::new(
            KNOWN_ADDRESSES_CSV.to_string(),
            TORNADO_CSV.to_string(),
            TRACES_CSV.to_string(),
            OUTPUT_DIR.to_string(),
        );

        data_loader.remove_trace_address_column();
    }
}
