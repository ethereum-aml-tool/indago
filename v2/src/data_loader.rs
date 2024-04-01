use fxhash::FxHashSet as HashSet;
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

    pub fn load_dataset(&self, dataset: Dataset) -> HashSet<String> {
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
        let reader = BufReader::with_capacity(4096, file);
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
}
