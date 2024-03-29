use anyhow::Result;
use fxhash::FxHashSet as HashSet;
use std::{
    fs::{self, File},
    io::{BufRead, BufReader, Lines},
};

pub struct DataLoader {
    known_addresses_csv: String,
    tornado_csv: String,
    traces_csv: String,
    output_csv: String,
}

impl DataLoader {
    pub fn new(
        known_addresses_csv: String,
        tornado_csv: String,
        traces_csv: String,
        output_csv: String,
    ) -> Self {
        for file in [&known_addresses_csv, &tornado_csv, &traces_csv, &output_csv] {
            if fs::metadata(file).is_err() {
                panic!("File {} does not exist", file);
            }
        }

        Self {
            known_addresses_csv,
            tornado_csv,
            traces_csv,
            output_csv,
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

    pub fn traces_iter(&self) -> Lines<BufReader<File>> {
        let file = fs::File::open(&self.traces_csv).unwrap();
        let reader = BufReader::with_capacity(4096, file);
        reader.lines()
    }
}
