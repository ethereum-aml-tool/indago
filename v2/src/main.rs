use algos::{Haircut, Poison, Seniority};
use anyhow::Result;
use clap::{Parser, ValueEnum};
use data_loader::DataLoader;

mod algos;
mod data_loader;
mod run_data;

#[derive(Debug, Clone, ValueEnum)]
enum Dataset {
    Tornado,
    KnownAddresses,
    Combined,
}

impl Dataset {
    fn to_str(&self) -> &str {
        match self {
            Dataset::Tornado => "tornado",
            Dataset::KnownAddresses => "known_addresses",
            Dataset::Combined => "combined",
        }
    }
}

#[derive(Debug, Clone, ValueEnum)]
enum Algorithm {
    Poison,
    Haircut,
    Seniority,
    Fifo,
}

trait BlacklistingAlgorithm {
    fn run(&self, data_loader: &DataLoader, dataset: Dataset, n_between_stats: usize)
        -> Result<()>;
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, value_enum)]
    algorithm: Algorithm,

    #[arg(short, long, value_enum)]
    dataset: Dataset,

    #[arg(long, default_value = "10000000")]
    stats_interval: usize,

    #[arg(long)]
    output_dir: Option<String>,

    #[arg(long)]
    traces_csv: Option<String>,

    #[arg(long)]
    known_addresses_csv: Option<String>,

    #[arg(long)]
    tornado_csv: Option<String>,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let data_loader = DataLoader::new(
        args.known_addresses_csv
            .unwrap_or(KNOWN_ADDRESSES_CSV.to_string()),
        args.tornado_csv.unwrap_or(TORNADO_CSV.to_string()),
        args.traces_csv.unwrap_or(TRACES_CSV.to_string()),
        args.output_dir.unwrap_or(OUTPUT_DIR.to_string()),
    );

    let algorithm: Box<dyn BlacklistingAlgorithm> = match args.algorithm {
        Algorithm::Poison => Box::new(Poison {}),
        Algorithm::Haircut => Box::new(Haircut {}),
        Algorithm::Seniority => Box::new(Seniority {}),
        _ => panic!("Algorithm not implemented"),
    };

    algorithm.run(&data_loader, args.dataset, args.stats_interval)?;

    // Number of unique addresses: 283,273,653
    // println!(
    //     "\nNumber of unique addresses: {}",
    //     data_loader.n_unique_addresses().separate_with_commas()
    // );

    Ok(())
}

// const OUTPUT_DIR: &str = "/data/blacklist";
// const TRACES_CSV: &str = "/data/blacklist/traces-sorted.csv";
// const KNOWN_ADDRESSES_CSV: &str = "/data/blacklist/known-addresses.csv";
// const TORNADO_CSV: &str = "/data/blacklist/tornado.csv";

const OUTPUT_DIR: &str = "/home/ponbac/dev/indago/data/tmp";
const TRACES_CSV: &str = "/home/ponbac/dev/indago/data/raw/traces-sorted.csv";
const KNOWN_ADDRESSES_CSV: &str = "/home/ponbac/dev/indago/data/known-addresses.csv";
const TORNADO_CSV: &str = "/home/ponbac/dev/indago/data/tornado.csv";
