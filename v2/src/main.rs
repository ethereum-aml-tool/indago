use algos::Poison;
use anyhow::Result;
use data_loader::DataLoader;
use run_data::{save_run_data, RunData};
use thousands::Separable;

mod algos;
mod data_loader;
mod run_data;

// block_number,transaction_index,trace_address,from_address,to_address,value,gas_used,status
// 19397712,2,"0,1,2",0x7a250d5630b4cf539739df2c5dacb4c659f2488d,0xa11243ac5a171a86ba64a2e9c3683ae1ffb58659,0,642.0,1
// 19397712,95,"1,0,3",0x74de5d4fcbf63e00296fd95d33236b9794016631,0xbb3d7f42c58abd83616ad7c8c72473ee46df2678,0,962.0,0

// address,name,account_type,entity,legitimacy,tags
// 0x8ab7404063ec4dbcfd4598215992dc3f8ec853d7, Akropolis (AKRO),contract,defi,1,defi
// 0x1c74cff0376fb4031cd7492cd6db2d66c3f2c6b9, bZx Protocol Token (BZRX),contract,defi,1,token contract

const TRACES_CSV: &str = "../data/raw/traces-sorted.csv";
const KNOWN_ADDRESSES_CSV: &str = "../data/known-addresses.csv";
const TORNADO_CSV: &str = "../data/tornado.csv";
const OUTPUT_DIR: &str = "../data/tmp";

enum Dataset {
    Tornado,
    KnownAddresses,
    Combined,
}

trait BlacklistingAlgorithm {
    fn run(
        &self,
        data_loader: &DataLoader,
        dataset: Dataset,
        n_between_stats: usize,
    ) -> Result<Vec<RunData>>;
}

fn main() -> Result<()> {
    let data_loader = DataLoader::new(
        KNOWN_ADDRESSES_CSV.to_string(),
        TORNADO_CSV.to_string(),
        TRACES_CSV.to_string(),
        OUTPUT_DIR.to_string(),
    );

    // let poison = Poison {};
    // let poison_rundata = poison.run(&data_loader, Dataset::Combined, 100_000)?;
    // save_run_data(
    //     poison_rundata,
    //     format!("{}/poison-rundata.csv", OUTPUT_DIR).as_str(),
    // )?;

    println!(
        "\nNumber of unique addresses: {}",
        data_loader.n_unique_addresses().separate_with_commas()
    );

    Ok(())
}
