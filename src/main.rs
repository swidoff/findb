use std::fs::File;
use std::sync::Arc;
use std::time::SystemTime;

use arrow::csv::Reader;
use arrow::error::Result;
use arrow::util::pretty::print_batches;
use itertools::Itertools;

use findb::{
    pricing_schema, write_csv_to_yearly_ipc_files_monthly_batches, Query,
    YearFileMonthlyBatchReader,
};

// const PRICING_FILE: &str = "/media/seth/external-500/prices.csv";
const PRICING_FILE: &str = "tests/content/faangm.csv";
const ROOT_DIR: &str = "content/ds_pricing";

fn main() -> Result<()> {
    if let Err(_) = YearFileMonthlyBatchReader::try_new(ROOT_DIR) {
        let start = SystemTime::now();
        eprintln!("Writing to: {:?}", ROOT_DIR);
        let mut csv_reader = read_pricing_file(PRICING_FILE, 1024);
        write_csv_to_yearly_ipc_files_monthly_batches(&mut csv_reader, ROOT_DIR)?;
        eprintln!("Elapsed: {:?}", start.elapsed());
    }

    let mut reader = YearFileMonthlyBatchReader::try_new(ROOT_DIR)?;

    let query = Query {
        build_date: 20191231,
        start_date: 20191015,
        end_date: 20191115,
        eff_timestamp: 1595807440,
        asset_ids: vec!["AAPL", "AMZN", "GOOG", "MSFT"]
            .iter()
            .map(|s| s.to_string())
            .collect_vec(),
    };

    println!("Issuing query.");
    let start = SystemTime::now();
    let res = query.query(&mut reader, 0, 1, 3, 4, 22)?;
    let elapsed = start.elapsed();
    print_batches(&res[..])?;
    eprintln!("Elapsed: {:?}", elapsed);
    Ok(())
}

fn read_pricing_file(file: &str, batch_size: usize) -> Reader<File> {
    let file = File::open(file).unwrap();
    Reader::new(
        file,
        Arc::new(pricing_schema()),
        true,
        None,
        batch_size,
        None,
    )
}
