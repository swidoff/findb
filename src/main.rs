use std::fs::File;
use std::sync::Arc;



use arrow::csv::Reader;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::Result;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;


use findb::Query;

const PRICING_FILE: &str = "test/content/faangm_202006_close.csv";

fn main() -> Result<()> {
    println!("Loading table.");
    let batch = read_pricing_file(PRICING_FILE, 1024).unwrap().unwrap();

    let query_list = vec![
        Query {
            build_date: 20200624,
            start_date: 20200619,
            end_date: 20200623,
            eff_timestamp: 1595807440,
            asset_ids: vec!["AMZN".to_string(), "MSFT".to_string()],
        },
        Query {
            build_date: 20200625,
            start_date: 20200622,
            end_date: 20200624,
            eff_timestamp: 1595807440,
            asset_ids: vec!["NTFZ".to_string(), "AAPL".to_string()],
        },
    ];

    let result = Query::query_all(&query_list, &batch, 4)?;
    print_batches(&result[..])
}

fn pricing_schema() -> Schema {
    Schema::new(vec![
        Field::new("date", DataType::UInt32, false),
        Field::new("asset_id", DataType::Utf8, false),
        Field::new("eff_start", DataType::UInt64, false),
        Field::new("eff_end", DataType::UInt64, false),
        Field::new("close", DataType::Float64, true),
    ])
}

fn read_pricing_file(file: &str, batch_size: usize) -> Result<Option<RecordBatch>> {
    let file = File::open(file).unwrap();
    let mut reader = Reader::new(
        file,
        Arc::new(pricing_schema()),
        true,
        None,
        batch_size,
        None,
    );
    reader.next()
}
