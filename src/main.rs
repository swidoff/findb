use std::fs::File;
use std::sync::Arc;
use std::time::SystemTime;

use arrow::csv::Reader;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::Result;
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use arrow::util::pretty::print_batches;

use findb::{Query, read_ipc_file, read_ipc_file_memmap, write_ipc_file};

const PRICING_FILE: &str = "content/prices-00.csv";
const IPC_FILE: &str = "content/prices-00.ipc";

fn main() -> Result<()> {
    if let Err(_) = File::open(IPC_FILE) {
        let mut reader = read_pricing_file(PRICING_FILE, 1024);

        let start = SystemTime::now();
        eprintln!("Writing to ipc_file: {:?}", IPC_FILE);
        write_ipc_file(&mut reader, IPC_FILE)?;
        eprintln!("Elapsed: {:?}", start.elapsed());
    }

    let start = SystemTime::now();
    eprintln!("Reading from: {:?}", IPC_FILE);
    let mut reader = read_ipc_file(IPC_FILE)?;
    eprintln!("Elapsed: {:?}. Num batches: {}", start.elapsed(), reader.num_batches());

    let query_list = vec![
        Query {
            build_date: 20200618,
            start_date: 20200612,
            end_date: 20200618,
            eff_timestamp: 1595807440,
            asset_ids: vec!["@ALIGN2".to_string(), "@YANTA4".to_string()],
        },
        Query {
            build_date: 20200618,
            start_date: 20200612,
            end_date: 20200618,
            eff_timestamp: 1595807440,
            asset_ids: vec!["@AMINE1".to_string(), "@ZVEZD3".to_string()],
        },
    ];

    println!("Issuing query.");
    let start = SystemTime::now();
    reader.set_index(reader.num_batches() - 100);
    while let Some(batch) = reader.next_batch()? {
        let result = Query::query_all(&query_list, &batch, 0, 1, 3, 4, 21)?;
        if result.len() > 0 {
            print_batches(&result[..]);
        }
    }
    eprintln!("Elapsed: {:?}", start.elapsed());
    Ok(())
}

fn pricing_schema() -> Schema {
    Schema::new(vec![
        Field::new("date", DataType::UInt32, false),
        Field::new("asset_id", DataType::Utf8, false),
        Field::new("infocode", DataType::UInt32, false),
        Field::new("eff_start", DataType::UInt64, false),
        Field::new("eff_end", DataType::UInt64, false),
        Field::new("currency", DataType::Utf8, false),
        Field::new("exchintcode", DataType::Utf8, false),
        Field::new("fx_rate_usd", DataType::Float64, false),
        Field::new("split_adj_factor", DataType::Float64, false),
        Field::new("open", DataType::Float64, false),
        Field::new("open_usd", DataType::Float64, false),
        Field::new("open_adj", DataType::Float64, false),
        Field::new("open_adj_usd", DataType::Float64, false),
        Field::new("high", DataType::Float64, false),
        Field::new("high_usd", DataType::Float64, false),
        Field::new("high_adj", DataType::Float64, false),
        Field::new("high_adj_usd", DataType::Float64, false),
        Field::new("low", DataType::Float64, false),
        Field::new("low_usd", DataType::Float64, false),
        Field::new("low_adj", DataType::Float64, false),
        Field::new("low_adj_usd", DataType::Float64, false),
        Field::new("close", DataType::Float64, false),
        Field::new("close_usd", DataType::Float64, false),
        Field::new("close_adj", DataType::Float64, false),
        Field::new("close_adj_usd", DataType::Float64, false),
        Field::new("volume", DataType::Float64, false),
        Field::new("volume_adj", DataType::Float64, false),
        Field::new("bid", DataType::Float64, false),
        Field::new("bid_usd", DataType::Float64, false),
        Field::new("bid_adj", DataType::Float64, false),
        Field::new("bid_adj_usd", DataType::Float64, false),
        Field::new("ask", DataType::Float64, false),
        Field::new("ask_usd", DataType::Float64, false),
        Field::new("ask_adj", DataType::Float64, false),
        Field::new("ask_adj_usd", DataType::Float64, false),
        Field::new("vwap", DataType::Float64, false),
        Field::new("vwap_usd", DataType::Float64, false),
        Field::new("vwap_adj", DataType::Float64, false),
        Field::new("vwap_adj_usd", DataType::Float64, false),
        Field::new("return_index", DataType::Float64, false),
        Field::new("return_index_usd", DataType::Float64, false),
        Field::new("shares", DataType::Float64, false),
        Field::new("shares_adj", DataType::Float64, false),
        Field::new("marketcap", DataType::Float64, false),
        Field::new("marketcap_usd", DataType::Float64, false),
    ])
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
