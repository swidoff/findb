use std::fs::File;
use std::sync::Arc;
use std::time::SystemTime;

use arrow::csv::Reader;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::Result;
use arrow::record_batch::RecordBatchReader;
use arrow::util::pretty::print_batches;

use findb::{pricing_schema, Query};

const PRICING_FILE: &str = "/media/seth/external-500/prices.csv";
const IPC_FILE: &str = "content/prices.ipc";
const INDEX_FILE: &str = "content/prices.idx";

fn main() -> Result<()> {
    // if let Err(_) = File::open(IPC_FILE) {
    //     let mut reader = read_pricing_file(PRICING_FILE, 1024);
    //
    //     let start = SystemTime::now();
    //     eprintln!("Writing to ipc_file: {:?}", IPC_FILE);
    //     findb::write_ipc_file(&mut reader, IPC_FILE)?;
    //     eprintln!("Elapsed: {:?}", start.elapsed());
    // }
    //
    // let start = SystemTime::now();
    // eprintln!("Reading from: {:?}", IPC_FILE);
    // let mut reader = findb::read_ipc_file_memmap(IPC_FILE)?;
    // eprintln!(
    //     "Elapsed: {:?}. Num batches: {}",
    //     start.elapsed(),
    //     reader.num_batches()
    // );
    //
    // let index = if let Err(_) = File::open(INDEX_FILE) {
    //     eprintln!("Creating index and writing to file: {:?}", INDEX_FILE);
    //     let start = SystemTime::now();
    //     let index = Index::new(&mut reader, 0)?;
    //     index.write_file(INDEX_FILE)?;
    //     eprintln!("Elapsed: {:?}", start.elapsed());
    //     index
    // } else {
    //     eprintln!("Reading index file: {:?}", INDEX_FILE);
    //     let start = SystemTime::now();
    //     let index = Index::read_file(INDEX_FILE)?;
    //     eprintln!("Elapsed: {:?}", start.elapsed());
    //     reader.set_index(0)?;
    //     index
    // };
    //
    // let query_list = vec![
    //     Query {
    //         build_date: 20200618,
    //         start_date: 20200612,
    //         end_date: 20200618,
    //         eff_timestamp: 1595807440,
    //         asset_ids: vec!["@ALIGN2".to_string(), "@YANTA4".to_string()],
    //     },
    //     Query {
    //         build_date: 20200618,
    //         start_date: 20200612,
    //         end_date: 20200618,
    //         eff_timestamp: 1595807440,
    //         asset_ids: vec!["@AMINE1".to_string(), "@ZVEZD3".to_string()],
    //     },
    // ];
    //
    // println!("Issuing query.");
    // let start = SystemTime::now();
    // let min_batch = index.first_index_of(20200612);
    // let max_batch = index.last_index_of(20200618) + 1;
    //
    // for i in min_batch..max_batch {
    //     reader.set_index(i)?;
    //     if let Some(batch) = reader.next_batch()? {
    //         let result = Query::query_all(&query_list, &batch, 0, 1, 3, 4, 21)?;
    //         if result.len() > 0 {
    //             print_batches(&result[..])?;
    //         }
    //     }
    // }
    // eprintln!(
    //     "Elapsed: {:?}. Min Batch: {}, Max Batch: {}",
    //     start.elapsed(),
    //     min_batch,
    //     max_batch
    // );
    // Ok(())

    println!("Placeholder");
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
