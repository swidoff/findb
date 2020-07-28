use std::fs::File;
use std::io::{IoSliceMut, Read, Seek, SeekFrom};
use std::io;
use std::sync::Arc;

use arrow::array::{BooleanArray, Float64Array, StringArray, UInt32Array, UInt64Array};
use arrow::compute::kernels::{boolean, comparison, filter};
use arrow::csv::Reader;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;
use itertools::Itertools;
use memmap::{Mmap, MmapOptions};
use arrow::record_batch::RecordBatchReader;

use findb::Query;

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("date", DataType::UInt32, false),
        Field::new("fid", DataType::Utf8, false),
        Field::new("eff_start", DataType::UInt64, false),
        Field::new("eff_end", DataType::UInt64, false),
        Field::new("close", DataType::Float64, true),
    ])
}

fn read_faangm_20206_close() -> RecordBatch {
    let file = File::open("tests/content/faangm_202006_close.csv").unwrap();
    let mut reader = Reader::new(file, Arc::new(schema()), true, None, 1024, None);
    reader.next().unwrap().unwrap()
}

fn get_column<T: 'static>(batch: &RecordBatch, index: usize) -> &T {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .expect("Failed to downcast")
}

#[test]
fn test_one_name_one_date() {
    let batch = read_faangm_20206_close();
    let target_date = 20200623;
    let target_name = "AMZN";

    let date_column: &UInt32Array = get_column(&batch, 0);
    let fid_column: &StringArray = get_column(&batch, 1);
    let close_column: &Float64Array = get_column(&batch, 4);

    let condition = boolean::and(
        &comparison::eq_scalar(&date_column, target_date).unwrap(),
        &comparison::eq_utf8_scalar(&fid_column, target_name).unwrap(),
    )
        .unwrap();

    let res = filter::filter(close_column, &condition).unwrap();
    let schema = Schema::new(vec![Field::new("close", DataType::Float64, true)]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![res]).unwrap();

    let batch_arr = [batch];
    print_batches(&batch_arr).unwrap()
}

#[test]
fn test_two_names_two_dates() {
    let batch = read_faangm_20206_close();
    let target_date1 = 20200623;
    let target_date2 = 20200624;
    let target_name1 = "AMZN";
    let target_name2 = "AAPL";

    let date_column: &UInt32Array = get_column(&batch, 0);
    let fid_column: &StringArray = get_column(&batch, 1);
    let close_column: &Float64Array = get_column(&batch, 4);

    let condition = boolean::and(
        &boolean::or(
            &comparison::eq_scalar(&date_column, target_date1).unwrap(),
            &comparison::eq_scalar(&date_column, target_date2).unwrap(),
        )
            .unwrap(),
        &boolean::or(
            &comparison::eq_utf8_scalar(&fid_column, target_name1).unwrap(),
            &comparison::eq_utf8_scalar(&fid_column, target_name2).unwrap(),
        )
            .unwrap(),
    )
        .unwrap();

    let schema = Schema::new(vec![
        Field::new("date", DataType::UInt32, false),
        Field::new("fid", DataType::Utf8, false),
        Field::new("close", DataType::Float64, true),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            filter::filter(date_column, &condition).unwrap(),
            filter::filter(fid_column, &condition).unwrap(),
            filter::filter(close_column, &condition).unwrap(),
        ],
    )
        .unwrap();

    let batch_arr = [batch];
    print_batches(&batch_arr).unwrap()
}

#[test]
fn test_query_list() {
    let batch = read_faangm_20206_close();
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

    let batch_arr = Query::query_all(&query_list, &batch, 4).unwrap();
    print_batches(&batch_arr[..]).unwrap()
}

#[test]
fn test_mmaped_db() {
    let schema = schema();
    let batch = read_faangm_20206_close();
    let ipc_file = File::create("tests/content/faangm_202006_close.ipc").unwrap();
    let mut writer = FileWriter::try_new(ipc_file, &schema).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();

    let ipc_file = File::open("tests/content/faangm_202006_close.ipc").unwrap();
    let mmap_file = MmapFile::new(ipc_file);
    let mut reader = FileReader::try_new(mmap_file).unwrap();
    println!("Num batches: {}", reader.num_batches());

    let batch = reader.next_batch().unwrap().unwrap();

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

    let batch_arr = Query::query_all(&query_list, &batch, 4).unwrap();
    print_batches(&batch_arr[..]).unwrap()
}

struct MmapFile {
    mmap: Mmap,
    offset: u64,
}

impl MmapFile {
    fn new(file: File) -> MmapFile {
        let mmap = unsafe { MmapOptions::new().map(&file).unwrap() };
        MmapFile { mmap, offset: 0 }
    }

    fn to_arr(&self) -> &[u8] {
        &self.mmap[self.offset as usize..]
    }
}

impl Seek for MmapFile {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        match pos {
            SeekFrom::Start(offset) => self.offset = offset,
            SeekFrom::End(offset) => self.offset = (self.mmap.len() as i64 + offset) as u64,
            SeekFrom::Current(offset) => self.offset = (self.offset as i64 + offset) as u64,
        }
        Ok(self.offset)
    }
}

impl Read for MmapFile {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.to_arr().read(buf)
    }

    #[inline]
    fn read_vectored(&mut self, bufs: &mut [IoSliceMut<'_>]) -> io::Result<usize> {
        self.to_arr().read_vectored(bufs)
    }

    #[inline]
    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        self.to_arr().read_to_end(buf)
    }

    #[inline]
    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        self.to_arr().read_exact(buf)
    }
}
