use crate::MmapFile;
use arrow::array::{
    ArrayBuilder, Float64Builder, StringBuilder, UInt32Array, UInt32Builder, UInt64Builder,
};
use arrow::compute::kernels::{boolean, comparison, filter};
use arrow::csv;
use arrow::datatypes::{DataType, SchemaRef};
use arrow::error::{ArrowError, Result};
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use itertools::Itertools;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;

type Year = u32;
type YearMonth = u32;
type StartIndex = usize;
type EndIndex = usize;

pub struct YearFileMonthlyBatchReader {
    readers: HashMap<Year, FileReader<File>>,
}

impl YearFileMonthlyBatchReader {
    pub fn try_new(root: &str) -> Result<YearFileMonthlyBatchReader> {
        let root_path = Path::new(&root[..]);
        let mut readers = HashMap::new();
        for entry in root_path.read_dir()? {
            let entry_path = entry?.path();
            if let Some(extension) = entry_path.extension() {
                if extension != "ipc" {
                    continue;
                }
            }

            if let Some(year_str) = entry_path.file_stem().and_then(|f| f.to_str()) {
                let year: Year = year_str
                    .parse::<Year>()
                    .map_err(|e| ArrowError::ParseError(e.to_string()))?;
                let file = File::open(entry_path)?;
                let reader = FileReader::try_new(file)?;
                readers.insert(year, reader);
            }
        }

        if readers.is_empty() {
            Err(ArrowError::InvalidArgumentError(format!(
                "Root directory {} is empty.",
                root
            )))
        } else {
            Ok(YearFileMonthlyBatchReader { readers })
        }
    }
}

pub fn write_csv_to_yearly_ipc_files_monthly_batches<T: Read>(
    csv_reader: &mut csv::Reader<T>,
    root: &str,
) -> Result<()> {
    let mut gen = YearFileGenerator::new(&csv_reader.schema(), root);
    while let Ok(Some(record_batch)) = csv_reader.next() {
        let date_column: &UInt32Array = get_column(&record_batch, 0);
        let year_month_indexes = year_month_index_ranges(date_column);

        for (year_month, start_index, end_index) in year_month_indexes {
            gen.append(year_month, &record_batch, start_index, end_index)?;
        }
    }

    gen.finish()?;
    Ok(())
}

struct YearFileWriter {
    year: Year,
    writer: FileWriter<File>,
}

impl YearFileWriter {
    fn new(schema: &SchemaRef, root: &str, year: u32) -> Result<YearFileWriter> {
        let path = format!("{}/{}.ipc", root, year);
        let new_file = File::create(path)?;
        let writer = FileWriter::try_new(new_file, &schema)?;
        Ok(YearFileWriter { year, writer })
    }
}

struct YearMonthBatch {
    year_month: YearMonth,
    batch: Vec<Box<dyn ArrayBuilder>>,
}

impl YearMonthBatch {
    fn new(schema: &SchemaRef, year_month: u32) -> Result<YearMonthBatch> {
        let mut batch = Vec::new();
        for field in schema.fields() {
            // TODO: Figure out a good value for capacity.
            let builder = new_builder(field.data_type(), 10000)?;
            batch.push(builder);
        }
        Ok(YearMonthBatch { year_month, batch })
    }

    /// Appends the column arrays in the `record_batch` from `start_index` to `end_index` to the arrays in the `batch`.
    fn append(
        &mut self,
        record_batch: &RecordBatch,
        start_index: usize,
        end_index: usize,
    ) -> Result<()> {
        assert_eq!(record_batch.num_columns(), self.batch.len());
        let length = end_index - start_index;

        for i in 0..record_batch.num_columns() {
            // Obtain a slice of the column from start_index to end_index.
            let source_column = record_batch.column(i);
            let source_slice = [source_column.slice(start_index, length).data()];
            self.batch[i].append_data(&source_slice[..])?;
        }

        Ok(())
    }

    fn finish(&mut self, schema: &SchemaRef) -> Result<RecordBatch> {
        RecordBatch::try_new(
            Arc::clone(schema),
            self.batch
                .iter_mut()
                .map(|builder| builder.finish())
                .collect_vec(),
        )
    }
}

struct YearFileGenerator {
    schema: SchemaRef,
    root: String,
    file: Option<YearFileWriter>,
    batch: Option<YearMonthBatch>,
}

impl YearFileGenerator {
    fn new(schema: &SchemaRef, root: &str) -> YearFileGenerator {
        return YearFileGenerator {
            schema: Arc::clone(schema),
            root: root.to_string(),
            file: None,
            batch: None,
        };
    }

    fn append(
        &mut self,
        year_month: YearMonth,
        record_batch: &RecordBatch,
        start_index: usize,
        end_index: usize,
    ) -> Result<()> {
        // TODO: Fill missing years.
        let last_batch = match &mut self.batch {
            Some(batch) if batch.year_month > year_month => {
                panic!("Months should be monotonically increasing.")
            }
            Some(batch) if batch.year_month < year_month => {
                Some((batch.year_month, batch.finish(&self.schema)?))
            }
            _ => None,
        };

        if let Some((batch_year_month, batch)) = &last_batch {
            let year = batch_year_month / 100;
            self.write(year, batch)?;
            self.batch = None
        }

        if self.batch.is_none() {
            let new_batch = YearMonthBatch::new(&self.schema, year_month)?;
            self.batch = Some(new_batch);
        }

        if let Some(batch) = &mut self.batch {
            batch.append(record_batch, start_index, end_index)?;
        }

        Ok(())
    }

    fn write(&mut self, year: u32, record_batch: &RecordBatch) -> Result<()> {
        // TODO: Fill missing years.
        // TODO: Collect Strings
        // Finish the current file if we've changed years.
        match &mut self.file {
            Some(current) if current.year > year => {
                panic!("Years should be monotonically increasing.")
            }
            Some(current) => {
                if current.year < year {
                    current.writer.finish()?;
                    self.file = None;
                }
            }
            _ => {}
        }

        // Initialize the file for the current year.
        if self.file.is_none() {
            let new_file = YearFileWriter::new(&self.schema, &self.root, year)?;
            self.file = Some(new_file);
        }

        // Write the batch to the file.
        if let Some(file) = &mut self.file {
            file.writer.write(record_batch)?
        }

        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        let last_batch = match &mut self.batch {
            Some(batch) => Some((batch.year_month, batch.finish(&self.schema)?)),
            _ => None,
        };
        if let Some((year_month, batch)) = last_batch {
            let year = year_month / 100;
            self.write(year, &batch)?;
        }
        if let Some(file) = &mut self.file {
            file.writer.finish()?
        }
        Ok(())
    }
}

#[inline]
fn yyyymm(yyyymmdd: u32) -> u32 {
    yyyymmdd / 100
}

/// Returns a vector that returns the start and end indexes of each YYYYMM date integer prefix in the supplied array.
/// The end_index is exclusive.
///
fn year_month_index_ranges(array: &UInt32Array) -> Vec<(YearMonth, StartIndex, EndIndex)> {
    let min_year_month: u32 = yyyymm(array.value(0));
    let max_year_month: u32 = yyyymm(array.value(array.len() - 1));
    let mut year_month = min_year_month;
    let mut res: Vec<(YearMonth, StartIndex, EndIndex)> = Vec::new();

    let slice: &[u32] = array.value_slice(0, array.len());
    while year_month <= max_year_month {
        let first_day = year_month * 100 + 1;
        let start_index = match slice.binary_search(&first_day) {
            Ok(index) => {
                // The binary search finds a match, but not necessarily the first match, so rollback to the first.
                let mut i = index;
                while i > 0 && slice[i - 1] == first_day {
                    i -= 1;
                }
                i
            }
            Err(index) => index,
        };
        res.push((year_month, start_index, array.len()));

        // Advance year/month.
        let mut year = year_month / 100;
        let mut month = year_month % 100;
        if month == 12 {
            year += 1;
            month = 1;
        } else {
            month += 1
        }
        year_month = year * 100 + month;
    }

    // Set the end index to be the start index of the next year_month. The last element will continue to have the
    // array length as its end index.
    for i in 0..(res.len() - 1) {
        res[i].2 = res[i + 1].1
    }

    res
}

fn get_column<T: 'static>(batch: &RecordBatch, index: usize) -> &T {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .expect("Failed to downcast")
}

fn new_builder(data_type: &DataType, capacity: usize) -> Result<Box<dyn ArrayBuilder>> {
    match data_type {
        DataType::UInt32 => Ok(Box::new(UInt32Builder::new(capacity))),
        DataType::UInt64 => Ok(Box::new(UInt64Builder::new(capacity))),
        DataType::Utf8 => Ok(Box::new(StringBuilder::new(capacity))),
        DataType::Float64 => Ok(Box::new(Float64Builder::new(capacity))),
        _ => Err(ArrowError::InvalidArgumentError(format!(
            "Not a supported data type: {:?}",
            data_type
        ))),
    }
}

/// OLD API

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pricing_schema;
    use arrow::array::StringArray;
    use arrow::record_batch::RecordBatchReader;

    #[test]
    fn write_from_single_file_two_years_validate_readers() {
        let root = "tests/content/faangm_pricing";
        let mut csv_reader = csv::Reader::new(
            File::open("tests/content/faangm_201X.csv").expect("Unable to open csv file"),
            Arc::new(pricing_schema()),
            false,
            None,
            1024,
            None,
        );
        write_csv_to_yearly_ipc_files_monthly_batches(&mut csv_reader, root)
            .expect("Failed to write IPC files");

        let mut ipc_reader =
            YearFileMonthlyBatchReader::try_new(root).expect("Failed to read IPC files");
        assert_eq!(ipc_reader.readers.len(), 10, "Years of readers.");

        for (year, year_reader) in ipc_reader.readers.iter_mut() {
            for month in 1..13 {
                let batch = year_reader
                    .next_batch()
                    .expect("Failed to read batch.")
                    .expect("Batch was None");

                // Assert all rows are for the year/month.
                let date_column: &UInt32Array = get_column(&batch, 0);
                let dates_within_month = filter::filter(
                    date_column,
                    &boolean::and(
                        &comparison::gt_eq_scalar(date_column, year * 10000 + month * 100).unwrap(),
                        &comparison::lt_eq_scalar(date_column, year * 10000 + month * 100 + 31)
                            .unwrap(),
                    )
                    .unwrap(),
                )
                .unwrap();
                assert_eq!(
                    date_column.len(),
                    dates_within_month.len(),
                    "All dates are within year {} and month {}",
                    year,
                    month
                );

                // Assert all five tickers are present.
                let asset_column: &StringArray = get_column(&batch, 1);
                let tickers = vec!["AAPL", "AMZN", "GOOG", "MSFT"]; // "NTFZ", "30303M10"];
                for ticker in tickers.into_iter() {
                    let rows_for_ticker = filter::filter(
                        asset_column,
                        &comparison::eq_utf8_scalar(asset_column, ticker).unwrap(),
                    )
                    .unwrap();
                    assert!(
                        rows_for_ticker.len() > 0,
                        "Ticker not present for year {} and month {}: {}",
                        year,
                        month,
                        ticker
                    )
                }
            }
        }
    }
}

pub fn write_ipc_file<T: Read>(reader: &mut csv::Reader<T>, file_name: &str) -> Result<()> {
    let ipc_file = File::create(file_name)?;
    let mut writer = FileWriter::try_new(ipc_file, &reader.schema())?;
    while let Ok(Some(batch)) = reader.next() {
        writer.write(&batch)?;
    }
    writer.finish()?;
    Ok(())
}

pub fn read_ipc_file(file_name: &str) -> Result<FileReader<File>> {
    let ipc_file = File::open(file_name)?;
    FileReader::try_new(ipc_file)
}

pub fn read_ipc_file_memmap(file_name: &str) -> Result<FileReader<MmapFile>> {
    let ipc_file = File::open(file_name)?;
    let mmap_file = MmapFile::new(ipc_file);
    FileReader::try_new(mmap_file)
}
