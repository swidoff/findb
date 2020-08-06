use crate::MmapFile;
use arrow::array::{ArrayBuilder, UInt32Array};
use arrow::csv::Reader;
use arrow::datatypes::SchemaRef;
use arrow::error::Result;
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use itertools::Itertools;
use std::fs::File;
use std::io::Read;
use std::sync::Arc;

struct YearFile {
    year: u32,
    writer: FileWriter<File>,
}

impl YearFile {
    fn new(schema: &SchemaRef, root: &String, prefix: &String, year: u32) -> Result<YearFile> {
        let path = format!("{}/{}_{}.ipc", root, prefix, year);
        let new_file = File::create(path)?;
        let writer = FileWriter::try_new(new_file, &schema)?;
        Ok(YearFile { year, writer })
    }
}

struct YearMonthBatch {
    year_month: u32,
    batch: Vec<Box<dyn ArrayBuilder>>,
}

impl YearMonthBatch {
    fn new(year_month: u32, batch: Vec<Box<dyn ArrayBuilder>>) -> YearMonthBatch {
        YearMonthBatch { year_month, batch }
    }

    fn append(
        &mut self,
        _record_batch: &RecordBatch,
        _start_index: usize,
        _end_index: usize,
    ) -> Result<()> {
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

struct YearFileGenerator<F>
where
    F: Fn() -> Vec<Box<dyn ArrayBuilder>>,
{
    schema: SchemaRef,
    root: String,
    prefix: String,
    factory: F,
    file: Option<YearFile>,
    batch: Option<YearMonthBatch>,
}

impl<F> YearFileGenerator<F>
where
    F: Fn() -> Vec<Box<dyn ArrayBuilder>>,
{
    fn new(schema: &SchemaRef, root: String, prefix: String, factory: F) -> YearFileGenerator<F> {
        return YearFileGenerator {
            schema: Arc::clone(schema),
            root,
            prefix,
            factory,
            file: None,
            batch: None,
        };
    }

    fn append(
        &mut self,
        year_month: u32,
        record_batch: &RecordBatch,
        start_index: usize,
        end_index: usize,
    ) -> Result<()> {
        // TODO: Fill missing years.
        let last_batch = match &mut self.batch {
            Some(batch) if batch.year_month > year_month => {
                panic!("Months should be monotonically increasing.")
            }
            Some(batch) if batch.year_month > year_month => Some(batch.finish(&self.schema)?),
            _ => None,
        };

        if let Some(batch) = &last_batch {
            let year = year_month / 100;
            self.write(year, batch);
            self.batch = None
        }

        if self.batch.is_none() {
            let new_batch = YearMonthBatch::new(year_month, (self.factory)());
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
            let new_file = YearFile::new(&self.schema, &self.root, &self.prefix, year)?;
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

pub fn write_to_yearly_files_monthly_batches<T: Read, F>(
    reader: &mut Reader<T>,
    root: String,
    prefix: String,
    factory: F,
) -> Result<()>
where
    F: Fn() -> Vec<Box<dyn ArrayBuilder>>,
{
    let mut gen = YearFileGenerator::new(&reader.schema(), root, prefix, factory);
    while let Ok(Some(record_batch)) = reader.next() {
        let date_column: &UInt32Array = get_column(&record_batch, 0);
        let year_month_indexes = month_indexes(date_column);

        for (year_month, start_index, end_index) in year_month_indexes {
            gen.append(year_month, &record_batch, start_index, end_index);
        }
    }

    gen.finish();
    Ok(())
}

type Month = u32;
type Index = usize;

#[inline]
fn yyyymm(yyyymmdd: u32) -> u32 {
    yyyymmdd / 100
}

pub fn month_indexes(array: &UInt32Array) -> Vec<(Month, Index, Index)> {
    let min_month: u32 = yyyymm(array.value(0));
    let max_month: u32 = yyyymm(array.value(array.len() - 1));
    let mut res: Vec<(Month, Index, Index)> = Vec::new();
    res.push((min_month, 0, array.len()));

    let slice: &[u32] = array.value_slice(0, array.len());
    for month in (min_month + 1)..(max_month + 1) {
        let first_day = month * 100 + 1;
        let index = match slice.binary_search(&first_day) {
            Ok(index) => {
                // The binary search finds a match, but not necessarily the first match
                let mut i = index;
                while i > 0 && slice[i - 1] == first_day {
                    i -= 1;
                }
                i
            }
            Err(index) => index,
        };
        res.push((month, index, array.len()))
    }

    for i in 0..(res.len() - 1) {
        res[i].2 = res[i + 1].1
    }

    res
}

pub fn write_ipc_file<T: Read>(reader: &mut Reader<T>, file_name: &str) -> Result<()> {
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

fn get_column<T: 'static>(batch: &RecordBatch, index: usize) -> &T {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .expect("Failed to downcast")
}
