use arrow::record_batch::RecordBatchReader;
use std::fs::File;
use arrow::ipc::writer::FileWriter;
use arrow::error::Result;
use arrow::csv::Reader;
use std::io::Read;
use arrow::ipc::reader::FileReader;
use crate::MmapFile;

pub fn write_ipc_file<T: Read>(reader: &mut Reader<T>, file_name: &str) -> Result<()> {
    let ipc_file = File::create(file_name)?;
    let mut writer = FileWriter::try_new(ipc_file, &reader.schema())?;
    while let Ok(Some(batch)) = reader.next() {
        writer.write(&batch)?;
    }
    writer.finish()?;
    Ok(())
}

pub fn read_ipc_file(file_name: &str) -> Result<FileReader<MmapFile>> {
    let ipc_file = File::open(file_name)?;
    let mmap_file = MmapFile::new(ipc_file);
    FileReader::try_new(mmap_file)
}