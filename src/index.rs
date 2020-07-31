use arrow;
use arrow::array::UInt32Array;
use arrow::ipc::reader::FileReader;
use arrow::record_batch::RecordBatchReader;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::fs::File;
use std::io;
use std::io::{Read, Seek};

/// Block Range Index
pub struct Index {
    block_range: Vec<(u32, u32)>,
}

impl Index {
    pub fn first_index_of(&self, value: u32) -> usize {
        let mut i = self.any_index_of(value);

        // Scan to the first block that contains the value.
        while i > 0 && value >= self.block_range[i - 1].0 && value <= self.block_range[i - 1].1 {
            i -= 1;
        }
        i
    }

    pub fn last_index_of(&self, value: u32) -> usize {
        let mut i = self.any_index_of(value);

        // Scan to the last block that contains the value.
        while i < self.block_range.len() - 1
            && value >= self.block_range[i + 1].0
            && value <= self.block_range[i + 1].1
        {
            i += 1;
        }
        i
    }

    fn any_index_of(&self, value: u32) -> usize {
        let mut min: usize = 0;
        let mut max = self.block_range.len();

        let mut i: usize = (max + min) / 2;
        while min < max {
            let (first_value, last_value) = self.block_range[i];
            if value < first_value {
                max = i - 1;
            } else if value > last_value {
                min = i + 1;
            } else {
                break;
            }
            i = (min + max) / 2;
        }
        i
    }

    pub fn new<R: Read + Seek>(
        reader: &mut FileReader<R>,
        column_index: usize,
    ) -> arrow::error::Result<Index> {
        let num_batches = reader.num_batches();
        let mut block_dates: Vec<(u32, u32)> = Vec::with_capacity(num_batches);

        while let Some(batch) = reader.next_batch()? {
            let column = batch
                .column(column_index)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .expect("Failed to downcast");

            let first_value = column.value(0);
            let last_value = column.value(column.len() - 1);
            block_dates.push((first_value, last_value))
        }
        Ok(Index {
            block_range: block_dates,
        })
    }

    pub fn write_file(&self, file_name: &str) -> io::Result<()> {
        let mut file = File::create(file_name)?;
        file.write_u32::<BigEndian>(self.block_range.len() as u32)?;
        for (first_value, last_value) in self.block_range.iter() {
            file.write_u32::<BigEndian>(*first_value)?;
            file.write_u32::<BigEndian>(*last_value)?;
        }
        Ok(())
    }

    pub fn read_file(file_name: &str) -> io::Result<Index> {
        let mut file = File::open(file_name)?;
        let num_batches = file.read_u32::<BigEndian>()?;
        let mut block_dates: Vec<(u32, u32)> = Vec::with_capacity(num_batches as usize);
        for _ in 0..num_batches {
            let first_value = file.read_u32::<BigEndian>()?;
            let second_value = file.read_u32::<BigEndian>()?;
            block_dates.push((first_value, second_value));
        }
        Ok(Index {
            block_range: block_dates,
        })
    }
}
