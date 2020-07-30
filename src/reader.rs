use arrow::error::Result;
use arrow::ipc::reader::FileReader;
use std::io::{Seek, Read};
use arrow::record_batch::RecordBatchReader;
use arrow::array::UInt32Array;
use arrow::error::ArrowError::ParseError;

pub trait BatchBinarySearch {
    fn binary_search(&mut self, column: usize, value: u32) -> Result<usize>;
}

impl<R: Read + Seek> BatchBinarySearch for FileReader<R> {
    fn binary_search(&mut self, column_index: usize, value: u32) -> Result<usize> {
        let num_batches = self.num_batches();
        let mut min: usize = 0;
        let mut max = num_batches;

        let mut i: usize = (max + min) / 2;
        while min < max {
            println!("Reading batch: i={}, min={}, max={}, value={}", i, min, max, value);
            self.set_index(i)?;

            if let Some(batch) = self.next_batch()? {
                let column = batch.column(column_index)
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .expect("Failed to downcast");

                let first_value = column.value(0);
                let last_value = column.value(column.len() - 1);
                if value < first_value {
                    max = i - 1;
                } else if value > last_value {
                    min = i + 1;
                } else {
                    break;
                }
                i = (min + max) / 2;
            } else {
                return Err(ParseError("next_batch returned none".to_string()));
            }
        }
        Ok(i)
    }
}