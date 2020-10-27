use std::sync::Arc;

use crate::ipc::{get_column, YearFileMonthlyBatchReader, YearMonthRange};
use arrow::array::{BooleanArray, Float64Array, StringArray, UInt32Array, UInt64Array};
use arrow::compute::kernels::{boolean, comparison, filter};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::Result;
use arrow::record_batch::RecordBatch;
use std::time::SystemTime;

pub struct Query {
    pub build_date: u32,
    pub start_date: u32,
    pub end_date: u32,
    pub eff_timestamp: u64,
    pub asset_ids: Vec<String>,
}

impl Query {
    pub fn query(
        &self,
        reader: &mut YearFileMonthlyBatchReader,
        date_index: usize,
        fid_index: usize,
        eff_start_index: usize,
        eff_end_index: usize,
        value_index: usize,
    ) -> Result<Vec<RecordBatch>> {
        let mut res = Vec::new();
        for year_month in YearMonthRange::new(self.start_date / 100, self.end_date / 100) {
            if let Some(batch) = reader.read(year_month)? {
                if let Some(result_batch) = self.query_batch(
                    &batch,
                    date_index,
                    fid_index,
                    eff_start_index,
                    eff_end_index,
                    value_index,
                )? {
                    res.push(result_batch);
                }
            }
        }
        return Ok(res);
    }

    fn query_batch(
        &self,
        batch: &RecordBatch,
        date_index: usize,
        fid_index: usize,
        eff_start_index: usize,
        eff_end_index: usize,
        value_index: usize,
    ) -> Result<Option<RecordBatch>> {
        eprintln!("batch of {} rows", batch.num_rows());
        let date_column: &UInt32Array = get_column(&batch, date_index);
        let fid_column: &StringArray = get_column(&batch, fid_index);
        let eff_start_column: &UInt64Array = get_column(&batch, eff_start_index);
        let eff_end_column: &UInt64Array = get_column(&batch, eff_end_index);
        let value_column: &Float64Array = get_column(&batch, value_index);

        let asset_id_query = self.query_asset_ids(fid_column)?;
        let date_range_query = self.query_date_range(date_column)?;
        let eff_date_query = self.query_eff_timestamp(eff_start_column, eff_end_column)?;

        let start = SystemTime::now();
        let selection_query = match asset_id_query {
            None => date_range_query,
            Some(asset_id_query) => boolean::and(&date_range_query, &asset_id_query)?,
        };
        let condition = boolean::and(&selection_query, &eff_date_query)?;
        eprintln!("combine_criteria: {:?}", start.elapsed());

        let start = SystemTime::now();
        let res_date = filter::filter(date_column, &condition)?;
        if res_date.len() == 0 {
            Ok(None)
        } else {
            let res_fid = filter::filter(fid_column, &condition)?;
            let res_value = filter::filter(value_column, &condition)?;
            eprintln!("filter: {:?}", start.elapsed());

            let start = SystemTime::now();
            let len = res_date.len();
            let mut build_date_column_builder = UInt32Array::builder(len);
            for _ in 0..len {
                build_date_column_builder
                    .append_value(self.build_date)
                    .unwrap();
            }
            let res_build_date = Arc::new(build_date_column_builder.finish());

            let schema = batch.schema();
            let value_column_name = schema.field(value_index).name();
            let res_schema = Schema::new(vec![
                Field::new("build_date", DataType::UInt32, false),
                Field::new("fid", DataType::Utf8, false),
                Field::new("data_date", DataType::UInt32, false),
                Field::new(&value_column_name[..], DataType::Float64, true),
            ]);
            let res = RecordBatch::try_new(
                Arc::new(res_schema),
                vec![res_build_date, res_fid, res_date, res_value],
            )
            .map(|b| Some(b));
            eprintln!("build batch: {:?}", start.elapsed());
            res
        }
    }

    fn query_date_range(&self, date_column: &UInt32Array) -> Result<BooleanArray> {
        let start = SystemTime::now();
        let res = boolean::and(
            &comparison::gt_eq_scalar(date_column, self.start_date)?,
            &comparison::lt_eq_scalar(date_column, self.end_date)?,
        );
        eprintln!("Query::query_date_range: {:?}", start.elapsed());
        res
    }

    fn query_asset_ids(&self, asset_id_column: &StringArray) -> Result<Option<BooleanArray>> {
        let start = SystemTime::now();
        let mut res: Option<BooleanArray> = Option::None;
        for asset_id in self.asset_ids.iter() {
            let asset_id_query = comparison::eq_utf8_scalar(asset_id_column, &asset_id[..])?;
            res = Some(match res {
                None => asset_id_query,
                Some(or_cond) => boolean::or(&or_cond, &asset_id_query)?,
            })
        }
        eprintln!("Query::query_asset_ids: {:?}", start.elapsed());
        Ok(res)
    }

    fn query_eff_timestamp(
        &self,
        eff_start_column: &UInt64Array,
        eff_end_column: &UInt64Array,
    ) -> Result<BooleanArray> {
        let start = SystemTime::now();
        let res = boolean::and(
            &comparison::lt_eq_scalar(eff_start_column, self.eff_timestamp)?,
            &comparison::gt_eq_scalar(eff_end_column, self.eff_timestamp)?,
        );
        eprintln!("Query::query_eff_timestamp: {:?}", start.elapsed());
        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ipc::write_csv_to_yearly_ipc_files_monthly_batches;
    use crate::pricing_schema;
    use arrow::csv;
    use arrow::util::pretty::pretty_format_batches;
    use itertools::Itertools;
    use std::fs::File;

    #[test]
    fn date_range_multiple_assets() {
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

        let query = Query {
            build_date: 20191231,
            start_date: 20191031,
            end_date: 20191101,
            eff_timestamp: 1595807440,
            asset_ids: vec!["AAPL", "AMZN", "GOOG", "MSFT"]
                .iter()
                .map(|s| s.to_string())
                .collect_vec(),
        };
        let res = query.query(&mut ipc_reader, 0, 1, 3, 4, 22).unwrap();

        let expected = "\
+------------+------+-----------+-----------+
| build_date | fid  | data_date | close_usd |
+------------+------+-----------+-----------+
| 20191231   | AAPL | 20191031  | 248.76    |
| 20191231   | AMZN | 20191031  | 1776.66   |
| 20191231   | GOOG | 20191031  | 1258.8001 |
| 20191231   | MSFT | 20191031  | 143.37    |
| 20191231   | AAPL | 20191101  | 255.82001 |
| 20191231   | AMZN | 20191101  | 1791.44   |
| 20191231   | GOOG | 20191101  | 1272.25   |
| 20191231   | MSFT | 20191101  | 143.72001 |
+------------+------+-----------+-----------+
";
        let actual = pretty_format_batches(&res[..]).unwrap();
        assert_eq!(expected, &actual[..]);
        // print_batches(&res[..]).unwrap();
    }
}
