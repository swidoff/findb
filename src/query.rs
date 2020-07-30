use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{BooleanArray, Float64Array, StringArray, UInt32Array, UInt64Array};
use arrow::compute::kernels::{boolean, comparison, filter};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::Result;
use arrow::record_batch::RecordBatch;
use itertools::{process_results, Itertools};

pub struct Query {
    pub build_date: u32,
    pub start_date: u32,
    pub end_date: u32,
    pub eff_timestamp: u64,
    pub asset_ids: Vec<String>,
}

impl Query {
    pub fn query_all(
        queries: &Vec<Query>,
        batch: &RecordBatch,
        date_index: usize,
        fid_index: usize,
        eff_start_index: usize,
        eff_end_index: usize,
        value_index: usize,
    ) -> Result<Vec<RecordBatch>> {
        let mut asset_id_queries = HashMap::new();
        let results = queries
            .into_iter()
            .map(|q| q.query(
                batch, date_index, fid_index, eff_start_index, eff_end_index, value_index,
                &mut asset_id_queries));
        process_results(results, |r| r.filter_map(|b| b).collect_vec())
    }

    fn query_date_range(&self, date_column: &UInt32Array) -> Result<BooleanArray> {
        boolean::and(
            &comparison::gt_eq_scalar(date_column, self.start_date)?,
            &comparison::lt_eq_scalar(date_column, self.end_date)?,
        )
    }

    fn query_asset_ids<'a>(
        &'a self,
        asset_id_column: &StringArray,
        asset_id_queries: &mut HashMap<&'a String, Arc<BooleanArray>>,
    ) -> Result<Option<Arc<BooleanArray>>> {
        let mut res: Option<Arc<BooleanArray>> = Option::None;
        for asset_id in self.asset_ids.iter() {
            let asset_id_query =
                Query::query_asset_id(&asset_id, asset_id_column, asset_id_queries)?;
            res = Some(match res {
                None => asset_id_query,
                Some(or_cond) => {
                    let new_or_cond = boolean::or(&or_cond, &asset_id_query)?;
                    Arc::new(new_or_cond)
                }
            })
        }

        Ok(res)
    }

    fn query_asset_id<'a>(
        asset_id: &'a String,
        asset_id_column: &StringArray,
        asset_id_queries: &mut HashMap<&'a String, Arc<BooleanArray>>,
    ) -> Result<Arc<BooleanArray>> {
        Ok(match asset_id_queries.get(asset_id) {
            Some(col) => Arc::clone(col),
            None => {
                let col = comparison::eq_utf8_scalar(asset_id_column, &asset_id[..])?;
                let res = Arc::new(col);
                let res_clone = Arc::clone(&res);
                asset_id_queries.insert(asset_id, res);
                res_clone
            }
        })
    }

    fn query_eff_timestamp(
        &self,
        eff_start_column: &UInt64Array,
        eff_end_column: &UInt64Array,
    ) -> Result<BooleanArray> {
        boolean::and(
            &comparison::lt_eq_scalar(eff_start_column, self.eff_timestamp)?,
            &comparison::gt_eq_scalar(eff_end_column, self.eff_timestamp)?,
        )
    }

    fn query<'a>(
        &'a self,
        batch: &RecordBatch,
        date_index: usize,
        fid_index: usize,
        eff_start_index: usize,
        eff_end_index: usize,
        value_index: usize,
        asset_id_queries: &mut HashMap<&'a String, Arc<BooleanArray>>
    ) -> Result<Option<RecordBatch>> {
        let date_column: &UInt32Array = get_column(&batch, date_index);
        let fid_column: &StringArray = get_column(&batch, fid_index);
        let eff_start_column: &UInt64Array = get_column(&batch, eff_start_index);
        let eff_end_column: &UInt64Array = get_column(&batch, eff_end_index);
        let value_column: &Float64Array = get_column(&batch, value_index);

        let asset_id_query = self.query_asset_ids(fid_column, asset_id_queries)?;
        let date_range_query = self.query_date_range(date_column)?;
        let eff_date_query = self.query_eff_timestamp(eff_start_column, eff_end_column)?;

        let selection_query = match asset_id_query {
            None => date_range_query,
            Some(asset_id_query) => boolean::and(&date_range_query, &asset_id_query)?,
        };
        let condition = boolean::and(&selection_query, &eff_date_query)?;

        let res_date = filter::filter(date_column, &condition)?;
        if res_date.len() == 0 {
            Ok(None)
        } else {
            let res_fid = filter::filter(fid_column, &condition)?;
            let res_close = filter::filter(value_column, &condition)?;

            let len = res_date.len();
            let mut build_date_column_builder = UInt32Array::builder(len);
            for _ in 0..len {
                build_date_column_builder
                    .append_value(self.build_date)
                    .unwrap();
            }
            let res_build_date = Arc::new(build_date_column_builder.finish());

            let res_schema = Schema::new(vec![
                Field::new("build_date", DataType::UInt32, false),
                Field::new("fid", DataType::Utf8, false),
                Field::new("data_date", DataType::UInt32, false),
                Field::new("close", DataType::Float64, true),
            ]);
            RecordBatch::try_new(
                Arc::new(res_schema),
                vec![res_build_date, res_fid, res_date, res_close],
            ).map(|b| Some(b))
        }
    }
}

fn get_column<T: 'static>(batch: &RecordBatch, index: usize) -> &T {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .expect("Failed to downcast")
}
