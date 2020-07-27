use std::fs::File;
use std::sync::Arc;

use arrow::array::{BooleanArray, Float64Array, StringArray, UInt32Array, UInt64Array};
use arrow::compute::kernels::{boolean, comparison, filter};
use arrow::csv::Reader;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;
use itertools::Itertools;

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

struct Query {
    build_date: u32,
    start_date: u32,
    end_date: u32,
    eff_timestamp: u64,
    asset_ids: Vec<String>,
}

impl Query {
    fn query_date_range(&self, date_column: &UInt32Array) -> BooleanArray {
        boolean::and(
            &comparison::gt_eq_scalar(date_column, self.start_date).unwrap(),
            &comparison::lt_eq_scalar(date_column, self.end_date).unwrap(),
        )
        .unwrap()
    }

    fn query_asset_ids(&self, asset_id_column: &StringArray) -> BooleanArray {
        let mut asset_id_condition =
            comparison::eq_utf8_scalar(asset_id_column, &self.asset_ids[0][..]).unwrap();

        for id in self.asset_ids.iter().dropping(1) {
            let eq_cond = comparison::eq_utf8_scalar(asset_id_column, id).unwrap();
            asset_id_condition = boolean::or(&asset_id_condition, &eq_cond).unwrap()
        }
        asset_id_condition
    }

    fn query_eff_timestamp(
        &self,
        eff_start_column: &UInt64Array,
        eff_end_column: &UInt64Array,
    ) -> BooleanArray {
        boolean::and(
            &comparison::lt_eq_scalar(eff_start_column, self.eff_timestamp).unwrap(),
            &comparison::gt_eq_scalar(eff_end_column, self.eff_timestamp).unwrap(),
        )
        .unwrap()
    }

    pub fn query(&self, batch: &RecordBatch) -> RecordBatch {
        let date_column: &UInt32Array = get_column(&batch, 0);
        let fid_column: &StringArray = get_column(&batch, 1);
        let eff_start_column: &UInt64Array = get_column(&batch, 2);
        let eff_end_column: &UInt64Array = get_column(&batch, 3);
        let close_column: &Float64Array = get_column(&batch, 4);

        let condition = boolean::and(
            &boolean::and(
                &self.query_date_range(date_column),
                &self.query_asset_ids(fid_column),
            )
            .unwrap(),
            &self.query_eff_timestamp(eff_start_column, eff_end_column),
        )
        .unwrap();

        let res_schema = Schema::new(vec![
            Field::new("build_date", DataType::UInt32, false),
            Field::new("fid", DataType::Utf8, false),
            Field::new("data_date", DataType::UInt32, false),
            Field::new("close", DataType::Float64, true),
        ]);

        let res_date = filter::filter(date_column, &condition).unwrap();
        let res_fid = filter::filter(fid_column, &condition).unwrap();
        let res_close = filter::filter(close_column, &condition).unwrap();

        let len = res_date.len();
        let mut build_date_column_builder = UInt32Array::builder(len);
        for _ in 0..len {
            build_date_column_builder
                .append_value(self.build_date)
                .unwrap();
        }
        let res_build_date = Arc::new(build_date_column_builder.finish());

        RecordBatch::try_new(
            Arc::new(res_schema),
            vec![res_build_date, res_fid, res_date, res_close],
        )
        .unwrap()
    }
}

#[test]
fn test_query_list() {
    let batch = read_faangm_20206_close();
    let query_list = [
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

    let batch_arr = [query_list[0].query(&batch), query_list[1].query(&batch)];
    print_batches(&batch_arr[..]).unwrap()
}
