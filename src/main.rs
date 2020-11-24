use findb::btree::v1::{read_csv, BTree, Query};
use std::fs::File;
use std::time;
use std::time::UNIX_EPOCH;

fn main() {
    // let mut iterator = read_csv("volume-APPL-IBM-GOOG-2020.csv");
    // BTree::write_from_iterator("volume-APPL-IBM-GOOG-2020.db", 1024, &mut iterator).unwrap();

    let mut file = File::open("volume-APPL-IBM-GOOG-2020.db").unwrap();
    let mut btree = BTree::from_file(file).unwrap();
    let iterator = btree.query(Query {
        id: 0,
        asset_id: 1,
        start_date: 20201001,
        end_date: 20201031,
        timestamp: time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as u32,
    });

    for result in iterator.unwrap() {
        println!("{:?}", result.unwrap())
    }
}

fn print_result(res: Result<usize, usize>) {
    match res {
        Ok(v) => println!("Ok({})", v),
        Err(v) => println!("Err({})", v),
    }
}
