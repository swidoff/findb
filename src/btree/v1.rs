use itertools::Itertools;
use std::fs::File;
use std::io::Write;
use std::mem::size_of;

/// Super simple on-disk btree implementation with fixed-size keys and a single floating point value contained  
/// inside the node itself rather than in a separate file.

pub type AssetId = u32;
pub type Date = u32;
pub type Timestamp = u32;
pub type Value = f64;

pub struct Key {
    asset_id: AssetId,
    date: Date,
    timestamp: Timestamp,
}

pub struct Query {
    id: usize,
    asset_ids: Vec<AssetId>,
    start_date: Date,
    end_date: Date,
    timestamp: Timestamp,
}

pub struct QueryResult {
    id: usize,
    key: Key,
    value: Value,
}

pub struct QueryResultIterator {}

impl Iterator for QueryResultIterator {
    type Item = QueryResult;

    fn next(&mut self) -> Option<Self::Item> {
        unimplemented!()
    }
}

struct FileHeader {
    page_size: u32,
    page_count: u32,
    root_offset: u32,
}

impl FileHeader {
    fn write(&self, file: &mut File) {
        // TODO: There has to be a less verbose way of doing this.
        let mut buf: [u8; size_of::<FileHeader>()] = [0; size_of::<FileHeader>()];
        let mut i: usize = 0;
        for b in self.page_size.to_be_bytes().iter() {
            buf[i] = *b;
            i += 1;
        }
        for b in self.page_count.to_be_bytes().iter() {
            buf[i] = *b;
            i += 1;
        }
        for b in self.root_offset.to_be_bytes().iter() {
            buf[i] = *b;
            i += 1;
        }
        file.write(&buf);
    }
}

enum PageType {
    Leaf,
    InnerNode,
}

struct PageHeader {
    page_type: PageType,
    num_keys: u32,
}

struct Leaf {
    key_values: Vec<(Key, Value)>,
}

pub struct BTree {
    file: File,
}

impl BTree {
    pub fn from_file(file: File) -> BTree {
        BTree { file }
    }

    /// Writes a new BTree file from an iterator that returns the keys and values to be loaded in their key sorted
    /// order.
    pub fn write_from_iterator(
        file_name: &str,
        page_size: u32,
        source: &mut dyn Iterator<Item = (Key, Value)>,
    ) -> std::io::Result<()> {
        let mut file_header = FileHeader {
            page_size,
            page_count: 0,
            root_offset: 0,
        };

        let mut file = File::create(file_name)?;
        file_header.write(&mut file);

        // Fill a leaf page.
        let max_keys_per_leaf = ((page_size as usize) - size_of::<PageHeader>())
            / (size_of::<Key>() + size_of::<Value>());
        let leaf_vec = source.take(max_keys_per_leaf).collect_vec();

        // Write the page to disk.
        // Propagate pointer to parent.
        // If parent is full, write parent to disk, create new parent and add new parent ot its parent recursively.
        return Ok(());
    }

    pub fn query(&self, query: &Query) -> QueryResultIterator {
        QueryResultIterator {}
    }

    pub fn bulk_query(&self, queries: &Vec<Query>) -> QueryResultIterator {
        QueryResultIterator {}
    }
}
