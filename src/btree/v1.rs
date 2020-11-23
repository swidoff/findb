use std::cmp::{min, Ordering};
use std::convert::TryInto;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem::size_of;

/// Super simple on-disk btree implementation with fixed-size keys and a single floating point value contained  
/// inside the node itself rather than in a separate file.

/// TODO: Perform iteration in reverse, from largest to smallest date. This allows for a max_periods more naturally.
/// The leaf "rightmost-offset" will be a back pointer, rather than a forward pointer.
///
/// For ts can return the first encountered where the key timestamp is less than the query timestamp. Can terminate
/// iteration after max_periods have been yielded.

pub type AssetId = u32;
pub type Date = u32;
pub type Timestamp = u32;
pub type PageNumber = u32;
pub type Value = f32;

#[derive(PartialEq, PartialOrd, Debug)]
pub struct Key {
    asset_id: AssetId,
    date: Date,
    timestamp: Timestamp,
}

impl Key {
    fn new(asset_id: AssetId, date: Date, timestamp: Timestamp) -> Key {
        Key {
            asset_id,
            date,
            timestamp,
        }
    }
}

pub struct Query {
    id: usize,
    asset_id: AssetId,
    start_date: Date,
    end_date: Date,
    timestamp: Timestamp,
}

#[derive(PartialEq, PartialOrd, Debug)]
pub struct QueryResult {
    id: usize,
    key: Key,
    value: Value,
}

#[derive(Debug)]
struct FileHeader {
    page_size: u32,
    page_count: u32,
    root_page_number: PageNumber,
}

struct FileHeaderBuffer {
    buf: [u8; size_of::<FileHeader>()],
}

impl FileHeaderBuffer {
    fn new() -> FileHeaderBuffer {
        FileHeaderBuffer {
            buf: [0; size_of::<FileHeader>()],
        }
    }

    fn from_file(file: &mut File) -> std::io::Result<FileHeaderBuffer> {
        let mut buf = [0; size_of::<FileHeader>()];
        file.read(&mut buf).map(|_| FileHeaderBuffer { buf })
    }

    fn set(&mut self, header: FileHeader) {
        write_u32(&mut self.buf[0..], header.page_size);
        write_u32(&mut self.buf[size_of::<u32>()..], header.page_count);
        write_u32(
            &mut self.buf[2 * size_of::<u32>()..],
            header.root_page_number,
        );
    }

    fn get(&self) -> FileHeader {
        FileHeader {
            page_size: read_u32(&self.buf[0..]),
            page_count: read_u32(&self.buf[size_of::<u32>()..]),
            root_page_number: read_u32(&self.buf[2 * size_of::<u32>()..]),
        }
    }
}

const LEAF_TYPE: u32 = 0;
const INNER_TYPE: u32 = 1;

struct PageBuffer {
    buf: Vec<u8>,
}

impl PageBuffer {
    fn header_size() -> usize {
        4 * size_of::<u32>()
    }

    fn key_value_size() -> usize {
        size_of::<Key>() + size_of::<Value>()
    }

    fn page_size_for_keys(num_keys: u32) -> usize {
        PageBuffer::header_size() + (num_keys as usize) * PageBuffer::key_value_size()
    }

    fn new(page_size: u32, page_type: u32) -> PageBuffer {
        let mut buf = Vec::with_capacity(page_size as usize);
        for _ in 0..page_size {
            buf.push(0);
        }
        let mut buf = PageBuffer { buf };
        buf.set_header_field(0, page_type);
        buf
    }

    fn header_field(&self, index: usize) -> u32 {
        read_u32(&self.buf[index * size_of::<u32>()..])
    }

    fn set_header_field(&mut self, index: usize, value: u32) {
        write_u32(&mut self.buf[index * size_of::<u32>()..], value)
    }

    fn page_type(&self) -> u32 {
        self.header_field(0)
    }

    fn num_keys(&self) -> u32 {
        self.header_field(1)
    }

    fn set_num_keys(&mut self, num_keys: u32) {
        self.set_header_field(1, num_keys);
    }

    fn extra_page_num(&self) -> u32 {
        self.header_field(2)
    }

    fn set_extra_page_num(&mut self, page_num: u32) {
        self.set_header_field(2, page_num);
    }

    fn key_capacity(&self) -> usize {
        (self.buf.capacity() - PageBuffer::header_size()) / PageBuffer::key_value_size()
    }

    fn key_offset(&self, index: usize) -> usize {
        PageBuffer::header_size() + PageBuffer::key_value_size() * index
    }

    fn set_key(&mut self, index: usize, key: Key) {
        let offset = self.key_offset(index);
        write_u32(&mut self.buf[offset..], key.asset_id);
        write_u32(&mut self.buf[offset + size_of::<u32>()..], key.date);
        write_u32(
            &mut self.buf[offset + 2 * size_of::<u32>()..],
            key.timestamp,
        );
    }

    fn key(&self, index: usize) -> Key {
        let offset = self.key_offset(index);
        Key {
            asset_id: read_u32(&self.buf[offset..]),
            date: read_u32(&self.buf[offset + size_of::<u32>()..]),
            timestamp: read_u32(&self.buf[offset + 2 * size_of::<u32>()..]),
        }
    }

    fn value_offset(&self, index: usize) -> usize {
        self.key_offset(index) + size_of::<Key>()
    }

    fn value(&self, index: usize) -> Value {
        read_f32(&self.buf[self.value_offset(index)..])
    }

    fn set_value(&mut self, index: usize, value: Value) {
        let offset = self.value_offset(index);
        write_f32(&mut self.buf[offset..], value)
    }

    fn page_number(&self, index: usize) -> PageNumber {
        read_u32(&self.buf[self.value_offset(index)..])
    }

    fn set_page_number(&mut self, index: usize, page_number: PageNumber) {
        let offset = self.value_offset(index);
        write_u32(&mut self.buf[offset..], page_number)
    }

    fn clear(&mut self) {
        for i in 0..self.buf.capacity() {
            self.buf[i] = 0;
        }
    }

    fn index_of(&self, key: &Key) -> u32 {
        let mut min = 0;
        let mut max = self.num_keys();

        while min < max {
            let midpoint = (max + min) / 2;
            let midpoint_key = self.key(midpoint as usize);
            match (*key).partial_cmp(&midpoint_key).unwrap() {
                Ordering::Greater => min = midpoint + 1,
                Ordering::Less => max = midpoint,
                Ordering::Equal => {
                    if self.page_type() == LEAF_TYPE {
                        min = midpoint;
                    } else {
                        min = midpoint + 1;
                    }
                    break;
                }
            }
        }
        min
    }

    fn print(&self) {
        let page_type = self.page_type();
        println!("Page Type: {}", page_type);
        println!("Num Keys: {}", self.num_keys());
        println!("Rightmost Page Num: {}", self.extra_page_num());
        let max_keys = if page_type == LEAF_TYPE {
            self.num_keys()
        } else {
            min(self.num_keys() + 1, self.key_capacity() as u32)
        };
        for i in 0..max_keys {
            if page_type == LEAF_TYPE {
                println!(
                    "Index {}: ({:?}, {})",
                    i,
                    self.key(i as usize),
                    self.value(i as usize)
                );
            } else {
                println!(
                    "Index {}: ({:?}, {})",
                    i,
                    self.key(i as usize),
                    self.page_number(i as usize)
                );
            }
        }
    }
}

struct FileBuffer {
    file: File,
    file_header: FileHeader,
    page_buf: PageBuffer,
}

impl FileBuffer {
    fn new(file: File) -> std::io::Result<FileBuffer> {
        let mut file = file;
        let file_header_buf = FileHeaderBuffer::from_file(&mut file)?;
        let file_header = file_header_buf.get();
        let page_size = file_header.page_size;
        let page_buf = PageBuffer::new(page_size, INNER_TYPE);
        Ok(FileBuffer {
            file,
            file_header,
            page_buf,
        })
    }

    fn read_root_page(&mut self) -> std::io::Result<&PageBuffer> {
        self.read_page(self.file_header.root_page_number)
    }

    fn read_page(&mut self, page_number: u32) -> std::io::Result<&PageBuffer> {
        self.file.seek(SeekFrom::Start(
            (size_of::<FileHeader>() as u64) + ((self.file_header.page_size * page_number) as u64),
        ))?;
        self.file.read(&mut self.page_buf.buf)?;
        return Ok(&self.page_buf);
    }
}

pub struct BTree {
    file_buf: FileBuffer,
}

impl BTree {
    pub fn from_file(file: File) -> std::io::Result<BTree> {
        Ok(BTree {
            file_buf: FileBuffer::new(file)?,
        })
    }

    /// Writes a new BTree file from an iterator that returns the keys and values to be loaded in their key sorted
    /// order.
    pub fn write_from_iterator(
        file_name: &str,
        page_size: u32,
        source: &mut dyn Iterator<Item = (Key, Value)>,
    ) -> std::io::Result<()> {
        let mut file = File::create(file_name)?;
        let mut file_header_buf = FileHeaderBuffer::new();
        file_header_buf.set(FileHeader {
            page_size,
            page_count: 0,
            root_page_number: 0,
        });
        file.write(&file_header_buf.buf)?;

        let mut leaf_buf = PageBuffer::new(page_size, LEAF_TYPE);
        let key_capacity = leaf_buf.key_capacity();

        let mut page_count = 0;
        let mut last_leaf_page_num = u32::max_value();
        let mut source_empty = false;
        let mut lineage: Vec<PageBuffer> = Vec::new();

        while !source_empty {
            let mut key_count = 0;

            // Read up to a leaf's worth of keys and values.
            let page_source = source.take(key_capacity);
            for (index, (key, value)) in page_source.enumerate() {
                key_count += 1;
                leaf_buf.set_key(index, key);
                leaf_buf.set_value(index, value);
                leaf_buf.set_num_keys(key_count as u32);
            }
            leaf_buf.set_extra_page_num(last_leaf_page_num);
            last_leaf_page_num = page_count;

            if key_count > 0 {
                file.write(&leaf_buf.buf)?;

                // If we were unable to fill a leaf, this is the last iteration. Don't continue if the iterator was empty.
                if key_count < key_capacity {
                    source_empty = true;
                } else {
                    // Add the last key and the page number of the parent node, receiving any filled inner nodes.
                    let last_key = leaf_buf.key((leaf_buf.num_keys() - 1) as usize);
                    match BTree::add_to_parent(
                        last_key,
                        &mut page_count,
                        0,
                        &mut lineage,
                        page_size,
                    ) {
                        Some(filled_inner_pages) => {
                            for page_buf in filled_inner_pages.iter().rev() {
                                file.write(&page_buf.buf)?;
                                // page_buf.print();
                            }
                        }
                        _ => {}
                    }

                    leaf_buf.clear();
                }
                page_count += 1;
            } else {
                source_empty = true;
            }
        }

        // Write out any incomplete parent nodes, pushing its page number to its parent.
        for index in 0..lineage.len() {
            let page_buf = &mut lineage[index];
            let num_keys = page_buf.num_keys() as usize;
            if num_keys < page_buf.key_capacity() {
                page_buf.set_page_number(num_keys, page_count - 1)
            } else {
                page_buf.set_extra_page_num(page_count - 1)
            }
            println!("{}", page_buf.page_type());
            file.write(&page_buf.buf)?;
            // page_buf.print();

            page_count += 1;
        }

        file_header_buf.set(FileHeader {
            page_size,
            page_count: page_count as u32,
            root_page_number: (page_count - 1) as u32,
        });
        file.seek(SeekFrom::Start(0))?;
        file.write(&file_header_buf.buf)?;
        return Ok(());
    }

    fn add_to_parent(
        key: Key,
        page_number: &mut PageNumber,
        index: usize,
        lineage: &mut Vec<PageBuffer>,
        page_size: u32,
    ) -> Option<Vec<PageBuffer>> {
        if index == lineage.len() {
            let mut inner_buf = PageBuffer::new(page_size, INNER_TYPE);
            inner_buf.set_key(0, key);
            inner_buf.set_page_number(0, *page_number);
            inner_buf.set_num_keys(1);
            lineage.push(inner_buf);
            None
        } else {
            let num_keys = lineage[index].num_keys();
            let key_capacity = lineage[index].key_capacity();
            if num_keys < (key_capacity as u32) {
                let inner_buf = &mut lineage[index];
                inner_buf.set_key(num_keys as usize, key);
                inner_buf.set_page_number(num_keys as usize, *page_number);
                inner_buf.set_num_keys(num_keys + 1);
                None
            } else {
                let new_inner_buf = PageBuffer::new(page_size, INNER_TYPE);
                lineage.push(new_inner_buf);

                let mut old_inner_buf = lineage.swap_remove(index);
                old_inner_buf.set_extra_page_num(*page_number);

                *page_number += 1;
                let res = BTree::add_to_parent(key, page_number, index + 1, lineage, page_size);
                match res {
                    None => Some(vec![old_inner_buf]),
                    Some(mut vec) => {
                        vec.push(old_inner_buf);
                        Some(vec)
                    }
                }
            }
        }
    }

    pub fn query(&mut self, query: Query) -> std::io::Result<QueryResultIterator> {
        let mut page_buf = self.file_buf.read_root_page()?;

        let key = Key {
            asset_id: query.asset_id,
            date: query.end_date,
            timestamp: query.timestamp,
        };
        while page_buf.page_type() == INNER_TYPE {
            let index = page_buf.index_of(&key) as usize;
            let page_num = if index < page_buf.key_capacity() {
                page_buf.page_number(index)
            } else {
                page_buf.extra_page_num()
            };

            page_buf = self.file_buf.read_page(page_num)?;
        }

        let key_index = min(page_buf.index_of(&key), page_buf.num_keys() - 1);

        Ok(QueryResultIterator {
            file_buf: &mut self.file_buf,
            key_index: Some(key_index),
            query,
            last_yielded_date: None,
        })
    }

    fn print(&mut self) -> std::io::Result<()> {
        let file_header = &self.file_buf.file_header;
        println!("Header: {:?}", file_header);
        println!("---");
        for i in 0..file_header.page_count {
            println!("Page number: {}", i);
            self.file_buf.read_page(i)?.print();
            println!("---");
        }
        Ok(())
    }

    // pub fn bulk_query(&self, _queries: &Vec<Query>) -> QueryResultIterator {
    //     QueryResultIterator {}
    // }
}

pub struct QueryResultIterator<'a> {
    file_buf: &'a mut FileBuffer,
    key_index: Option<u32>,
    query: Query,
    last_yielded_date: Option<u32>,
}

enum QueryResultIteratorState {
    Continue,
    YieldResult(Option<QueryResult>),
}

impl<'a> Iterator for QueryResultIterator<'a> {
    type Item = std::io::Result<QueryResult>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut state = self.iterate();

        while let Ok(QueryResultIteratorState::Continue) = state {
            state = self.iterate()
        }

        match state {
            Ok(QueryResultIteratorState::YieldResult(Some(result))) => {
                self.last_yielded_date = Some(result.key.date);
                Some(Ok(result))
            }
            Ok(QueryResultIteratorState::YieldResult(None)) => None,
            Err(e) => Some(Err(e)),
            _ => None,
        }
    }
}

impl<'a> QueryResultIterator<'a> {
    fn iterate(&mut self) -> std::io::Result<QueryResultIteratorState> {
        match self.key_index {
            None if self.file_buf.page_buf.extra_page_num() == u32::max_value() => {
                Ok(QueryResultIteratorState::YieldResult(None))
            }
            None => {
                let page_num = self.file_buf.page_buf.extra_page_num();
                self.file_buf.read_page(page_num)?;

                let num_keys = self.file_buf.page_buf.num_keys();
                self.key_index = Some(num_keys - 1);
                Ok(QueryResultIteratorState::Continue)
            }
            Some(key_index) => {
                let page_buf = &self.file_buf.page_buf;
                let key = page_buf.key(key_index as usize);
                if key.asset_id < self.query.asset_id || key.date < self.query.start_date {
                    Ok(QueryResultIteratorState::YieldResult(None))
                } else {
                    self.key_index = if key_index == 0 {
                        None
                    } else {
                        Some(key_index - 1)
                    };
                    match self.last_yielded_date {
                        None if key.asset_id > self.query.asset_id
                            || key.date > self.query.end_date
                            || key.timestamp > self.query.timestamp =>
                        {
                            Ok(QueryResultIteratorState::Continue)
                        }
                        Some(d) if d == key.date || key.timestamp > self.query.timestamp => {
                            Ok(QueryResultIteratorState::Continue)
                        }
                        _ => Ok(QueryResultIteratorState::YieldResult(Some(QueryResult {
                            id: self.query.id,
                            key,
                            value: page_buf.value(key_index as usize),
                        }))),
                    }
                }
            }
        }
    }
}

fn read_u32(buf: &[u8]) -> u32 {
    let (int_bytes, _) = buf.split_at(size_of::<u32>());
    return u32::from_be_bytes(int_bytes.try_into().unwrap());
}

fn write_u32(buf: &mut [u8], source: u32) {
    buf[0..size_of::<u32>()].copy_from_slice(&source.to_be_bytes()[..])
}

fn read_f32(buf: &[u8]) -> f32 {
    let (float_bytes, _) = buf.split_at(size_of::<f32>());
    return f32::from_be_bytes(float_bytes.try_into().unwrap());
}

fn write_f32(buf: &mut [u8], source: f32) {
    buf[0..size_of::<f32>()].copy_from_slice(&source.to_be_bytes()[..])
}

#[cfg(test)]
mod tests {
    use crate::btree::v1::{BTree, Key, PageBuffer, Query, QueryResult};
    use itertools::Itertools;
    use std::fs;
    use std::fs::File;
    use std::io::Error;

    #[test]
    fn test_small() {
        let path = "test_small.db";
        match fs::remove_file(path) {
            Ok(()) => println!("Removed test file {}", path),
            _ => {}
        }

        let inputs = vec![
            (Key::new(0, 20200131, 0), 1.0),
            (Key::new(0, 20200131, 10), 2.0),
            (Key::new(0, 20200131, 20), 3.0),
            (Key::new(0, 20200229, 5), 11.0),
            (Key::new(0, 20200229, 15), 12.0),
            (Key::new(0, 20200229, 25), 13.0),
            (Key::new(0, 20200331, 10), 110.0),
            (Key::new(0, 20200331, 20), 120.0),
            (Key::new(0, 20200331, 25), 130.0),
            (Key::new(1, 20200229, 5), 21.0),
            (Key::new(1, 20200229, 15), 22.0),
            (Key::new(1, 20200229, 25), 23.0),
            (Key::new(1, 20200331, 10), 220.0),
            (Key::new(1, 20200331, 20), 220.0),
            (Key::new(1, 20200331, 25), 230.0),
            (Key::new(1, 20200430, 10), 2100.0),
            (Key::new(1, 20200430, 20), 2200.0),
            (Key::new(1, 20200430, 25), 2300.0),
        ];
        let mut iter = inputs.into_iter();
        let page_size = PageBuffer::page_size_for_keys(3);
        BTree::write_from_iterator(path, page_size as u32, &mut iter).unwrap();

        let file = File::open(path).unwrap();
        let mut btree = BTree::from_file(file).unwrap();
        btree.print();

        check_query(
            &mut btree,
            Query {
                id: 0,
                asset_id: 0,
                start_date: 20200131,
                end_date: 20200131,
                timestamp: 20,
            },
            &[3.0],
        );
        check_query(
            &mut btree,
            Query {
                id: 0,
                asset_id: 0,
                start_date: 20200131,
                end_date: 20200131,
                timestamp: 15,
            },
            &[2.0],
        );
        check_query(
            &mut btree,
            Query {
                id: 0,
                asset_id: 0,
                start_date: 20200115,
                end_date: 20200405,
                timestamp: 20,
            },
            &[120.0, 12.0, 3.0],
        );
        check_query(
            &mut btree,
            Query {
                id: 0,
                asset_id: 1,
                start_date: 20200315,
                end_date: 20200515,
                timestamp: 21,
            },
            &[2200.0, 220.0],
        );
    }

    fn check_query(btree: &mut BTree, query: Query, expected: &[f32]) {
        let actual = btree.query(query).unwrap().collect_vec();
        assert_eq!(actual.len(), expected.len());

        for (i, actual_result) in actual.iter().enumerate() {
            assert_eq!(actual_result.as_ref().unwrap().value, expected[i]);
        }
    }
}
