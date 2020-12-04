use crate::btree::cache::PageCache;
use std::cmp::{min, Ordering};
use std::convert::TryInto;
use std::fs::File;
use std::io::prelude::*;
use std::io::{BufReader, Error, ErrorKind, Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::str::FromStr;

/// Super simple on-disk btree implementation with fixed-size keys and a single floating point value contained  
/// inside the node itself rather than in a separate file.

pub type AssetId = u32;
pub type Date = u32;
pub type Timestamp = u32;
pub type PageNumber = u32;
pub type Value = f32;
const U32_SIZE: usize = size_of::<u32>();

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
    pub id: usize,
    pub asset_id: AssetId,
    pub start_date: Date,
    pub end_date: Date,
    pub timestamp: Timestamp,
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
    root_page_num: PageNumber,
}

const FILE_HEADER_SIZE: usize = size_of::<FileHeader>();

struct FileHeaderBuffer {
    buf: [u8; FILE_HEADER_SIZE],
}

impl FileHeaderBuffer {
    fn new() -> FileHeaderBuffer {
        FileHeaderBuffer {
            buf: [0; FILE_HEADER_SIZE],
        }
    }

    fn from_file(file: &mut File) -> std::io::Result<FileHeaderBuffer> {
        let mut buf = [0; FILE_HEADER_SIZE];
        file.read(&mut buf).map(|_| FileHeaderBuffer { buf })
    }

    fn set(&mut self, header: FileHeader) {
        write_u32(&mut self.buf[0..], header.page_size);
        write_u32(&mut self.buf[U32_SIZE..], header.page_count);
        write_u32(&mut self.buf[2 * U32_SIZE..], header.root_page_num);
    }

    fn get(&self) -> FileHeader {
        FileHeader {
            page_size: read_u32(&self.buf[0..]),
            page_count: read_u32(&self.buf[U32_SIZE..]),
            root_page_num: read_u32(&self.buf[2 * U32_SIZE..]),
        }
    }
}

const LEAF_TYPE: u32 = 0;
const INNER_TYPE: u32 = 1;
const PAGE_HEADER_SIZE: usize = 4 * U32_SIZE;
const KEY_VALUE_SIZE: usize = size_of::<Key>() + size_of::<Value>();

fn page_size_for_keys(num_keys: u32) -> usize {
    PAGE_HEADER_SIZE + (num_keys as usize) * KEY_VALUE_SIZE
}

trait Page {
    fn buf(&self) -> &[u8];

    fn header_field(&self, index: usize) -> u32 {
        read_u32(&self.buf()[index * U32_SIZE..])
    }

    fn page_type(&self) -> u32 {
        self.header_field(0)
    }

    fn num_keys(&self) -> u32 {
        self.header_field(1)
    }

    fn extra_page_num(&self) -> u32 {
        self.header_field(2)
    }

    fn key_capacity(&self) -> usize {
        (self.buf().len() - PAGE_HEADER_SIZE) / KEY_VALUE_SIZE
    }

    fn key_offset(&self, index: usize) -> usize {
        PAGE_HEADER_SIZE + KEY_VALUE_SIZE * index
    }

    fn key(&self, index: usize) -> Key {
        let offset = self.key_offset(index);
        Key {
            asset_id: read_u32(&self.buf()[offset..]),
            date: read_u32(&self.buf()[offset + U32_SIZE..]),
            timestamp: read_u32(&self.buf()[offset + 2 * U32_SIZE..]),
        }
    }

    fn value_offset(&self, index: usize) -> usize {
        self.key_offset(index) + size_of::<Key>()
    }

    fn value(&self, index: usize) -> Value {
        read_f32(&self.buf()[self.value_offset(index)..])
    }

    fn page_number(&self, index: usize) -> PageNumber {
        read_u32(&self.buf()[self.value_offset(index)..])
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

trait MutPage: Page {
    fn mut_buf(&mut self) -> &mut [u8];

    fn set_header_field(&mut self, index: usize, value: u32) {
        write_u32(&mut self.mut_buf()[index * U32_SIZE..], value)
    }

    fn set_num_keys(&mut self, num_keys: u32) {
        self.set_header_field(1, num_keys);
    }

    fn set_extra_page_num(&mut self, page_num: u32) {
        self.set_header_field(2, page_num);
    }

    fn set_key(&mut self, index: usize, key: Key) {
        let offset = self.key_offset(index);
        write_u32(&mut self.mut_buf()[offset..], key.asset_id);
        write_u32(&mut self.mut_buf()[offset + U32_SIZE..], key.date);
        write_u32(&mut self.mut_buf()[offset + 2 * U32_SIZE..], key.timestamp);
    }

    fn set_value(&mut self, index: usize, value: Value) {
        let offset = self.value_offset(index);
        write_f32(&mut self.mut_buf()[offset..], value)
    }

    fn set_page_number(&mut self, index: usize, page_number: PageNumber) {
        let offset = self.value_offset(index);
        write_u32(&mut self.mut_buf()[offset..], page_number)
    }
}

struct PageBuffer {
    buf: Vec<u8>,
}

impl PageBuffer {
    fn new(page_size: u32, page_type: u32) -> PageBuffer {
        let mut buf = Vec::with_capacity(page_size as usize);
        for _ in 0..page_size {
            buf.push(0);
        }
        let mut buf = PageBuffer { buf };
        buf.set_header_field(0, page_type);
        buf
    }

    fn clear(&mut self) {
        for i in 0..self.buf.capacity() {
            self.buf[i] = 0;
        }
    }
}

impl Page for PageBuffer {
    fn buf(&self) -> &[u8] {
        &self.buf[..]
    }
}

impl MutPage for PageBuffer {
    fn mut_buf(&mut self) -> &mut [u8] {
        &mut self.buf[..]
    }
}

impl Page for &[u8] {
    fn buf(&self) -> &[u8] {
        self
    }
}

pub struct BTree {
    file_header: FileHeader,
    page_cache: PageCache,
}

impl BTree {
    pub fn from_file(file: File, page_cache_size: usize) -> std::io::Result<BTree> {
        let mut file = file;
        let file_header_buf = FileHeaderBuffer::from_file(&mut file)?;
        let file_header = file_header_buf.get();
        let page_size = file_header.page_size as usize;
        let page_cache = PageCache::new(file, page_size, page_cache_size, FILE_HEADER_SIZE as u64);

        Ok(BTree {
            file_header,
            page_cache,
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
            root_page_num: 0,
        });
        file.write(&file_header_buf.buf)?;

        let mut leaf_buf = PageBuffer::new(page_size, LEAF_TYPE);
        let key_capacity = leaf_buf.key_capacity();

        let mut page_count = 0;
        let mut last_leaf_page_num = u32::max_value();
        let mut lineage: Vec<PageBuffer> = Vec::new();
        let mut peekable_source = source.peekable();

        while peekable_source.peek().is_some() {
            if last_leaf_page_num < u32::max_value() {
                let last_key = leaf_buf.key(0);
                match BTree::add_to_parent(last_key, &mut page_count, 0, &mut lineage, page_size) {
                    Some(filled_inner_pages) => {
                        for page_buf in filled_inner_pages.iter().rev() {
                            file.write(&page_buf.buf)?;
                        }
                    }
                    _ => {}
                }
                page_count += 1;
                leaf_buf.clear();
            }

            // Read up to a leaf's worth of keys and values.
            let mut key_index = 0;
            while key_index < key_capacity {
                match peekable_source.next() {
                    None => break,
                    Some((key, value)) => {
                        leaf_buf.set_key(key_index, key);
                        leaf_buf.set_value(key_index, value);
                        key_index += 1;
                        leaf_buf.set_num_keys(key_index as u32);
                    }
                }
            }
            leaf_buf.set_extra_page_num(last_leaf_page_num);
            last_leaf_page_num = page_count;
            file.write(&leaf_buf.buf)?;
        }
        page_count += 1;

        // Write out any incomplete parent nodes, pushing its page number to its parent.
        for index in 0..lineage.len() {
            let last_key = leaf_buf.key(0);
            let page_buf = &mut lineage[index];
            let num_keys = page_buf.num_keys();
            page_buf.set_key(num_keys as usize, last_key);
            if num_keys < ((key_capacity - 1) as u32) {
                page_buf.set_page_number((num_keys + 1) as usize, page_count - 1);
            } else {
                page_buf.set_extra_page_num(page_count - 1);
            }
            page_buf.set_num_keys(num_keys + 1);
            println!("{}", page_buf.page_type());
            file.write(&page_buf.buf)?;
            // page_buf.print();

            page_count += 1;
        }

        file_header_buf.set(FileHeader {
            page_size,
            page_count: page_count as u32,
            root_page_num: (page_count - 1) as u32,
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
            inner_buf.set_page_number(0, *page_number);
            lineage.push(inner_buf);
            None
        } else {
            let num_keys = lineage[index].num_keys();
            let key_capacity = lineage[index].key_capacity();
            if num_keys < (key_capacity as u32) {
                let inner_buf = &mut lineage[index];
                inner_buf.set_key(num_keys as usize, key);
                if num_keys < ((key_capacity - 1) as u32) {
                    inner_buf.set_page_number((num_keys + 1) as usize, *page_number);
                } else {
                    inner_buf.set_extra_page_num(*page_number);
                }
                inner_buf.set_num_keys(num_keys + 1);
                None
            } else {
                let new_inner_buf = PageBuffer::new(page_size, INNER_TYPE);
                lineage.push(new_inner_buf);

                let old_inner_buf = lineage.swap_remove(index);

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
        let mut page_num = self.file_header.root_page_num;
        let mut page = self.page_cache.load(page_num as usize)?;

        let key = Key {
            asset_id: query.asset_id,
            date: query.end_date,
            timestamp: query.timestamp,
        };
        while page.page_type() == INNER_TYPE {
            let index = page.index_of(&key) as usize;
            page_num = if index < page.key_capacity() {
                page.page_number(index)
            } else {
                page.extra_page_num()
            };

            page = self.page_cache.load(page_num as usize)?;
        }

        let key_index = min(page.index_of(&key), page.num_keys() - 1);
        Ok(QueryResultIterator::new(
            &mut self.page_cache,
            query,
            page_num,
            key_index,
        ))
    }

    fn print(&mut self) -> std::io::Result<()> {
        let file_header = &self.file_header;
        println!("Header: {:?}", file_header);
        println!("---");
        for i in 0..file_header.page_count {
            println!("Page number: {}", i);
            self.page_cache.load(i as usize)?.print();
            println!("---");
        }
        Ok(())
    }

    // pub fn bulk_query(&self, _queries: &Vec<Query>) -> QueryResultIterator {
    //     QueryResultIterator {}
    // }
}

pub struct QueryResultIterator<'a> {
    page_cache: &'a mut PageCache,
    page_num: u32,
    key_index: Option<u32>,
    query: Query,
    last_yielded_date: Option<u32>,
    pages_read: u32,
}

enum QueryResultIteratorState {
    Continue,
    YieldResult(Option<QueryResult>),
}

// impl<'a> Iterator for QueryResultIterator<'a> {
//     type Item = std::io::Result<QueryResult>;
//
//
// }

impl<'a> QueryResultIterator<'a> {
    fn new(
        page_cache: &'a mut PageCache,
        query: Query,
        page_num: u32,
        key_index: u32,
    ) -> QueryResultIterator<'a> {
        QueryResultIterator {
            page_cache,
            page_num,
            key_index: Some(key_index),
            query,
            last_yielded_date: None,
            pages_read: 1,
        }
    }

    fn next(&mut self) -> Option<std::io::Result<QueryResult>> {
        let mut state = Ok(QueryResultIteratorState::Continue);

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

    fn iterate(&mut self) -> std::io::Result<QueryResultIteratorState> {
        let page = self.page_cache.load(self.page_num as usize)?;
        match self.key_index {
            None if page.extra_page_num() == u32::max_value() => {
                Ok(QueryResultIteratorState::YieldResult(None))
            }
            None => {
                self.page_num = page.extra_page_num();
                self.pages_read += 1;

                let page = self.page_cache.load(self.page_num as usize)?;
                let num_keys = page.num_keys();
                self.key_index = Some(num_keys - 1);
                Ok(QueryResultIteratorState::Continue)
            }
            Some(key_index) => {
                let key = page.key(key_index as usize);
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
                            value: page.value(key_index as usize),
                        }))),
                    }
                }
            }
        }
    }
}

fn read_u32(buf: &[u8]) -> u32 {
    let (int_bytes, _) = buf.split_at(U32_SIZE);
    return u32::from_be_bytes(int_bytes.try_into().unwrap());
}

fn write_u32(buf: &mut [u8], source: u32) {
    buf[0..U32_SIZE].copy_from_slice(&source.to_be_bytes()[..])
}

fn read_f32(buf: &[u8]) -> f32 {
    let (float_bytes, _) = buf.split_at(size_of::<f32>());
    return f32::from_be_bytes(float_bytes.try_into().unwrap());
}

fn write_f32(buf: &mut [u8], source: f32) {
    buf[0..size_of::<f32>()].copy_from_slice(&source.to_be_bytes()[..])
}

pub fn read_csv(file_name: &str) -> Box<dyn Iterator<Item = (Key, Value)>> {
    let file = File::open(file_name).unwrap();
    let reader = BufReader::new(file);

    Box::new(reader.lines().map(|line| {
        let line = line.unwrap();
        let mut columns = line.split(",");
        let asset_id = columns.next().map(|r| u32::from_str(r).unwrap()).unwrap();
        let date = columns.next().map(|r| u32::from_str(r).unwrap()).unwrap();
        let timestamp = columns.next().map(|r| u32::from_str(r).unwrap()).unwrap();
        let value = columns.next().map(|r| f32::from_str(r).unwrap()).unwrap();
        (Key::new(asset_id, date, timestamp), value)
    }))
}

#[cfg(test)]
mod tests {
    use crate::btree::file::{page_size_for_keys, BTree, Key, PageBuffer, Query, QueryResult};
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
        let page_size = page_size_for_keys(3);
        BTree::write_from_iterator(path, page_size as u32, &mut iter).unwrap();

        let file = File::open(path).unwrap();
        let mut btree = BTree::from_file(file, 10).unwrap();
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
            1,
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
            1,
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
            3,
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
            2,
        );
    }

    fn check_query(btree: &mut BTree, query: Query, expected: &[f32], pages_read: u32) {
        let mut iterator = btree.query(query).unwrap();

        for i in 0..expected.len() {
            match iterator.next() {
                Some(Ok(v)) => assert_eq!(v.value, expected[i]),
                _ => panic!("Iterator ran out of elements"),
            };
        }

        assert_eq!(iterator.pages_read, pages_read);
    }
}
