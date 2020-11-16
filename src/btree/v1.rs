use std::convert::TryInto;
use std::fs::File;
use std::io::{Read, Write};
use std::mem::size_of;

use itertools::Itertools;

/// Super simple on-disk btree implementation with fixed-size keys and a single floating point value contained  
/// inside the node itself rather than in a separate file.

pub type AssetId = u32;
pub type Date = u32;
pub type Timestamp = u32;
pub type PageNumber = u32;
pub type Value = f32;

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
        write_u32(&mut self.buf[2 * size_of::<u32>()..], header.root_offset);
    }

    fn get(&self) -> FileHeader {
        FileHeader {
            page_size: read_u32(&self.buf[0..]),
            page_count: read_u32(&self.buf[size_of::<u32>()..]),
            root_offset: read_u32(&self.buf[2 * size_of::<u32>()..]),
        }
    }
}

const LEAF_TYPE: u32 = 0;
const INNER_TYPE: u32 = 1;

struct PageHeader {
    page_type: u32,
    num_keys: u32,
    reserved: u32,
    rightmost_offset: PageNumber,
}

struct PageBuffer {
    buf: Vec<u8>,
}

impl PageBuffer {
    fn new(page_size: u32) -> PageBuffer {
        let mut buf = Vec::with_capacity(page_size as usize);
        for _ in 0..page_size {
            buf.push(0);
        }
        PageBuffer { buf }
    }

    fn key_capacity(&self) -> usize {
        (self.buf.capacity() - size_of::<PageHeader>()) / (size_of::<Key>() + size_of::<Value>())
    }

    fn set_header(&mut self, header: PageHeader) {
        write_u32(&mut self.buf[0..], header.page_type);
        write_u32(&mut self.buf[size_of::<u32>()..], header.num_keys);
        write_u32(&mut self.buf[2 * size_of::<u32>()..], header.reserved);
        write_u32(
            &mut self.buf[3 * size_of::<u32>()..],
            header.rightmost_offset,
        );
    }

    fn get_header(&self) -> PageHeader {
        PageHeader {
            page_type: read_u32(&self.buf[0..]),
            num_keys: read_u32(&self.buf[size_of::<u32>()..]),
            reserved: read_u32(&self.buf[2 * size_of::<u32>()..]),
            rightmost_offset: read_u32(&self.buf[3 * size_of::<u32>()..]),
        }
    }

    fn key_offset(&self, index: usize) -> usize {
        size_of::<PageHeader>() + (size_of::<Key>() + size_of::<Value>()) * index
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

    fn get_key(&self, index: usize) -> Key {
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

    fn get_value(&self, index: usize) -> Value {
        read_f32(&self.buf[self.value_offset(index)..])
    }

    fn set_value(&mut self, index: usize, value: Value) {
        let offset = self.value_offset(index);
        write_f32(&mut self.buf[offset..], value)
    }

    fn get_page_number(&self, index: usize) -> PageNumber {
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
        let mut file_header_buf = FileHeaderBuffer::new();
        file_header_buf.set(FileHeader {
            page_size,
            page_count: 0,
            root_offset: 0,
        });

        let mut page_bufs = [PageBuffer::new(page_size), PageBuffer::new(page_size)];
        let key_capacity = page_bufs[0].key_capacity();

        let mut file = File::create(file_name)?;
        file.write(&file_header_buf.buf)?;

        let mut page_count = 0;
        let mut source_empty = false;
        while !source_empty {
            let mut key_count = 0;

            {
                let page_buf = &mut page_bufs[page_count % 2];
                page_buf.clear();
                let page_source = source.take(key_capacity);
                for (index, (key, value)) in page_source.enumerate() {
                    page_buf.set_key(index, key);
                    page_buf.set_value(index, value);
                    key_count += 1;
                }
                page_count += 1;
            }

            if page_count > 0 {
                let page_buf = &mut page_bufs[(page_count) - 1 % 2];
                page_buf.set_header(PageHeader {
                    page_type: LEAF_TYPE,
                    num_keys: key_capacity as u32,
                    reserved: 0,
                    rightmost_offset: (page_count - 1) as u32,
                });
                file.write(&page_buf.buf);
            }

            if key_count < key_capacity {
                if key_count > 0 {
                    let page_buf = &mut page_bufs[page_count % 2];
                    page_buf.set_header(PageHeader {
                        page_type: LEAF_TYPE,
                        num_keys: key_count as u32,
                        reserved: 0,
                        rightmost_offset: 0,
                    });
                    file.write(&page_buf.buf);
                }
                source_empty = true;
            }
        }
        return Ok(());
    }

    pub fn query(&self, query: &Query) -> QueryResultIterator {
        QueryResultIterator {}
    }

    pub fn bulk_query(&self, queries: &Vec<Query>) -> QueryResultIterator {
        QueryResultIterator {}
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
