use std::convert::TryInto;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem::size_of;

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

struct PageBuffer {
    buf: Vec<u8>,
}

impl PageBuffer {
    fn new(page_size: u32, page_type: u32) -> PageBuffer {
        let mut buf = Vec::with_capacity(page_size as usize);
        for _ in 0..page_size {
            buf.push(0);
        }
        write_u32(&mut buf[0..], page_type);
        PageBuffer { buf }
    }

    fn num_keys(&self) -> u32 {
        read_u32(&self.buf[size_of::<u32>()..])
    }

    fn set_num_keys(&mut self, num_keys: u32) {
        write_u32(&mut self.buf[size_of::<u32>()..], num_keys as u32);
    }

    fn rightmost_page_num(&self) -> u32 {
        read_u32(&self.buf[2 * size_of::<u32>()..])
    }

    fn set_rightmost_page_num(&mut self, page_num: u32) {
        write_u32(&mut self.buf[2 * size_of::<u32>()..], page_num);
    }

    fn key_capacity(&self) -> usize {
        (self.buf.capacity() - 4 * size_of::<u32>()) / (size_of::<Key>() + size_of::<Value>())
    }

    fn key_offset(&self, index: usize) -> usize {
        (4 * size_of::<u32>()) + (size_of::<Key>() + size_of::<Value>()) * index
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
        let mut file = File::create(file_name)?;
        let mut file_header_buf = FileHeaderBuffer::new();
        file_header_buf.set(FileHeader {
            page_size,
            page_count: 0,
            root_offset: 0,
        });
        file.write(&file_header_buf.buf)?;

        let mut leaf_buf = PageBuffer::new(page_size, LEAF_TYPE);
        let key_capacity = leaf_buf.key_capacity();

        let mut page_count = 0;
        let mut source_empty = false;
        let mut lineage: Vec<PageBuffer> = Vec::new();

        while !source_empty {
            let mut key_count = 0;
            leaf_buf.clear();

            let page_source = source.take(key_capacity);
            for (index, (key, value)) in page_source.enumerate() {
                key_count += 1;
                leaf_buf.set_key(index, key);
                leaf_buf.set_value(index, value);
                leaf_buf.set_num_keys(key_count as u32)
            }

            if key_count < key_capacity {
                if key_count == 0 {
                    break;
                }
                source_empty = true;
            }

            let last_key = leaf_buf.get_key((leaf_buf.num_keys() - 1) as usize);
            let filled_inner_pages =
                BTree::add_to_parent(last_key, &mut page_count, 0, &mut lineage, page_size);
            leaf_buf.set_rightmost_page_num(page_count + 1);
            file.write(&leaf_buf.buf)?;

            if filled_inner_pages.is_some() {
                for page_buf in filled_inner_pages.unwrap().iter().rev() {
                    file.write(&page_buf.buf)?;
                }
            }
            page_count += 1;
        }

        for page_buf in lineage.iter() {
            file.write(&page_buf.buf)?;
        }

        file_header_buf.set(FileHeader {
            page_size,
            page_count: page_count as u32,
            root_offset: (page_count - 1) as u32,
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
                old_inner_buf.set_rightmost_page_num(*page_number);

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

    pub fn query(&self, _query: &Query) -> QueryResultIterator {
        QueryResultIterator {}
    }

    pub fn bulk_query(&self, _queries: &Vec<Query>) -> QueryResultIterator {
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
