use std::cmp::Ordering;
use std::convert::TryInto;
use std::fs::File;
use std::io::{Error, Read, Seek, SeekFrom, Write};
use std::mem::size_of;

/// Super simple on-disk btree implementation with fixed-size keys and a single floating point value contained  
/// inside the node itself rather than in a separate file.

pub type AssetId = u32;
pub type Date = u32;
pub type Timestamp = u32;
pub type PageNumber = u32;
pub type Value = f32;

#[derive(PartialEq, PartialOrd)]
pub struct Key {
    asset_id: AssetId,
    date: Date,
    timestamp: Timestamp,
}

pub struct Query {
    id: usize,
    asset_id: AssetId,
    start_date: Date,
    end_date: Date,
    timestamp: Timestamp,
}

pub struct QueryResult {
    id: usize,
    key: Key,
    value: Value,
}

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
    fn new(page_size: u32, page_type: u32) -> PageBuffer {
        let mut buf = Vec::with_capacity(page_size as usize);
        for _ in 0..page_size {
            buf.push(0);
        }
        write_u32(&mut buf[0..], page_type);
        PageBuffer { buf }
    }

    fn page_type(&self) -> u32 {
        read_u32(&self.buf[0..])
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
            let mut midpoint = (max + min) / 2;
            let midpoint_key = self.get_key(midpoint as usize);
            match (*key).partial_cmp(&midpoint_key).unwrap() {
                Ordering::Greater => min = midpoint + 1,
                Ordering::Less => max = midpoint,
                Ordering::Equal => {
                    min = midpoint;
                    break;
                }
            }
        }
        min
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
            (self.file_header.page_size * page_number) as u64,
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
        let mut source_empty = false;
        let mut lineage: Vec<PageBuffer> = Vec::new();

        while !source_empty {
            let mut key_count = 0;
            leaf_buf.clear();

            // Read up to a leaf's worth of keys and values.
            let page_source = source.take(key_capacity);
            for (index, (key, value)) in page_source.enumerate() {
                key_count += 1;
                leaf_buf.set_key(index, key);
                leaf_buf.set_value(index, value);
                leaf_buf.set_num_keys(key_count as u32);
                leaf_buf.set_num_keys(key_count as u32);
            }

            // If we were unable to fill a leaf, this is the last iteration. Don't continue if the iterator was empty.
            if key_count < key_capacity {
                if key_count == 0 {
                    break;
                }
                source_empty = true;
            }

            // Add the last key and the page number of the parent node, receiving any filled inner nodes.
            let last_key = leaf_buf.get_key((leaf_buf.num_keys() - 1) as usize);
            let filled_inner_pages =
                BTree::add_to_parent(last_key, &mut page_count, 0, &mut lineage, page_size);

            // Page count now includes the filled inner pages to be written. The next page will be the next leaf.
            // We can set the right-pointer on the leaf to the number of the next page.
            if !source_empty {
                leaf_buf.set_rightmost_page_num(page_count + 1);
            }
            file.write(&leaf_buf.buf)?;

            // Write out the filled inner pages.
            if filled_inner_pages.is_some() {
                for page_buf in filled_inner_pages.unwrap().iter().rev() {
                    file.write(&page_buf.buf)?;
                }
            }
            page_count += 1;
        }

        // Write out any incomplete parent nodes, pushing its page number to its parent.
        for index in 0..lineage.len() {
            let page_buf = &lineage[index];
            file.write(&page_buf.buf)?;
            page_count += 1;

            let parent_buf = &mut lineage[index];
            let num_parent_keys = parent_buf.num_keys() as usize;
            if num_parent_keys < parent_buf.key_capacity() {
                parent_buf.set_page_number(num_parent_keys, page_count)
            } else {
                parent_buf.set_rightmost_page_num(page_count)
            }
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

    pub fn query(&mut self, query: Query) -> std::io::Result<QueryResultIterator> {
        let mut page_buf = self.file_buf.read_root_page()?;

        let key = Key {
            asset_id: query.asset_id,
            date: query.start_date,
            timestamp: query.timestamp,
        };
        while page_buf.page_type() == LEAF_TYPE {
            let index = page_buf.index_of(&key);
            let mut page_num = 0;

            if index < page_buf.num_keys() {
                page_num = page_buf.page_number(index as usize);
            } else {
                page_num = page_buf.rightmost_page_num();
            }

            page_buf = self.file_buf.read_page(page_num)?;
        }

        let key_index = page_buf.index_of(&key);

        Ok(QueryResultIterator {
            file_buf: &mut self.file_buf,
            key_index,
            query,
        })
    }

    // pub fn bulk_query(&self, _queries: &Vec<Query>) -> QueryResultIterator {
    //     QueryResultIterator {}
    // }
}

pub struct QueryResultIterator<'a> {
    file_buf: &'a mut FileBuffer,
    key_index: u32,
    query: Query,
}

enum QueryResultIteratorState {
    Continue(Option<QueryResult>),
    YieldResult(Option<QueryResult>),
}

impl<'a> Iterator for QueryResultIterator<'a> {
    type Item = std::io::Result<QueryResult>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut state = self.iterate(None);

        while let Ok(QueryResultIteratorState::Continue(prior_result)) = state {
            state = self.iterate(prior_result)
        }

        match state {
            Ok(QueryResultIteratorState::YieldResult(result)) => result.map(|v| Ok(v)),
            Err(e) => Some(Err(e)),
            _ => None,
        }
    }
}

impl<'a> QueryResultIterator<'a> {
    fn iterate(
        &mut self,
        prior_result: Option<QueryResult>,
    ) -> std::io::Result<QueryResultIteratorState> {
        let page_buf = &self.file_buf.page_buf;
        let new_state = if page_buf.page_type() == INNER_TYPE {
            QueryResultIteratorState::YieldResult(prior_result)
        } else if self.key_index > page_buf.num_keys() {
            let next_page_number = page_buf.rightmost_page_num();
            if next_page_number == 0 {
                QueryResultIteratorState::YieldResult(prior_result)
            } else {
                self.file_buf.read_page(next_page_number)?;
                self.key_index = 0;
                QueryResultIteratorState::Continue(prior_result)
            }
        } else {
            let key = page_buf.get_key(self.key_index as usize);
            if key.asset_id != self.query.asset_id || key.date > self.query.end_date {
                QueryResultIteratorState::YieldResult(prior_result)
            } else {
                let new_state = match prior_result {
                    Some(prior_result) if key.date > prior_result.key.date => {
                        QueryResultIteratorState::YieldResult(Some(prior_result))
                    }
                    _ if key.timestamp >= self.query.timestamp => {
                        QueryResultIteratorState::Continue(Some(QueryResult {
                            id: self.query.id,
                            key,
                            value: page_buf.value(self.key_index as usize),
                        }))
                    }
                    _ => QueryResultIteratorState::Continue(None),
                };
                self.key_index += 1;
                new_state
            }
        };
        Ok(new_state)
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
