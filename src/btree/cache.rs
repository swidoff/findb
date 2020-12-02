use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

struct Clock {
    clock: Vec<u8>,
    slots: usize,
    slot_index: usize,
}

impl Clock {
    fn new(slots: usize) -> Clock {
        let mut clock = Vec::with_capacity(slots / 8 + if slots % 8 == 0 { 0 } else { 1 });
        for _ in 0..clock.capacity() {
            clock.push(0);
        }
        Clock {
            clock,
            slots,
            slot_index: 0,
        }
    }

    fn set(&mut self, slot: usize) {
        let byte = slot / 8;
        let bit = slot % 8;
        let mask = 1 << bit;
        self.clock[byte] = self.clock[byte] | mask;
    }

    fn unset(&mut self, slot: usize) {
        let byte = slot / 8;
        let bit = slot % 8;
        let mask = 1 << bit;
        self.clock[byte] = self.clock[byte] & !mask;
    }

    fn test(&self, slot: usize) -> bool {
        let byte = slot / 8;
        let bit = slot % 8;
        let mask = 1 << bit;
        self.clock[byte] & mask != 0
    }

    fn advance(&mut self) {
        self.slot_index = (self.slot_index + 1) % self.slots;
    }

    fn evict(&mut self) -> usize {
        while self.test(self.slot_index) {
            self.unset(self.slot_index);
            self.advance();
        }

        let res = self.slot_index;
        self.advance();
        res
    }
}

pub struct PageCache {
    file: File,
    page_size: usize,
    pages: usize,
    header_bytes: u64,
    buf: Vec<u8>,
    clock: Clock,
    page_map: HashMap<usize, usize>,
    slot_map: HashMap<usize, usize>,
}

pub struct Page<'a> {
    pub buf: &'a mut [u8],
}

impl PageCache {
    pub fn new(file: File, page_size: usize, pages: usize, header_bytes: u64) -> PageCache {
        let mut buf = Vec::with_capacity(page_size * pages);
        for _ in 0..buf.capacity() {
            buf.push(0);
        }

        PageCache {
            file,
            page_size,
            pages,
            header_bytes,
            buf,
            clock: Clock::new(pages),
            page_map: HashMap::new(),
            slot_map: HashMap::new(),
        }
    }

    pub fn load(&mut self, page_number: usize) -> std::io::Result<Page> {
        match self.page_map.get(&page_number) {
            Some(slot_number) => {
                self.clock.set(*slot_number);
                self.page_from_slot(*slot_number, false)
            }
            None => {
                let slot_number = if self.page_map.len() < self.pages {
                    self.page_map.len()
                } else {
                    self.clock.evict()
                };

                self.page_map.insert(page_number, slot_number);
                self.slot_map.insert(slot_number, page_number);
                self.page_from_slot(slot_number, true)
            }
        }
    }

    fn page_from_slot(&mut self, slot_number: usize, read: bool) -> std::io::Result<Page> {
        let page_start = slot_number * self.page_size;
        let page_end = (slot_number + 1) * self.page_size;
        let buf = &mut self.buf[page_start..page_end];
        if read {
            let offset = (page_start as u64) + self.header_bytes;
            self.file.seek(SeekFrom::Start(offset))?;
            self.file.read(buf)?;
        }

        self.clock.set(slot_number);
        Ok(Page { buf })
    }
}
