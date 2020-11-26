use std::collections::HashMap;
use std::fs::File;

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

struct PageCache {
    file: File,
    page_size: usize,
    pages: usize,
    buf: Vec<u8>,
    clock: Clock,
    page_map: HashMap<usize, usize>,
    slot_map: HashMap<usize, usize>,
}

struct Page<'a> {
    buf: &'a [u8],
}

impl PageCache {
    fn new(file: File, page_size: usize, pages: usize) -> PageCache {
        let mut buf = Vec::with_capacity(page_size * pages);
        for i in 0..buf.capacity() {
            buf.push(0);
        }

        PageCache {
            file,
            page_size,
            pages,
            buf,
            clock: Clock::new(pages),
            page_map: HashMap::new(),
            slot_map: HashMap::new(),
        }
    }

    fn load(&mut self, page_number: usize) -> std::io::Result<Page> {
        match self.page_map.get(&page_number) {
            Some(slot_number) => {
                let page_start = slot_number * self.page_size;
                let page_end = (slot_number + 1) * self.page_size;
                let buf = &self.buf[page_start..page_end];
                self.clock.set(*slot_number);
                Ok(Page { buf })
            }
            None => {}
        }
    }
}
