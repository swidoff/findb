use memmap::{Mmap, MmapOptions};
use std::fs::File;
use std::io::{Seek, SeekFrom, Read, IoSliceMut};
use std::io;

pub struct MmapFile {
    mmap: Mmap,
    offset: u64,
}

impl MmapFile {
    pub fn new(file: File) -> MmapFile {
        let mmap = unsafe { MmapOptions::new().map(&file).unwrap() };
        MmapFile { mmap, offset: 0 }
    }

    pub fn to_arr(&self) -> &[u8] {
        &self.mmap[self.offset as usize..]
    }
}

impl Seek for MmapFile {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        match pos {
            SeekFrom::Start(offset) => self.offset = offset,
            SeekFrom::End(offset) => self.offset = (self.mmap.len() as i64 + offset) as u64,
            SeekFrom::Current(offset) => self.offset = (self.offset as i64 + offset) as u64,
        }
        Ok(self.offset)
    }
}

impl Read for MmapFile {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.to_arr().read(buf)
    }

    #[inline]
    fn read_vectored(&mut self, bufs: &mut [IoSliceMut<'_>]) -> io::Result<usize> {
        self.to_arr().read_vectored(bufs)
    }

    #[inline]
    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        self.to_arr().read_to_end(buf)
    }

    #[inline]
    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        self.to_arr().read_exact(buf)
    }
}
