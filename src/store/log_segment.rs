extern crate bytebuffer;
use self::bytebuffer::ByteBuffer;

use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::path::Path;

pub struct LogSegment {
    name: String,
    file: File,
    capacity_in_bytes: u64,
    end_offset: u64,
    start_offset: u64,

}

impl LogSegment {
    const VERSION: u8 = 0;
    //    const VERSION_HEADER_SIZE: u32 = 2;
    //    const CAPACITY_HEADER_SIZE: u32 = 8;
    //    const CRC_SIZE: u32 = 8;

    pub fn new(name: String, file: File, capacity_in_bytes: u64) -> LogSegment {
        let mut log_segment = LogSegment {
            name: name,
            file: file,
            capacity_in_bytes: capacity_in_bytes,
            start_offset: 0,
            end_offset: 0,
        };

        log_segment.write_header().unwrap();
        log_segment
    }

    pub fn restore_from(
        path: &Path,
        name: String,
        capacity_in_bytes: u64
    ) -> LogSegment {
        unimplemented!()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn capacity_in_bytes(&self) -> u64 {
        self.capacity_in_bytes
    }

    pub fn remaining_bytes(&self) -> u64 {
        self.capacity_in_bytes() - self.end_offset
    }

    fn write_header(&mut self) -> io::Result<()> {
        let mut header = ByteBuffer::new();
        header.write_u8(LogSegment::VERSION);
        header.write_u64(self.capacity_in_bytes);
        // TODO(caojiafeng): add crc

        let _ = self.write_all(&header.to_bytes())?;
        self.start_offset = self.end_offset;
        Ok(())
    }
}

impl Drop for LogSegment {
    fn drop(&mut self) {
        // TODO(caojiafeng): use `flush`?
        let _ = self.file.sync_all();
    }
}


impl Write for LogSegment {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        // write until finished
        let write_size = self.file.write(data)? as u64;
        self.end_offset += write_size;
        Ok(write_size as usize)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.sync_all()
    }

}

// TODO(caojiafeng): impl Write, Read for LogSegment
