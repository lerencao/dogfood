extern crate skiplist;
use self::skiplist::SkipMap;

extern crate bytebuffer;
use self::bytebuffer::ByteBuffer;

use std::fs;
use std::io;
use std::io::prelude::*;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::iter::Iterator;

use super::FILENAME_SEPARATOR;

const SUFFIX: &str = "_log";

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct SegmentId {
    pos: u64,
    gen: u64,
}

fn segment_id_to_filename(id: &SegmentId) -> String {
    format!("{pos}{sep}{gen}{suffix}",
            pos = id.pos,
            sep = FILENAME_SEPARATOR,
            gen = id.gen,
            suffix = SUFFIX
    )
}

fn next_segment_id_pos(id: &SegmentId) -> SegmentId {
    SegmentId {
        pos: id.pos + 1,
        gen: id.gen,
    }
}


pub struct LogSegment {
    // name: String,
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

    pub fn new(file: File, capacity_in_bytes: u64) -> LogSegment {
        let mut log_segment = LogSegment {
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
        capacity_in_bytes: u64
    ) -> LogSegment {
        unimplemented!()
    }

    // pub fn name(&self) -> &str {
    //     &self.name
    // }

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
        // TODO(caojiafeng): use `sync_all`?
        let _ = self.file.flush();
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
        self.file.flush()
    }

}

// TODO(caojiafeng): impl Read for LogSegment

trait PathExt {
    fn ls_files<F>(&self, preficate: F) -> io::Result<Vec<PathBuf>>
        where F: Fn(&Path) -> bool;
}

impl PathExt for Path {
    fn ls_files<F>(&self, predicate: F) -> io::Result<Vec<PathBuf>>
        where F: Fn(&Path) -> bool {
        let read_dir = self.read_dir()?;
        read_dir.fold(Ok(vec![]), |seq_result, entry_result| {
            seq_result.and_then(|mut seq| entry_result.map(|entry| {
                if predicate(entry.path().as_path()) {
                    seq.push(entry.path());
                }
                seq
            }))
        })
    }
}


pub struct Log {
    data_dir: String,
    capacity_in_bytes: u64,
    segment_capacity_in_bytes: u64,
    // TODO(caojiafeng): verify that the access to active_segment is O(1)
    segments_by_name: SkipMap<SegmentId, LogSegment>,
}

impl Log {
    pub fn new(
        data_dir: &str,
        total_capacity_in_bytes: u64,
        segment_capacity_in_bytes: u64
    ) -> Log {
        fs::create_dir_all(data_dir).unwrap();

        // TODO(caojiafeng): handle segment restore
        // let path = Path::new(data_dir);
        // let segment_files = path.ls_files(|x| x.ends_with("_log")).unwrap();

        let mut segments_by_name = SkipMap::<SegmentId, LogSegment>::new();

        // create first segment
        let name = SegmentId { pos: 0, gen: 0 };
        let filename = segment_id_to_filename(&name);
        let segment_file = fs::File::create(&filename).unwrap();
        let segment = LogSegment::new(segment_file, segment_capacity_in_bytes);
        segments_by_name.insert(name, segment);
            // Self::load_segments(segment_files, segment_capacity_in_bytes);

        let log = Log {
            data_dir: data_dir.to_string(),
            capacity_in_bytes: total_capacity_in_bytes,
            segment_capacity_in_bytes: segment_capacity_in_bytes,
            segments_by_name: segments_by_name,
        };
        log
    }

    fn active_segment_mut(&mut self) -> (&SegmentId, &mut LogSegment)  {
        self.segments_by_name.back_mut().unwrap()
    }

    fn active_segment(&self) -> (&SegmentId, &LogSegment) {
        self.segments_by_name.back().unwrap()
    }

    fn load_segments(paths: Vec<PathBuf>, segment_capacity_in_bytes: u64) {
        unimplemented!()
    }
}

impl Log {
    fn should_rollover(&self, write_size: u64) -> bool {
        self.active_segment().1.remaining_bytes() < write_size
    }

    fn rollover(&mut self) -> io::Result<()> {
        let name = next_segment_id_pos(self.active_segment().0);
        let filename = segment_id_to_filename(&name);
        let segment_file = fs::File::create(&filename).unwrap();
        let segment = LogSegment::new(segment_file, self.segment_capacity_in_bytes);
        self.segments_by_name.insert(name, segment);
        Ok(())
    }
}

impl Write for Log {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        if self.should_rollover(data.len() as u64) {
            self.rollover().unwrap();
        }
        self.active_segment_mut().1.write(data)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.active_segment_mut().1.flush()
    }

}

#[cfg(test)]
mod test {
    #[test]
    fn test_read_dir() {}
}
