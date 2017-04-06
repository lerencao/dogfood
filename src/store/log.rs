extern crate skiplist;
use self::skiplist::SkipMap;

extern crate bytebuffer;
use self::bytebuffer::ByteBuffer;

extern crate crc;
use self::crc::crc32;

extern crate fs2;
use self::fs2::FileExt;

use std::fs;
use std::io;
use std::fmt;
use std::io::prelude::*;
use std::fs::{File, DirEntry};
use std::path::{Path, PathBuf};
use std::iter::Iterator;

use super::FILENAME_SEPARATOR;

const LOG_SUFFIX: &str = "log";

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct SegmentId {
    pos: u64,
    gen: u64,
}

impl fmt::Display for SegmentId {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "SegmentId(pos: {}, gen: {})", self.pos, self.gen)
    }
}

fn segment_id_to_filename(id: &SegmentId) -> String {
    format!("{pos}{sep}{gen}{sep}{suffix}",
            pos = id.pos,
            sep = FILENAME_SEPARATOR,
            gen = id.gen,
            suffix = LOG_SUFFIX
    )
}

fn segment_id_from_filename(filename: &str) -> SegmentId {
    let mut v = filename.split(FILENAME_SEPARATOR);
    SegmentId {
        pos: v.next().and_then(|s| s.parse::<u64>().ok()).unwrap(),
        gen: v.next().and_then(|s| s.parse::<u64>().ok()).unwrap(),
    }
}

// generate next pos segment id
fn next_segment_id_pos(id: &SegmentId) -> SegmentId {
    SegmentId {
        pos: id.pos + 1,
        gen: id.gen,
    }
}


pub struct LogSegment {
    id: SegmentId,
    capacity_in_bytes: u64,
    end_offset: u64,
    start_offset: u64,
    file: File,
}

impl LogSegment {
    const VERSION: u8 = 0;
    // const VERSION_HEADER_SIZE: u8 = 2;
    // const CAPACITY_HEADER_SIZE: u8 = 8;
    // const CRC_SIZE: u8 = 4;

    // HEADER_SIZE = VERSION_HEADER_SIZE + CAPACITY_HEADER_SIZE + CRC_SIZE
    const HEADER_SIZE: u16 = 14;

    fn new(id: SegmentId, capacity_in_bytes: u64, file: File) -> LogSegment {
        // make sure their is enough space to write header
        if capacity_in_bytes <= LogSegment::HEADER_SIZE as u64 {
            panic!("segment capacity should > {} bytes", LogSegment::HEADER_SIZE);
        }
        let mut log_segment = LogSegment {
            id: id,
            capacity_in_bytes: capacity_in_bytes,
            start_offset: 0,
            end_offset: 0,
            file: file,
        };
        log_segment.allocate(capacity_in_bytes).unwrap();
        log_segment.file.seek(io::SeekFrom::Start(0)).unwrap();
        log_segment.write_header().unwrap();
        log_segment
    }

    // FIXME(caojiafeng): the endoffset should be set correctly.
    fn restore_from(
        id: SegmentId,
        segment_file: File,
    ) -> LogSegment {
        let mut log_segment = LogSegment {
            id: id,
            capacity_in_bytes: 0,
            start_offset: 0,
            end_offset: 0,
            file: segment_file,
        };

        let mut header = [0; LogSegment::HEADER_SIZE as usize];
        log_segment.read_exact(&mut header).unwrap();
        log_segment.start_offset = log_segment.end_offset;

        let mut header_buffer = ByteBuffer::from_bytes(&header);

        let version = header_buffer.read_u8();
        match version {
            LogSegment::VERSION => {
                let capacity_in_bytes = header_buffer.read_u64();
                let crc_from_file = header_buffer.read_u32();
                let computed_crc = Self::compute_header_crc(version, capacity_in_bytes);
                if crc_from_file != computed_crc {
                    panic!(
                        "bad crc, crc_from_file: {crc_from_file}, computed_crc: {computed_crc}",
                        crc_from_file = crc_from_file,
                        computed_crc = computed_crc
                    );
                }

                log_segment.capacity_in_bytes = capacity_in_bytes;
                log_segment
            },
            _ => panic!(
                "Unknown version {} in segment {}",
                version,
                log_segment.id
            ),
        }
    }

    fn compute_header_crc(version: u8, capacity_in_bytes: u64) -> u32 {
        let mut data = ByteBuffer::new();
        data.write_u8(version);
        data.write_u64(capacity_in_bytes);

         crc32::checksum_ieee(&data.to_bytes())
    }

    fn id(&self) -> &SegmentId {
        &self.id
    }

    pub fn capacity_in_bytes(&self) -> u64 {
        self.capacity_in_bytes
    }

    pub fn remaining_bytes(&self) -> u64 {
        self.capacity_in_bytes() - self.end_offset
    }

    fn allocate(&mut self, byte_size: u64) -> io::Result<()> {
        if self.capacity_in_bytes > byte_size {
            Err(io::Error::new(io::ErrorKind::InvalidInput, "input byte_size is smaller than existed capacity size"))
        } else {
            self.file.allocate(byte_size)?;
            // self.file.set_len(byte_size)?;
            // let pos = self.file.seek(io::SeekFrom::Start(0))?;
            // assert_eq!(0, pos);
            Ok(())
        }
    }

    fn write_header(&mut self) -> io::Result<()> {
        let mut header = ByteBuffer::new();
        header.write_u8(LogSegment::VERSION);
        header.write_u64(self.capacity_in_bytes);

        let checksum = crc32::checksum_ieee(&header.to_bytes());
        header.write_u32(checksum);

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

impl Read for LogSegment {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read_size = self.file.read(buf)?;
        self.end_offset += read_size as u64;
        Ok(read_size)
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

trait PathExt {
    fn ls_files<F>(&self, predicate: F) -> io::Result<Vec<DirEntry>>
        where F: Fn(&DirEntry) -> bool;
}

impl PathExt for Path {
    fn ls_files<F>(&self, predicate: F) -> io::Result<Vec<DirEntry>>
        where F: Fn(&DirEntry) -> bool {
        let read_dir = self.read_dir()?;
        read_dir.fold(Ok(vec![]), |seq_result, entry_result| {
            seq_result.and_then(|mut seq| entry_result.map(|entry| {
                if predicate(&entry) {
                    seq.push(entry);
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
    available_space_in_bytes: u64,
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

        let path = Path::new(data_dir);
        let segment_paths: Vec<PathBuf> = path
            .ls_files(|x| x.file_name().to_str().unwrap().ends_with(LOG_SUFFIX))
            .unwrap()
            .iter()
            .map(|dir_entry| dir_entry.path())
            .collect();

        let mut segments_by_name = SkipMap::<SegmentId, LogSegment>::new();

        if segment_paths.is_empty() {
            // create first segment
            let segment_id = SegmentId { pos: 0, gen: 0 };
            let filename = segment_id_to_filename(&segment_id);
            let segment_file = fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&path.join(&filename))
                .unwrap();
            let segment = LogSegment::new(segment_id, segment_capacity_in_bytes, segment_file);
            segments_by_name.insert(segment_id, segment);
        } else {
            // load from segment files
            for segment_path in &segment_paths {
                let segment_file = fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&segment_path)
                    .unwrap();
                let segment_id = segment_id_from_filename(segment_path.file_name().and_then(|n| n.to_str()).unwrap());
                let segment = LogSegment::restore_from(
                    segment_id,
                    segment_file
                );
                // TODO(caojiafeng): handle duplicate segment_id
                segments_by_name.insert(segment_id, segment);
            }
        }

        let byte_in_use: u64 = segments_by_name.iter().fold(0u64, |acc, s| acc + s.1.capacity_in_bytes);

        if total_capacity_in_bytes < byte_in_use {
            panic!("total capacity is too small, cannot bootstrap the dir {}", data_dir);
        }
        let available_space = total_capacity_in_bytes - byte_in_use;

        Log {
            data_dir: data_dir.to_string(),
            capacity_in_bytes: total_capacity_in_bytes,
            segment_capacity_in_bytes: segment_capacity_in_bytes,
            available_space_in_bytes: available_space,
            segments_by_name: segments_by_name,
        }
    }

    fn active_segment_mut(&mut self) -> &mut LogSegment  {
        self.segments_by_name.back_mut().unwrap().1
    }

    fn active_segment(&self) -> &LogSegment {
        self.segments_by_name.back().unwrap().1
    }
}

impl Log {

    // assume the write_size is less than segment_caiacity_in_bytes
    fn should_rollover(&self, write_size: u64) -> bool {
        self.active_segment().remaining_bytes() < write_size
    }

    fn rollover(&mut self) -> io::Result<()> {
        // flush the current active segment
        self.active_segment_mut().flush()?;

        if self.available_space_in_bytes < self.segment_capacity_in_bytes {
            Err(io::Error::new(io::ErrorKind::Other, "no more space to spawn another segment"))
        } else {
            // then create new segment and make it active
            let segment_id = next_segment_id_pos(self.active_segment().id());
            let filename = segment_id_to_filename(&segment_id);
            let segment_file = fs::File::create(&filename).unwrap();
            let segment = LogSegment::new(segment_id, self.segment_capacity_in_bytes, segment_file);
            self.segments_by_name.insert(segment_id, segment);

            self.available_space_in_bytes = self.available_space_in_bytes - self.segment_capacity_in_bytes;
            Ok(())
        }
    }
}

impl Write for Log {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        if self.should_rollover(data.len() as u64) {
            self.rollover().unwrap();
        }
        self.active_segment_mut().write(data)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.active_segment_mut().flush()
    }

}

#[cfg(test)]
mod test {
    use super::Log;
    use std::env;
    use std::fs::{self, DirEntry};
    use std::io::prelude::*;
    use super::PathExt;

    #[test]
    fn test_ls_files() {
        let tmp_dir = env::temp_dir().join("test_ls_files");
        fs::create_dir(&tmp_dir).unwrap();

        fs::File::create(&tmp_dir.join("0_log")).unwrap();

        let segment_files: Vec<DirEntry> =
            tmp_dir.ls_files(|x| true).unwrap();
        assert_eq!(segment_files.len(), 1);

        fs::remove_dir_all(&tmp_dir).unwrap();
    }
}
