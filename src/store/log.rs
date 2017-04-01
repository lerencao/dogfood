use std::fs;
use std::io;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::iter::Iterator;
use std::io::prelude::*;
use super::log_segment::LogSegment;
use super::log_segment_name_helper::{ self, get_segment_name };

pub struct Log {
    data_dir: String,
    capacity_in_bytes: u64,
    segment_capacity_in_bytes: u64,
    segments_by_name: HashMap<String, LogSegment>,
}

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


impl Log {
    pub fn new(
        data_dir: &str,
        total_capacity_in_bytes: u64,
        segment_capacity_in_bytes: u64
    ) -> Log {
        fs::create_dir_all(data_dir).unwrap();

        // TODO(caojiafeng): handle segment restore
        let path = Path::new(data_dir);
        let segment_files = path.ls_files(|x| x.ends_with("_log")).unwrap();

        let mut segments_by_name = HashMap::<String, LogSegment>::new();

        // create first segment
        let name = get_segment_name(0, 0);
        let filename = log_segment_name_helper::name_to_filename(&name);
        let segment_file = fs::File::create(&filename).unwrap();
        let segment = LogSegment::new(name.clone(), segment_file, segment_capacity_in_bytes);
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

    pub fn active_segment(&self) -> &mut LogSegment {
        unimplemented!()
    }

    fn load_segments(paths: Vec<PathBuf>, segment_capacity_in_bytes: u64) {
        unimplemented!()
    }
}

impl Log {
    fn should_rollover(&self, write_size: u64) -> bool {
        self.active_segment().remaining_bytes() < write_size
    }

    fn rollover(&mut self) -> io::Result<()> {
        let name = log_segment_name_helper::next_pos_name(self.active_segment().name());
        let filename = log_segment_name_helper::name_to_filename(&name);
        let segment_file = fs::File::create(&filename).unwrap();
        let segment = LogSegment::new(name.clone(), segment_file, self.segment_capacity_in_bytes);
        self.segments_by_name.insert(name, segment);
        Ok(())
    }
}

impl Write for Log {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        if self.should_rollover(data.len() as u64) {
            self.rollover().unwrap();
        }
        self.active_segment().write(data)
    }

    fn flush(&mut self) -> io::Result<()> {
        unimplemented!()
    }

}

#[cfg(test)]
mod test {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_read_dir() {}
}
