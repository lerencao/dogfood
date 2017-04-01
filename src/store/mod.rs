pub use self::log_segment::LogSegment;
pub use self::log::Log;


mod log_segment;
mod log_segment_name_helper;
mod log;

const FILENAME_SEPARATOR: &str = "_";
