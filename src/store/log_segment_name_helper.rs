use super::FILENAME_SEPARATOR;

// const SUFFIX: &str = &(FileNameSeparator.to_owned() + "log");

// FIXME(caojiafeng): the separator should be FileNameSeparator
const SUFFIX: &str = "_log";

pub fn get_segment_name(pos: u64, gen: u64) -> String {
    format!("{pos}{sep}{gen}", pos = pos, sep = FILENAME_SEPARATOR, gen = gen)
}

pub fn name_to_filename(name: &str) -> String {
    name.to_owned() + SUFFIX
}

pub fn next_pos_name(cur_name: &str) -> String {
    let mut s = cur_name.split(FILENAME_SEPARATOR);
    let pos = s.next().unwrap().parse::<u64>().unwrap();
    let gen = s.next().unwrap().parse::<u64>().unwrap();
    get_segment_name(pos, gen)
}

