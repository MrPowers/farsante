use crate::generators::RowGenerator;
use std::io::{prelude::*, BufWriter};
use std::{fs, path::PathBuf};

use kdam::{Bar, BarExt};

pub fn pretty_sci(num: u64) -> String {
    if num == 0 {
        return "NA".to_string();
    };
    let mut digits: Vec<u8> = Vec::new();
    let mut x = num;
    while x > 0 {
        digits.push((x % 10) as u8);
        x = x / 10;
    }
    format!("{}e{}", digits.pop().unwrap_or(0), digits.len())
}

pub fn generate_csv(
    generator: &mut dyn RowGenerator,
    file_name: &str,
    pb: &mut Bar,
    n_rows: u64,
) -> () {
    let _ = fs::write(PathBuf::from(&file_name), generator.get_csv_header());
    let file = fs::OpenOptions::new()
        .append(true)
        .write(true)
        .open(file_name)
        .unwrap();

    let mut writer = BufWriter::new(file);
    for _ in 0..n_rows {
        writer
            .write(generator.get_csv_row().as_bytes())
            .expect("couldn't write to file");
        pb.update(1).unwrap();
    }
}
