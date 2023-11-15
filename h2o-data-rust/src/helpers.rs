use crate::generators::{
    GroupByGenerator, JoinGeneratorBig, JoinGeneratorMedium, JoinGeneratorSmall, RowGenerator,
};
use std::io::{prelude::*, BufWriter};
use std::{fs, path::PathBuf};

use kdam::{tqdm, BarExt};

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

pub enum DsType {
    GroupBy,
    JoinBig,
    JoinBigNa,
    JoinSmall,
    JoinMedium,
}

pub fn generate_csv(
    output_name: String,
    n: u64,
    k: u64,
    nas: u8,
    seed: u64,
    ds_type: &DsType,
) -> () {
    // initialize an instance of rowgenerator depending on the ds_type
    let mut generator: Box<dyn RowGenerator> = match ds_type {
        DsType::GroupBy => Box::new(GroupByGenerator::new(n, k, nas, seed)),
        DsType::JoinBig => Box::new(JoinGeneratorBig::new(n, k, nas, seed)),
        DsType::JoinBigNa => Box::new(JoinGeneratorBig::new(n, k, nas, seed)),
        DsType::JoinMedium => Box::new(JoinGeneratorMedium::new(n, k, nas, seed)),
        DsType::JoinSmall => Box::new(JoinGeneratorSmall::new(n, k, nas, seed)),
    };

    let n_divisor = match ds_type {
        DsType::GroupBy => 1,
        DsType::JoinBig => 1,
        DsType::JoinBigNa => 1,
        DsType::JoinMedium => 1_000,
        DsType::JoinSmall => 1_000_000,
    };

    // let bar_position = match ds_type {
    //     DsType::GroupBy => 0,
    //     DsType::JoinBig => 1,
    //     DsType::JoinBigNa => 2,
    //     DsType::JoinMedium => 3,
    //     DsType::JoinSmall => 4,
    // };

    let bar_position = 0;

    let n_rows = n / n_divisor;
    let mut pb = tqdm!(total = n_rows as usize, position = bar_position);
    pb.set_postfix(format!("{}", output_name));
    let _ = pb.refresh();

    let _ = fs::write(PathBuf::from(&output_name), generator.get_csv_header());
    let file = fs::OpenOptions::new()
        .append(true)
        .write(true)
        .open(output_name)
        .unwrap();

    let mut writer = BufWriter::new(file);
    for _ in 0..n_rows {
        writer
            .write(generator.get_csv_row().as_bytes())
            .expect("couldn't write to file");
        pb.update(1).unwrap();
    }
}
