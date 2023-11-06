use std::io::prelude::*;
use std::{fs, path::PathBuf};

use clap::Parser;
use kdam::tqdm;
use rand::distributions::{Distribution, Uniform};
use rand::prelude::*;
use rand_chacha::ChaCha8Rng;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct CliArgs {
    /// Number of rows
    #[arg(long)]
    n: u64,
    /// Number of keys
    #[arg(long)]
    k: u64,
    /// Number of NAs
    #[arg(long, default_value_t = 0)]
    nas: u8,
    /// Random seed
    #[arg(long, default_value_t = 108)]
    seed: u64,
}

struct RowGenerator {
    nas: u8,
    distr_k: Uniform<u64>,
    distr_nk: Uniform<u64>,
    distr_5: Uniform<u8>,
    distr_15: Uniform<u8>,
    distr_float: Uniform<f64>,
    distr_nas: Uniform<u8>,
    rng: ChaCha8Rng,
}

impl RowGenerator {
    fn new(n: u64, k: u64, nas: u8, seed: u64) -> Self {
        RowGenerator {
            nas,
            distr_k: Uniform::<u64>::try_from(1..=k).unwrap(),
            distr_nk: Uniform::<u64>::try_from(1..=(n / k)).unwrap(),
            distr_5: Uniform::<u8>::try_from(1..=5).unwrap(),
            distr_15: Uniform::<u8>::try_from(1..=15).unwrap(),
            distr_float: Uniform::<f64>::try_from(0.0..=100.0).unwrap(),
            distr_nas: Uniform::<u8>::try_from(0..=100).unwrap(),
            rng: ChaCha8Rng::seed_from_u64(seed),
        }
    }

    fn get_csv_header(&self) -> String {
        "id1,id2,id3,id4,id5,id6,v1,v2,v3\n".to_string()
    }

    fn get_csv_row(&mut self) -> String {
        format!(
            "{},{},{},{},{},{},{},{},{}",
            if self.distr_nas.sample(&mut self.rng) >= self.nas {
                format!("id{:03}", self.distr_k.sample(&mut self.rng))
            } else {
                "".to_string()
            },
            if self.distr_nas.sample(&mut self.rng) >= self.nas {
                format!("id{:03}", self.distr_k.sample(&mut self.rng))
            } else {
                "".to_string()
            },
            if self.distr_nas.sample(&mut self.rng) >= self.nas {
                format!("id{:010}", self.distr_nk.sample(&mut self.rng))
            } else {
                "".to_string()
            },
            if self.distr_nas.sample(&mut self.rng) >= self.nas {
                format!("{}", self.distr_k.sample(&mut self.rng))
            } else {
                "".to_string()
            },
            if self.distr_nas.sample(&mut self.rng) >= self.nas {
                format!("{}", self.distr_k.sample(&mut self.rng))
            } else {
                "".to_string()
            },
            if self.distr_nas.sample(&mut self.rng) >= self.nas {
                format!("{}", self.distr_nk.sample(&mut self.rng))
            } else {
                "".to_string()
            },
            self.distr_5.sample(&mut self.rng),
            self.distr_15.sample(&mut self.rng),
            self.distr_float.sample(&mut self.rng),
        )
    }
}

fn pretty_sci(num: u64) -> String {
    let mut digits: Vec<u8> = Vec::new();
    let mut x = num;
    while x > 0 {
        digits.push((x % 10) as u8);
        x = x / 10;
    }
    format!("{}e{}", digits.pop().unwrap(), digits.len())
}

fn main() {
    let args = CliArgs::parse();

    let output_file_name = format!(
        "G1_{}_{}_{}.csv",
        pretty_sci(args.n),
        pretty_sci(args.k),
        args.nas
    );
    println!("Write output to {}", output_file_name);
    let mut row_generator = RowGenerator::new(args.n, args.k, args.nas, args.seed);
    let _ = fs::write(
        PathBuf::from(&output_file_name),
        row_generator.get_csv_header(),
    );
    let mut file = fs::OpenOptions::new()
        .append(true)
        .write(true)
        .open(output_file_name)
        .unwrap();

    for _ in tqdm!(0..args.n) {
        if let Err(e) = writeln!(file, "{}", row_generator.get_csv_row()) {
            eprintln!("couldn't write to file: {}", e);
        }
    }
}
