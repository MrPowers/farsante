use std::{fs, path::PathBuf};
use std::io::prelude::*;

use clap::Parser;
use kdam::tqdm;
use rand::distributions::{Distribution, Uniform};
use rand::prelude::*;
use rand_chacha::ChaCha8Rng;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct CliArgs {
    // Number of rows
    #[arg(long)]
    n: u64,
    // Number of keys
    #[arg(long)]
    k: u64,
    // Number of NAs
    #[arg(long, default_value_t = 0)]
    nas: u8,
    // Random seed
    #[arg(long, default_value_t = 108)]
    seed: u64,
}

struct Row {
    id1: String,
    id2: String,
    id3: String,
    id4: u64,
    id5: u64,
    id6: u64,
    v1: u8,
    v2: u8,
    v3: f64,
}

impl Row {
    fn generate(
        rng: &mut ChaCha8Rng,
        distr_k: Uniform<u64>,
        distr_nk: Uniform<u64>,
        distr_5: Uniform<u8>,
        distr_15: Uniform<u8>,
        distr_float: Uniform<f64>,
    ) -> Self {
        Row {
            id1: format!("id{:03}", distr_k.sample(rng)),
            id2: format!("id{:03}", distr_k.sample(rng)),
            id3: format!("id{:010}", distr_nk.sample(rng)),
            id4: distr_k.sample(rng),
            id5: distr_k.sample(rng),
            id6: distr_nk.sample(rng),
            v1: distr_5.sample(rng),
            v2: distr_15.sample(rng),
            v3: distr_float.sample(rng),
        }
    }

    fn get_csv_header() -> String {
        "id1,id2,id3,id4,id5,id6,v1,v2,v3\n".to_string()
    }

    fn get_csv_row(&self, nas_distr: Uniform<u8>, rng: &mut ChaCha8Rng, nas: u8) -> String {
        format!(
            "{},{},{},{},{},{},{},{},{}",
            if nas_distr.sample(rng) >= nas {
                self.id1.as_str()
            } else {
                ""
            },
            if nas_distr.sample(rng) >= nas {
                self.id2.as_str()
            } else {
                ""
            },
            if nas_distr.sample(rng) >= nas {
                self.id3.as_str()
            } else {
                ""
            },
            if nas_distr.sample(rng) >= nas {
                format!("{}", self.id4)
            } else {
                "".to_string()
            },
            if nas_distr.sample(rng) >= nas {
                format!("{}", self.id5)
            } else {
                "".to_string()
            },
            if nas_distr.sample(rng) >= nas {
                format!("{}", self.id6)
            } else {
                "".to_string()
            },
            self.v1,
            self.v2,
            self.v3
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
    let distr_k = Uniform::<u64>::try_from(1..=args.k).unwrap();
    let distr_nk = Uniform::<u64>::try_from(1..=args.k).unwrap();
    let distr_5 = Uniform::<u8>::try_from(1..=5).unwrap();
    let distr_15 = Uniform::<u8>::try_from(1..=15).unwrap();
    let distr_float = Uniform::<f64>::try_from(0.0..=100.0).unwrap();
    let nas_distr = Uniform::<u8>::try_from(0..=100).unwrap();

    let output_file_name = format!(
        "G1_{}_{}_{}.csv",
        pretty_sci(args.n),
        pretty_sci(args.k),
        args.nas
    );
    let mut rng = ChaCha8Rng::seed_from_u64(args.seed);
    println!("Write output to {}", output_file_name);
    let _ = fs::write(PathBuf::from(&output_file_name), Row::get_csv_header());
    let mut file = fs::OpenOptions::new()
        .append(true)
        .write(true)
        .open(output_file_name)
        .unwrap();

    for _ in tqdm!(0..args.n) {
        let row = Row::generate(&mut rng, distr_k, distr_nk, distr_5, distr_15, distr_float);
        if let Err(e) = writeln!(file, "{}", row.get_csv_row(nas_distr, &mut rng, args.nas)) {
            eprintln!("couldn't write to file: {}", e);
        }
    }
}
