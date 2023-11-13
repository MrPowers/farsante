mod generators;

use std::io::{prelude::*, BufWriter};
use std::thread;
use std::{fs, path::PathBuf};

use clap::Parser;
use generators::JoinGeneratorMedium;
use kdam::{tqdm, Bar, BarExt};

use crate::generators::{GroupByGenerator, RowGenerator, JoinGeneratorBig, JoinGeneratorSmall};

/// H2O benchmark data generator. See https://github.com/h2oai/db-benchmark/tree/master/_data for details.
/// Generate four datasets. G1_1e7_1e2_0.csv - groupby dataset (1e7 points, 1e2 keys);
/// J1_1e7_1e7_NA.csv - big join dataset (1e7 points);
/// J1_1e7_1e7_5e0.csv - big join dataset (1e7 points, 5% NA in join keys);
/// J1_1e7_1e4.csv - medium join dataset (1e4 points, 1e7 base);
/// J1_1e7_1e1.csv - small join dataset (1e1 points, 1e7 base);
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

fn pretty_sci(num: u64) -> String {
    if num == 0 {
        return "NA".to_string()
    };
    let mut digits: Vec<u8> = Vec::new();
    let mut x = num;
    while x > 0 {
        digits.push((x % 10) as u8);
        x = x / 10;
    }
    format!("{}e{}", digits.pop().unwrap_or(0), digits.len())
}

fn generate_csv(
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

fn main() {
    let args = CliArgs::parse();

    let _groupby_gen = thread::spawn(move || {
        let output_name = format!(
            "G1_{}_{}_{}_{}.csv",
            pretty_sci(args.n),
            pretty_sci(args.n),
            args.k,
            args.nas
        );
        let mut pb = tqdm!(total = args.n as usize, position = 0);
        pb.set_postfix(format!("{}", output_name));
        let _ = pb.refresh();
        generate_csv(
            &mut GroupByGenerator::new(args.n, args.k, args.nas, args.seed),
            &output_name,
            &mut pb,
            args.n,
        );
    });

    let _joinbig_gen = thread::spawn(move || {
        let output_name = format!(
            "J1_{}_{}_{}.csv",
            pretty_sci(args.n),
            pretty_sci(args.n),
            "NA",
        );
        let mut pb = tqdm!(total = args.n as usize, position = 1);
        pb.set_postfix(format!("{}", output_name));
        let _ = pb.refresh();
        generate_csv(
            &mut JoinGeneratorBig::new(args.n, args.k, 0, args.seed),
            &output_name,
            &mut pb,
            args.n,
        );
    });

    let _joinbig_na_gen = thread::spawn(move || {
        let output_name = format!(
            "J1_{}_{}_{}.csv",
            pretty_sci(args.n),
            pretty_sci(args.n),
            args.nas,
        );
        let mut pb = tqdm!(total = args.n as usize, position = 2);
        pb.set_postfix(format!("{}", output_name));
        let _ = pb.refresh();
        generate_csv(
            &mut JoinGeneratorBig::new(args.n, args.k, args.nas, args.seed),
            &output_name,
            &mut pb,
            args.n,
        );
    });

    let _joinsmall_gen = thread::spawn(move || {
        let output_name = format!(
            "J1_{}_{}_{}.csv",
            pretty_sci(args.n),
            pretty_sci(args.n / 1_000_000),
            args.nas,
        );
        let mut pb = tqdm!(total = args.n as usize / 1_000_000, position = 3);
        pb.set_postfix(format!("{}", output_name));
        let _ = pb.refresh();
        generate_csv(
            &mut JoinGeneratorSmall::new(args.n, args.k, args.nas, args.seed),
            &output_name,
            &mut pb,
            args.n / 1_000_000,
        );
    });

    let _joinmedium_gen = thread::spawn(move || {
        let output_name = format!(
            "J1_{}_{}_{}.csv",
            pretty_sci(args.n),
            pretty_sci(args.n / 1_000),
            args.nas,
        );
        let mut pb = tqdm!(total = args.n as usize / 1_000, position = 4);
        pb.set_postfix(format!("{}", output_name));
        let _ = pb.refresh();
        generate_csv(
            &mut JoinGeneratorMedium::new(args.n, args.k, args.nas, args.seed),
            &output_name,
            &mut pb,
            args.n / 1_000,
        );
    });

    _groupby_gen.join().unwrap();
    _joinbig_gen.join().unwrap();
    _joinbig_na_gen.join().unwrap();
    _joinsmall_gen.join().unwrap();
    _joinmedium_gen.join().unwrap();

    println!("{}", "Done.")
}
