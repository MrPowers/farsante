mod generators;
mod helpers;

use std::thread;

use clap::Parser;

use crate::helpers::generate_csv;
use crate::helpers::pretty_sci;
use crate::helpers::DsType;
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
        generate_csv(
            output_name,
            args.n,
            args.k,
            args.nas,
            args.seed,
            &DsType::GroupBy,
        );
    });
    let _joinbig_gen = thread::spawn(move || {
        let output_name = format!(
            "J1_{}_{}_{}.csv",
            pretty_sci(args.n),
            pretty_sci(args.n),
            "NA",
        );
        generate_csv(output_name, args.n, args.k, 0, args.seed, &DsType::JoinBig);
    });

    let _joinbig_na_gen = thread::spawn(move || {
        let output_name = format!(
            "J1_{}_{}_{}.csv",
            pretty_sci(args.n),
            pretty_sci(args.n),
            args.nas,
        );
        generate_csv(
            output_name,
            args.n,
            args.k,
            args.nas,
            args.seed,
            &DsType::JoinBigNa,
        );
    });

    let _joinsmall_gen = thread::spawn(move || {
        let output_name = format!(
            "J1_{}_{}_{}.csv",
            pretty_sci(args.n),
            pretty_sci(args.n / 1_000_000),
            args.nas,
        );
        generate_csv(
            output_name,
            args.n,
            args.k,
            args.nas,
            args.seed,
            &DsType::JoinSmall,
        );
    });

    let _joinmedium_gen = thread::spawn(move || {
        let output_name = format!(
            "J1_{}_{}_{}.csv",
            pretty_sci(args.n),
            pretty_sci(args.n / 1_000),
            args.nas,
        );
        generate_csv(
            output_name,
            args.n,
            args.k,
            args.nas,
            args.seed,
            &DsType::JoinMedium,
        );
    });

    _groupby_gen.join().unwrap();
    _joinbig_gen.join().unwrap();
    _joinbig_na_gen.join().unwrap();
    _joinsmall_gen.join().unwrap();
    _joinmedium_gen.join().unwrap();

    println!("{}", "Done.")
}
