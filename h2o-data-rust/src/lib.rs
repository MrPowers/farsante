mod generators;
mod helpers;

use crate::generators::{GroupByGenerator, RowGenerator};
use crate::helpers::generate_csv;
use crate::helpers::pretty_sci;

use kdam::{tqdm, BarExt};
use pyo3::prelude::*;

#[pyfunction]
fn generate_groupby_csv(n: u64, k: u64, nas: u8, seed: u64) -> () {
    let output_name = format!("G1_{}_{}_{}_{}.csv", pretty_sci(n), pretty_sci(n), k, nas);
    let mut pb = tqdm!(total = n as usize, position = 0);
    pb.set_postfix(format!("{}", output_name));
    let _ = pb.refresh();
    generate_csv(
        &mut GroupByGenerator::new(n, k, nas, seed),
        &output_name,
        &mut pb,
        n,
    );
}

#[pyfunction]
fn hello_rust() -> () {
    println!("Hello Rust!");
}

#[pymodule]
fn h2o_data_rust(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(generate_groupby_csv, m)?)?;
    m.add_function(wrap_pyfunction!(hello_rust, m)?)?;
    Ok(())
}
