mod generators;
mod helpers;

use crate::helpers::generate_csv;
use crate::helpers::DsType;
use pyo3::prelude::*;

#[pyfunction]
fn generate_groupby_csv(
    output_name: String,
    n: u64,
    k: u64,
    nas: u8,
    seed: u64,
    ds_type: String,
) -> () {
    let ds_type = match ds_type.as_str() {
        "groupby" => DsType::GroupBy,
        "join_big" => DsType::JoinBig,
        "join_big_na" => DsType::JoinBigNa,
        "join_medium" => DsType::JoinMedium,
        "join_small" => DsType::JoinSmall,
        _ => panic!("Invalid ds_type"),
    };
    generate_csv(output_name, n, k, nas, seed, &ds_type);
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
