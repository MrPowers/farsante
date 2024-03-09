import pandas as pd
import subprocess

H2O_WORKING_DIR = "h2o-data-rust"


def _test_groupby_dataset_creation(extension: str):
    command_str = "cargo run -- --n 1000 --k 10 --nas 10 --seed 591"
    # command_str = f"cargo run -- --ext {extension} --n 1000 --k 10 --nas 10 --seed 591"
    command_list = command_str.split(" ")
    subprocess.run(command_list, cwd=H2O_WORKING_DIR)
    if extension == "csv":
        generated_df = pd.read_csv("h2o-data-rust/G1_1e3_1e3_10_10.csv")
    else:
        raise ValueError(f"Unknown extension: {extension}")

    expected_df = pd.read_csv("tests/test_files/G1_1e3_1e3_10_10.csv")
    assert generated_df.shape[0] == 1000
    assert generated_df.equals(expected_df)


def test_groupby_dataset_creation_csv():
    _test_groupby_dataset_creation("csv")
