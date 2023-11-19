from h2o_data_rust import generate_csv_py
from concurrent.futures import ProcessPoolExecutor
from itertools import repeat


def pretty_sci(n: int) -> str:
    if n == 0:
        return "NA"

    # format in scientific notation and remove +
    formatted_num = f"{n:.0e}".replace("+", "")
    e_value = int(formatted_num.split("e")[1])

    if e_value >= 10:
        return formatted_num

    elif e_value == 0:
        return formatted_num.replace("00", "0")

    elif e_value < 10:
        return formatted_num.replace("0", "")

    else:
        raise ValueError("Unexpected value following e")


def generate_h2o_dataset(ds_type: str, n: int, k: int, nas: int, seed: int) -> None:
    output_names = {
        "groupby": "G1_{n}_{n}_{k}_{nas}.csv",
        "join_big": "J1_{n}_{n}_NA.csv",
        "join_big_na": "J1_{n}_{n}_{nas}.csv",
        "join_small": "J1_{n}_{n_divided}_{nas}.csv",
        "join_medium": "J1_{n}_{n_divided}_{nas}.csv",
    }

    divisors = {
        "groupby": 1,
        "join_big": 1,
        "join_big_na": 1,
        "join_small": 1_000_000,
        "join_medium": 1_000,
    }

    n_divisor = divisors[ds_type]
    n_divided = n // n_divisor
    output_name = output_names[ds_type].format(
        n=pretty_sci(n), n_divided=pretty_sci(n_divided), k=k, nas=nas
    )

    generate_csv_py(
        output_name=output_name,
        n=n,
        k=k,
        nas=nas,
        seed=seed,
        ds_type=ds_type,
    )


def generate_all_h2o_datasets(n: int, k: int, nas: int, seed: int) -> None:
    datasets = ["groupby", "join_big", "join_big_na", "join_small", "join_medium"]

    with ProcessPoolExecutor() as executor:
        executor.map(
            generate_h2o_dataset,
            datasets,
            repeat(n),
            repeat(k),
            repeat(nas),
            repeat(seed),
        )


if __name__ == "__main__":
    generate_all_h2o_datasets(n=10_000_000, k=10, nas=10, seed=42)
