from h2o_data_rust import generate_groupby_csv


def pretty_sci(n: int) -> str:
    if n == 0:
        return "NA"

    formatted_num = f"{n:.0e}".replace("+", "")
    e_value = int(formatted_num.split("e")[1])
    if e_value < 10:
        formatted_num = formatted_num.replace("0", "")
    return formatted_num


def generate_dataset(dataset_type: str, n: int, k: int, nas: int, seed: int) -> None:
    if dataset_type == "groupby":
        output_name = f"G1_{pretty_sci(n)}_{pretty_sci(n)}_{k}_{nas}.csv"
        generate_groupby_csv(output_name, n, k, nas, seed)


generate_dataset(dataset_type="groupby", n=1000000, k=10, nas=10, seed=10)
