import argparse

from farsante import generate_all_h2o_datasets


def main():
    parser = argparse.ArgumentParser(description="Generate all h2o datasets")
    parser.add_argument(
        "--n", type=int, help="Number of rows to generate", required=True
    )

    parser.add_argument(
        "--k", type=int, help="Number of unique keys in dataset", required=True
    )

    parser.add_argument(
        "--nas",
        type=int,
        help="Percent of null values to generate (0-100) [default: 0]",
        required=False,
        default=0,
    )

    parser.add_argument(
        "--seed",
        type=int,
        help="Seed for random number generator [default: 42]",
        required=False,
        default=42,
    )

    args = parser.parse_args()
    generate_all_h2o_datasets(n=args.n, k=args.k, nas=args.nas, seed=args.seed)


if __name__ == "__main__":
    main()
