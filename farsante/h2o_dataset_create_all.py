from farsante import generate_all_h2o_datasets
import argparse


def main():
    parser = argparse.ArgumentParser(description="Optional app description")
    parser.add_argument(
        "--n", type=int, help="Number of rows to generate", required=True
    )

    parser.add_argument(
        "--k", type=int, help="Number of columns to generate", required=True
    )

    parser.add_argument(
        "--nas",
        type=int,
        help="Number of nas to generate",
        required=False,
        default=0,
    )

    parser.add_argument(
        "--seed",
        type=int,
        help="Seed for random number generator",
        required=False,
        default=42,
    )

    args = parser.parse_args()
    generate_all_h2o_datasets(n=args.n, k=args.k, nas=args.nas, seed=args.seed)


if __name__ == "__main__":
    main()
