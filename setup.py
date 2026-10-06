import argparse
import subprocess
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"


def scale_factors(s):
    sfs = [sf.strip() for sf in s.split(",")]
    for sf in sfs:
        try:
            ok = float(sf) > 0
        except ValueError:
            ok = False
        if not ok:
            raise argparse.ArgumentTypeError(f"scale factor must be a positive number, got '{sf}'")
    return sfs


def generate(benchmark, sf):
    output_dir = DATA_DIR / f"{benchmark}-{sf}"
    output_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["tpcgen-cli", benchmark, "parquet", "-s", sf, "--output-dir", str(output_dir)],
        check=True,
    )


def main():
    parser = argparse.ArgumentParser(description="Generate TPC benchmark data.")
    subparsers = parser.add_subparsers(dest="benchmark", required=True)
    for benchmark in ["tpch", "tpcds"]:
        sub = subparsers.add_parser(benchmark, help=f"generate {benchmark} data")
        sub.add_argument("scale_factors", type=scale_factors, help="comma-separated, e.g. 1,10,100")

    args = parser.parse_args()
    for sf in args.scale_factors:
        generate(args.benchmark, sf)


if __name__ == "__main__":
    main()
