"""
Main script to convert JSON data to csv.

Usage:
    python main.py
"""

import argparse
from pathlib import Path
from export_reviews_to_csv import export_reviews_to_csv


def parse_args(project_root: Path) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export Letterboxd reviews (JSON → CSV)."
    )

    default_users = project_root / "Data" / "JSON" / "State_1" / "users"
    default_output = project_root / "Data" / "CSV" / "reviews.csv"

    parser.add_argument(
        "--users_dir",
        type=str,
        default=str(default_users),
        help=f"Path to users directory. Default: {default_users}",
    )

    parser.add_argument(
        "--output_file",
        type=str,
        default=str(default_output),
        help=f"Output CSV path. Default: {default_output}",
    )

    return parser.parse_args()


def main():
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent.parent

    args = parse_args(project_root)

    users_dir = Path(args.users_dir)
    output_file = Path(args.output_file)

    print(f"Using users_dir:  {users_dir}")
    print(f"Using output_file: {output_file}")

    export_reviews_to_csv(
        users_dir=users_dir,
        output_file=output_file
    )


if __name__ == "__main__":
    main()