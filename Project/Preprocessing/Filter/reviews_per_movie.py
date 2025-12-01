#!/usr/bin/env python3

import csv
import math
import sys
import argparse
from pathlib import Path
from collections import Counter
from typing import List, Tuple, Optional


# ---------------------------------------------------------
# Function: Count reviews per movie
# ---------------------------------------------------------

def count_reviews_by_movie(
    csv_path: Path,
    top_percent: Optional[float] = None,
) -> List[Tuple[str, int]]:
    """
    Count how many reviews each movie has.

    Returns a sorted list:
        [(movie_name, number_of_reviews), ...]
    """
    counts = Counter()

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            counts[row["moviename"]] += 1

    # Sort by number_of_reviews DESC, movie_name ASC
    sorted_counts = sorted(
        counts.items(),
        key=lambda x: (-x[1], x[0])
    )

    # Top X% filter
    if top_percent is not None and 0 < top_percent < 100:
        n = len(sorted_counts)
        k = max(1, math.ceil(n * (top_percent / 100)))
        sorted_counts = sorted_counts[:k]

    return sorted_counts


# ---------------------------------------------------------
# Main CLI entry point
# ---------------------------------------------------------

def main() -> None:
    # Resolve project root = folder above "Preprocessing"
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent.parent

    default_input = project_root / "Data" / "CSV" / "reviews.csv"
    default_output = project_root / "Data" / "CSV" / "reviews_per_movie.csv"

    parser = argparse.ArgumentParser(
        description="Generate movie_name,number_of_reviews from review CSV."
    )
    parser.add_argument(
        "--input",
        "-i",
        type=Path,
        default=default_input,
        help=f"Input reviews CSV (default: {default_input})"
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=default_output,
        help=f"Output CSV path (default: {default_output})"
    )
    parser.add_argument(
        "--top",
        type=float,
        default=100,
        help="Keep only the top X%% most-reviewed movies."
    )

    args = parser.parse_args()

    rows = count_reviews_by_movie(
        csv_path=args.input,
        top_percent=args.top,
    )

    # Save output
    with args.output.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["movie_name", "number_of_reviews"])
        for movie, n in rows:
            writer.writerow([movie, n])

    print(f"✔ Wrote {len(rows)} movies to {args.output}")


if __name__ == "__main__":
    main()