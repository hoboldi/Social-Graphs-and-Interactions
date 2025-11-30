import argparse
import csv
import logging
import re
import time
from pathlib import Path

from letterboxdpy.movie import Movie

MAX_RETRIES = 3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def slugify_title(title: str) -> str:
    s = title.lower()
    s = s.replace("&", "and")
    s = s.replace("’", "").replace("'", "")
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return re.sub(r"-{2,}", "-", s).strip("-")


def join_names(items, key="name"):
    return "|".join(str(it.get(key, "")).strip() for it in items if it.get(key)) if items else ""


def extract_movie_info(movie: Movie) -> dict:
    def attr(name, default=None):
        return getattr(movie, name, default)

    trailer = attr("trailer") or {}
    details = attr("details") or []
    genres = attr("genres") or []
    cast = attr("cast") or []
    crew = attr("crew") or {}
    popular_reviews = attr("popular_reviews") or []

    details_by_type = {}
    for d in details:
        t = d.get("type")
        if t:
            details_by_type.setdefault(t, []).append(d)

    directors = crew.get("director", []) or []
    writers = (
        crew.get("writer", [])
        or crew.get("screenplay", [])
        or crew.get("screenwriter", [])
        or []
    )
    producers = crew.get("producer", []) or []

    data = {
        "slug": attr("slug", ""),
        "url": attr("url", ""),
        "letterboxd_id": attr("letterboxd_id", ""),
        "title": attr("title", ""),
        "original_title": attr("original_title", ""),
        "year": attr("year", ""),
        "runtime": attr("runtime", ""),
        "tagline": attr("tagline", ""),
        "description": attr("description", ""),
        "poster": attr("poster", ""),
        "banner": attr("banner", ""),
        "tmdb_link": attr("tmdb_link", ""),
        "imdb_link": attr("imdb_link", ""),
        "rating": attr("rating", ""),
        "num_popular_reviews": len(popular_reviews),
        "genres": join_names(genres),
        "genre_slugs": "|".join(g.get("slug", "") for g in genres if g.get("slug")),
        "studios": join_names(details_by_type.get("studio", [])),
        "countries": join_names(details_by_type.get("country", [])),
        "languages": join_names(details_by_type.get("language", [])),
        "cast": join_names(cast),
        "directors": join_names(directors),
        "writers": join_names(writers),
        "producers": join_names(producers),
        "trailer_id": trailer.get("id", ""),
        "trailer_link": trailer.get("link", ""),
        "trailer_embed_url": trailer.get("embed_url", ""),
    }
    return data


# --------------------------------------------------------------------------- #
# Progress tracking
# --------------------------------------------------------------------------- #

def progress_file_for(output_csv: Path) -> Path:
    return output_csv.with_suffix(output_csv.suffix + ".progress")


def load_progress(progress_file: Path) -> int:
    if progress_file.exists():
        try:
            return int(progress_file.read_text().strip())
        except:
            return -1
    return -1


def save_progress(progress_file: Path, index: int):
    try:
        progress_file.write_text(str(index))
    except Exception as e:
        logger.warning(f"Failed to write progress file: {e}")


# --------------------------------------------------------------------------- #
# Scraper
# --------------------------------------------------------------------------- #

def scrape_movies(input_csv: Path, output_csv: Path, delay: float):
    # Load input
    with input_csv.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    logger.info(f"Loaded {len(rows)} movies from {input_csv}")

    # Progress
    pfile = progress_file_for(output_csv)
    last_done = load_progress(pfile)
    start = last_done + 1

    if start >= len(rows):
        logger.info("All movies already scraped according to progress.")
        return

    logger.info(f"Resuming at index {start} (human index {start+1})")

    # Fields
    fieldnames = [
        "movie_name",
        "number_of_reviews",
        "slug",
        "url",
        "letterboxd_id",
        "title",
        "original_title",
        "year",
        "runtime",
        "tagline",
        "description",
        "poster",
        "banner",
        "tmdb_link",
        "imdb_link",
        "rating",
        "num_popular_reviews",
        "genres",
        "genre_slugs",
        "studios",
        "countries",
        "languages",
        "cast",
        "directors",
        "writers",
        "producers",
        "trailer_id",
        "trailer_link",
        "trailer_embed_url",
    ]

    write_header = not output_csv.exists() or last_done < 0

    # Open output for append or overwrite
    with output_csv.open("a" if output_csv.exists() else "w", encoding="utf-8", newline="") as fout:
        writer = csv.DictWriter(fout, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
            fout.flush()

        for i in range(start, len(rows)):
            human = i + 1
            movie_name = rows[i].get("movie_name") or rows[i].get("moviename") or ""

            logger.info(f"({human:04d}) Fetching '{movie_name}'")

            if not movie_name:
                logger.warning(f"({human:04d}) empty movie name → skipping")
                save_progress(pfile, i)
                continue

            slug = slugify_title(movie_name)

            # Try scraping with retries. If it doesn't work: SKIP.
            movie_data = None
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    m = Movie(slug)
                    movie_data = extract_movie_info(m)
                    break
                except Exception as e:
                    logger.warning(
                        f"({human:04d}) attempt {attempt}/{MAX_RETRIES} failed "
                        f"for '{movie_name}': {e}"
                    )
                    time.sleep(delay)

            if movie_data is None:
                logger.error(f"({human:04d}) FAILED permanently → skipping movie")
                save_progress(pfile, i)
                continue

            # Write result
            out_row = {
                "movie_name": movie_name,
                "number_of_reviews": rows[i].get("number_of_reviews", ""),
                **movie_data,
            }
            writer.writerow(out_row)
            fout.flush()

            # Update progress
            save_progress(pfile, i)

            time.sleep(delay)


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

def main():
    proj = project_root()

    default_in = proj / "Data" / "CSV" / "reviews_per_movie.csv"
    default_out = proj / "Data" / "CSV" / "movies.csv"

    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=default_in)
    parser.add_argument("--output", type=Path, default=default_out)
    parser.add_argument("--delay", type=float, default=1.0)

    args = parser.parse_args()

    print("=== Letterboxd Movie Scraper ===")
    print(f"Input:  {args.input}")
    print(f"Output: {args.output}")
    print(f"Delay:  {args.delay}s\n")

    try:
        scrape_movies(args.input, args.output, args.delay)
    except KeyboardInterrupt:
        print("\nInterrupted, exiting.")


if __name__ == "__main__":
    main()