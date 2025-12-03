import re
import csv
import time
import random
import argparse
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BASE = "https://letterboxd.com/"
HEADERS = {
    # Be a good citizen: identify yourself (put your own contact if you like)
    "User-Agent": "letterboxd-research/1.0 (+support@example.com)"
}

# Known routes for the Popular Members views
SORT_PATHS = {
    "all-time": "members/popular/",
    "week":     "members/popular/this/week/",
    "month":    "members/popular/this/month/",
    "year":     "members/popular/this/year/",
}

def sleep_polite(low=0.8, high=1.6):
    time.sleep(random.uniform(low, high))

def get(url, max_retries=6, timeout=20):
    """GET with retry/backoff on 429/5xx."""
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
        except requests.RequestException:
            # Network hiccup → small backoff
            time.sleep(min(8, 2**attempt) + random.random())
            continue

        if r.status_code in (429, 502, 503, 504):
            # Server stressed or rate-limiting → exponential backoff
            time.sleep(min(16, 2**attempt) + random.random())
            continue

        r.raise_for_status()
        return r
    # Final try raises if still failing
    r.raise_for_status()

def looks_like_profile_href(href: str) -> bool:
    """
    Accept only '/username/' (exactly two slashes, trailing slash).
    Filters out '/film/...', '/members/...', etc.
    """
    return (
        isinstance(href, str)
        and href.startswith("/")
        and href.endswith("/")
        and href.count("/") == 2
        and not href.startswith("/film/")
        and not href.startswith("/list/")
        and not href.startswith("/journal/")
        and not href.startswith("/members/")
        and not href.startswith("/crew/")
        and not href.startswith("/news/")
    )

def extract_usernames(soup: BeautifulSoup):
    """
    Grab profile links from member cards. Prefer h3 → a,
    fall back to any <a> that matches '/username/'.
    """
    usernames = []

    # Primary: member cards often have <h3><a href="/user/">
    for a in soup.select("h3 a[href]"):
        href = a.get("href", "")
        if looks_like_profile_href(href):
            usernames.append(href.strip("/"))

    # Fallback: scan all anchors (keeps order; dedup later)
    if not usernames:
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            if looks_like_profile_href(href):
                usernames.append(href.strip("/"))

    # Keep order, drop dups
    seen, ordered = set(), []
    for u in usernames:
        if u not in seen:
            seen.add(u)
            ordered.append(u)
    return ordered

def iterate_popular_usernames(period="all-time", max_users=50, max_pages=20):
    """
    Yield usernames from the Popular Members pages until max_users collected.
    """
    path = SORT_PATHS.get(period, SORT_PATHS["all-time"])
    base = urljoin(BASE, path)

    page = 1
    collected = []
    while len(collected) < max_users and page <= max_pages:
        url = base if page == 1 else urljoin(base, f"page/{page}/")
        resp = get(url)
        soup = BeautifulSoup(resp.text, "html.parser")

        batch = extract_usernames(soup)
        # Stop if the page structure changed or we exhausted
        if not batch:
            break

        # Append new names only
        for u in batch:
            if u not in collected:
                collected.append(u)
                if len(collected) >= max_users:
                    break

        page += 1
        sleep_polite()

    return collected[:max_users]

def write_seed_csv(usernames, out_path="seed.csv"):
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["username", "profile_url"])
        for u in usernames:
            w.writerow([u, urljoin(BASE, f"{u}/")])

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--period", choices=["all-time", "week", "month", "year"],
                    default="all-time", help="Popularity period to scrape")
    ap.add_argument("--max", type=int, default=50,
                    help="How many usernames to collect")
    ap.add_argument("--out", type=str, default="seed.csv",
                    help="Output CSV path")
    args = ap.parse_args()

    users = iterate_popular_usernames(period=args.period, max_users=args.max)
    write_seed_csv(users, args.out)
    print(f"Wrote {len(users)} usernames to {args.out}")

if __name__ == "__main__":
    main()