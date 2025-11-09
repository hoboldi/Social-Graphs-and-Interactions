import re, csv, time, random, math
from datetime import datetime
from collections import deque
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup

BASE = "https://letterboxd.com"
HEADERS = {
    "User-Agent": "letterboxd-research/1.0 (contact: you@example.com)"
}

# ---------- Helpers ----------

# To not cause high traffic
def sleep_polite(low=1.0, high=2.0):
    time.sleep(random.uniform(low, high))

# Get a page
def get(url):
    for attempt in range(5):
        r = requests.get(url, headers=HEADERS, timeout=20)
        # Backoff on server stress or rate limiting
        if r.status_code in (429, 502, 503, 504):
            delay = 2 ** attempt + random.random()
            time.sleep(delay)
            continue
        r.raise_for_status()
        return r
    r.raise_for_status()

# Normalize usernames
def norm_username_from_url(u):
    # "/filipe_furtado/" -> "filipe_furtado"
    p = urlparse(u).path.strip("/")
    return p.split("/")[0] if p else None

# Parse integers
def parse_int_like(s):
    # "29,927", "13,114", "12k" -> int
    s = (s or "").strip().lower().replace(",", "")
    m = re.match(r"(\d+(\.\d+)?)([km]?)", s)
    if not m: return None
    num = float(m.group(1))
    suf = m.group(3)
    if suf == "k": num *= 1_000
    elif suf == "m": num *= 1_000_000
    return int(num)

# Parse ratings
def stars_to_float(stars):
    # "★★★½" -> 3.5
    full = stars.count("★")
    half = 0.5 if "½" in stars else 0.0
    return full + half

# ---------- Seed: Popular Members ----------
def popular_member_urls(max_pages=50):
    urls = []
    page = 1
    while page <= max_pages:
        url = f"{BASE}/members/popular/" + ("" if page == 1 else f"page/{page}/")
        soup = BeautifulSoup(get(url).text, "html.parser")
        # Names are links in h3 blocks; profile hrefs look like "/username/"
        for a in soup.select("h3 a[href^='/']"):
            href = a.get("href", "")
            if href.count("/") == 2 and href.endswith("/"):
                urls.append(urljoin(BASE, href))
        # Stop if no "Next" link
        if not soup.find("a", string=re.compile(r"\bNext\b")):
            break
        page += 1
        sleep_polite()
    return sorted(set(urls))

# ---------- Profile ----------
def fetch_profile(username):
    url = f"{BASE}/{username}/"
    soup = BeautifulSoup(get(url).text, "html.parser")
    # Header has counts, e.g. "494 Following" / "29,927 Followers"
    def grab(label):
        el = soup.find("a", string=re.compile(fr"\b{label}\b", re.I))
        if el and el.text:
            # capture the number directly before the label
            m = re.search(r"([\d,\.kKmM]+)\s+" + re.escape(label), el.text)
            if m: return parse_int_like(m.group(1))
        return None
    following_count = grab("Following")
    followers_count = grab("Followers")
    return {
        "username": username,
        "profile_url": url,
        "followers_count": followers_count,
        "following_count": following_count,
    }

# ---------- Followers / Following (paginated) ----------
def iter_network_page(username, kind="followers", max_pages=5):
    assert kind in ("followers", "following")
    base = f"{BASE}/{username}/{kind}/"
    page = 1
    while page <= max_pages:
        url = base if page == 1 else f"{base}page/{page}/"
        soup = BeautifulSoup(get(url).text, "html.parser")
        for h3 in soup.select("h3"):
            a = h3.find("a", href=True)
            if not a: continue
            href = a["href"]
            if href.count("/") == 2 and href.endswith("/"):
                yield norm_username_from_url(href)
        # next?
        nxt = soup.find("a", string=re.compile(r"\bNext\b"))
        if not nxt: break
        page += 1
        sleep_polite()

# ---------- Reviews (paginated) ----------
def iter_reviews(username, max_pages=2):
    # Scrape first few pages of /username/reviews/
    page = 1
    while page <= max_pages:
        url = f"{BASE}/{username}/reviews/" + ("" if page == 1 else f"page/{page}/")
        soup = BeautifulSoup(get(url).text, "html.parser")
        # Each review block has film link + a line with stars and "Watched"
        for block in soup.select("li .film-detail") + soup.select("section"):
            # Film title link
            a = block.find("a", href=re.compile(r"/film/"))
            if not a: continue
            film_title = a.get_text(strip=True)
            film_url = urljoin(BASE, a.get("href"))
            # Stars before the word "Watched" or "Rewatched"
            text = block.get_text("\n", strip=True)
            m = re.search(r"([★½]+)\s+(?:Watched|Rewatched)\s+([0-9A-Za-z ,]+)", text)
            stars = m.group(1) if m else ""
            watched_date = m.group(2).strip() if m else None
            # Try to get the actual review text: often next sibling paragraph
            # Fallback: capture the trailing sentence-ish text after the date line
            review_txt = ""
            p = block.find_next("div", class_=re.compile(r"review.*|body|truncate", re.I))
            if p: review_txt = p.get_text(" ", strip=True)
            if not review_txt:
                # crude fallback from full text blob
                parts = text.split("Watched")
                review_txt = parts[-1].strip() if len(parts) > 1 else ""
            yield {
                "username": username,
                "film_title": film_title,
                "film_url": film_url,
                "rating_stars": stars,
                "rating_float": stars_to_float(stars) if stars else None,
                "watched_date": watched_date,
                "review_text": review_txt
            }
        # next page?
        if not soup.find("a", string=re.compile(r"\bOlder\b")) and not soup.find("a", string=re.compile(r"\b\d+\b")):
            break
        page += 1
        sleep_polite()

# ---------- Main crawl ----------
def crawl(target_users=10_000,
          max_edges_per_dir=75,
          seed_pages=40,
          review_pages_per_user=1):
    seen = set()
    q = deque()

    # CSV writers
    users_f = open("users.csv", "w", newline="", encoding="utf-8")
    edges_f = open("edges.csv", "w", newline="", encoding="utf-8")
    reviews_f = open("reviews.csv", "w", newline="", encoding="utf-8")
    uw = csv.DictWriter(users_f, fieldnames=["username","profile_url","followers_count","following_count","scraped_at"])
    ew = csv.DictWriter(edges_f, fieldnames=["src_username","dst_username","relation"])
    rw = csv.DictWriter(reviews_f, fieldnames=["username","film_title","film_url","rating_float","rating_stars","watched_date","review_text"])
    uw.writeheader(); ew.writeheader(); rw.writeheader()

    # Seed from Popular Members
    for u in popular_member_urls(max_pages=seed_pages):
        uname = norm_username_from_url(u)
        if uname and uname not in seen:
            seen.add(uname)
            q.append(uname)
    print(f"Seeded {len(q)} users from Popular Members")

    while q and len(seen) <= target_users:
        u = q.popleft()
        # Profile counts
        try:
            prof = fetch_profile(u)
        except Exception as e:
            print("profile error:", u, e); continue

        prof["scraped_at"] = datetime.utcnow().isoformat()
        uw.writerow(prof); users_f.flush()
        sleep_polite()

        # Reviews (first few pages)
        try:
            n_rev = 0
            for rev in iter_reviews(u, max_pages=review_pages_per_user):
                rw.writerow(rev); n_rev += 1
            if n_rev:
                reviews_f.flush()
        except Exception as e:
            print("reviews error:", u, e)
        sleep_polite()

        # Following (outgoing edges)
        try:
            cnt = 0
            for v in iter_network_page(u, "following", max_pages=math.ceil(max_edges_per_dir/30)):
                if not v: continue
                ew.writerow({"src_username": u, "dst_username": v, "relation": "follows"})
                cnt += 1
                if v not in seen and len(seen) < target_users:
                    seen.add(v); q.append(v)
                if cnt >= max_edges_per_dir: break
            if cnt: edges_f.flush()
        except Exception as e:
            print("following error:", u, e)
        sleep_polite()

        # Followers (incoming edges) — we still store as "follows" reversed
        try:
            cnt = 0
            for v in iter_network_page(u, "followers", max_pages=math.ceil(max_edges_per_dir/30)):
                if not v: continue
                ew.writerow({"src_username": v, "dst_username": u, "relation": "follows"})
                cnt += 1
                if v not in seen and len(seen) < target_users:
                    seen.add(v); q.append(v)
                if cnt >= max_edges_per_dir: break
            if cnt: edges_f.flush()
        except Exception as e:
            print("followers error:", u, e)
        sleep_polite()

    users_f.close(); edges_f.close(); reviews_f.close()
    print("Done. Users:", len(seen))

if __name__ == "__main__":
    # Default budget ~10k users, gentle rate, small per-user review slice
    crawl(
        target_users=10_000,
        max_edges_per_dir=75,       # sample per user to avoid explosion
        seed_pages=40,              # ~40 pages of Popular Members as seeds
        review_pages_per_user=1     # ~30 most recent reviews per user
    )