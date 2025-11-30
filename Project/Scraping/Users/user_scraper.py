import os
import sys
import csv
import time
import random
import heapq, re, json
from typing import List, Tuple, Set, Iterable, Optional

from letterboxdpy import user as lb_user
from letterboxdpy.utils.utils_file import (
    build_path,
    check_and_create_dirs,
    save_json,
    build_click_url,
)

# -------- Parameters --------
MAX_NEW_PER_USER = 200
LOOKUP_FOLLOWER_COUNT_FOR_NEW = True
PROCESSED_LIMIT = 10000

# -------- HELPERS --------
def sleep_polite(low=1, high=2):
    time.sleep(random.uniform(low, high))

def _maybe_int(x) -> Optional[int]:
    try:
        if isinstance(x, int):
            return x
        if isinstance(x, str) and x.isdigit():
            return int(x)
    except Exception:
        pass
    return None

# ---- Priority Queue HELPERS ---------
Number = int
PQItem = Tuple[int, str, str]

def _to_int(s: str) -> Number:
    s = (s or "").strip()
    if not s:
        return 0
    s = re.sub(r"[^\d]", "", s)
    return int(s) if s.isdigit() else 0

def read_seed_pq(path: str = "seed.csv") -> List[PQItem]:
    pq: List[PQItem] = []
    if not os.path.exists(path):
        return pq
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            u = (row.get("username") or "").strip().strip("/")
            if not u:
                continue
            url = (row.get("profile_url") or f"https://letterboxd.com/{u}/").strip()
            raw_following = row.get("following") or row.get("following_count") or "0"
            try:
                prio = int(str(raw_following).replace(",", ""))
            except ValueError:
                try:
                    prio = int(str(row.get("followers_count", "0")).replace(",", ""))
                except Exception:
                    prio = 0
            heapq.heappush(pq, (-prio, u, url))
    return pq

def pq_pop(pq: List[PQItem]) -> Tuple[str, int, str]:
    neg_f, u, p = heapq.heappop(pq)
    return u, -neg_f, p

def pq_peek(pq: List[PQItem]) -> Tuple[str, int, str]:
    neg_f, u, p = pq[0]
    return u, -neg_f, p

def extract_followers_count(u: lb_user.User) -> Optional[int]:
    for attr in ("followers_count", "follower_count"):
        if hasattr(u, attr):
            v = _maybe_int(getattr(u, attr))
            if v is not None and v >= 0:
                return v
    try:
        data = u.jsonify()
        if isinstance(data, dict):
            for k in ("followers_count", "followerCount", "followers"):
                v = _maybe_int(data.get(k))
                if v is not None:
                    return v
            for blk in ("counts", "stats", "statistics", "profile", "meta"):
                sub = data.get(blk)
                if isinstance(sub, dict):
                    for k in ("followers", "followers_count", "followerCount"):
                        v = _maybe_int(sub.get(k))
                        if v is not None:
                            return v
    except Exception:
        pass
    try:
        lst = lb_user.User.get_followers(u)
        return len(lst) if lst is not None else 0
    except Exception:
        return None

# --- Resume helpers ------------------------------------------------------

def _load_json_if_exists(path_no_ext: str):
    fp = f"{path_no_ext}.json"
    if not os.path.exists(fp):
        return None
    with open(fp, "r", encoding="utf-8") as f:
        return json.load(f)

def _save_checkpoint(state_dir: str, pq: List[PQItem], exported: Set[str], enqueued: Set[str]):
    # Convert to JSON-serializable forms
    pq_list = [[neg, u, url] for (neg, u, url) in pq]
    save_json(build_path(state_dir, "pq"), pq_list)
    save_json(build_path(state_dir, "exported"), sorted(list(exported)))
    save_json(build_path(state_dir, "enqueued"), sorted(list(enqueued)))

def _load_checkpoint(state_dir: str):
    pq_data = _load_json_if_exists(build_path(state_dir, "pq"))
    exp_data = _load_json_if_exists(build_path(state_dir, "exported"))
    enq_data = _load_json_if_exists(build_path(state_dir, "enqueued"))
    if pq_data is None or exp_data is None or enq_data is None:
        return None
    pq: List[PQItem] = [(int(neg), str(u), str(url)) for neg, u, url in pq_data]
    heapq.heapify(pq)  # ensure heap property
    exported = set(map(str, exp_data))
    enqueued = set(map(str, enq_data))
    return pq, exported, enqueued

def _scan_exported_from_disk(users_dir: str) -> Set[str]:
    """
    If no checkpoint exists, treat users as 'exported' if their reviews.json exists.
    This makes export idempotent and safe to re-run a partially completed user.
    """
    done = set()
    if not os.path.isdir(users_dir):
        return done
    for name in os.listdir(users_dir):
        udir = build_path(users_dir, name)
        if not os.path.isdir(udir):
            continue
        reviews_fp = build_path(udir, "reviews") + ".json"
        if os.path.exists(reviews_fp):
            done.add(name)
    return done

# --- Enqueue helpers -----------------------------------------------------

def _item_to_username_and_url(item) -> Tuple[Optional[str], Optional[str]]:
    if isinstance(item, str):
        u = item.strip().strip("/")
        return (u or None), (f"https://letterboxd.com/{u}/" if u else None)
    if isinstance(item, dict):
        for k in ("username", "name", "user", "slug"):
            if k in item and item[k]:
                u = str(item[k]).strip().strip("/")
                if u:
                    url = item.get("profile_url") or item.get("url") or f"https://letterboxd.com/{u}/"
                    return u, url
        for blk in ("user", "profile"):
            sub = item.get(blk)
            if isinstance(sub, dict):
                for k in ("username", "name", "slug"):
                    if k in sub and sub[k]:
                        u = str(sub[k]).strip().strip("/")
                        if u:
                            url = sub.get("url") or f"https://letterboxd.com/{u}/"
                            return u, url
        return None, None
    for attr in ("username", "name", "slug"):
        if hasattr(item, attr):
            u = str(getattr(item, attr) or "").strip().strip("/")
            if u:
                url = getattr(item, "profile_url", None) or getattr(item, "url", None) or f"https://letterboxd.com/{u}/"
                return u, url
    return None, None

def _priority_for_new_user(username: str) -> int:
    if not LOOKUP_FOLLOWER_COUNT_FOR_NEW:
        return 0
    try:
        u = lb_user.User(username)
        v = extract_followers_count(u)
        return int(v) if v is not None else 0
    except Exception:
        return 0

def enqueue_new_from_iterable(
    pq: List[PQItem],
    items: Iterable,
    enqueued: Set[str],
    exported: Set[str],
    max_new: int = MAX_NEW_PER_USER,
) -> int:
    added = 0
    for it in items or []:
        if added >= max_new:
            break
        u, url = _item_to_username_and_url(it)
        if not u:
            continue
        if u in exported or u in enqueued:
            continue
        prio = _priority_for_new_user(u)
        heapq.heappush(pq, (-prio, u, url or f"https://letterboxd.com/{u}/"))
        enqueued.add(u)
        added += 1
    return added

# --- export_user (idempotent) -------------------------------------------------

def export_user(username: str, users_dir: str):
    """Export following, followers count, and reviews for one user. Returns `following` raw list."""
    sleep_polite(1, 2)
    t0 = time.time()
    print(f"\n→ {username}: starting…")
    try:
        u = lb_user.User(username)
    except Exception as e:
        print(f"  ! failed to init user '{username}': {e!r}")
        return None

    user_dir = build_path(users_dir, u.username)
    check_and_create_dirs([user_dir])

    sleep_polite(1, 2)

    # Following
    following = None
    t = time.time()
    try:
        following = lb_user.User.get_following(u)
        path = build_path(user_dir, "following")
        save_json(path, following)
        print(f"  {time.time() - t:5.2f}s - following       → {build_click_url(path)}.json")
    except Exception as e:
        print(f"  ! get_following failed: {e!r}")

    sleep_polite(1, 2)

    # Followers COUNT only
    t = time.time()
    try:
        follower_count = extract_followers_count(u)
        path = build_path(user_dir, "followers_count")
        save_json(path, {"username": u.username, "followers_count": int(follower_count or 0)})
        print(
            f"  {time.time() - t:5.2f}s - followers_count  → {build_click_url(path)}.json "
            f"(count={follower_count})"
        )
    except Exception as e:
        print(f"  ! followers_count failed: {e!r}")

    sleep_polite(1, 2)

    # Reviews
    t = time.time()
    try:
        all_reviews = lb_user.User.get_reviews(u)
        path = build_path(user_dir, "reviews")
        save_json(path, all_reviews)
        print(
            f"  {time.time() - t:5.2f}s - reviews (all)    → {build_click_url(path)}.json "
            f"({len(all_reviews) if all_reviews is not None else 0} total)"
        )
    except Exception as e:
        print(f"  ! get_reviews failed: {e!r}")

    print(f"← {username}: done in {time.time() - t0:.2f}s")
    return following

# -------- Main with resume support --------
def main() -> None:
    start = time.time()
    fresh = ("--fresh" in sys.argv)

    # Prepare export + state dirs
    root = os.getcwd()
    exports = build_path(root, "exports")
    users_dir = build_path(exports, "users")
    state_dir = build_path(exports, "_state")
    check_and_create_dirs([exports, users_dir, state_dir])
    print("Export directories ready.")

    # Try to resume
    resumed = False
    loaded = None if fresh else _load_checkpoint(state_dir)

    if loaded:
        pq, exported, enqueued = loaded
        resumed = True
        print(f"Resuming from checkpoint: {len(exported)} exported, {len(pq)} queued.")
    else:
        # New run or no checkpoint: build from seed + disk scan
        exported = _scan_exported_from_disk(users_dir)
        pq = read_seed_pq("seed.csv")
        # Filter PQ against already exported
        if exported:
            pq = [item for item in pq if item[1] not in exported]
            heapq.heapify(pq)
        enqueued = set(u for _, u, _ in pq)
        if exported:
            print(f"Found {len(exported)} previously completed users on disk (via reviews.json).")
        if not pq:
            print("No usernames queued from seed.csv. If this is unexpected, check seed.csv.")
        # Save initial checkpoint so we can resume even if interrupted early
        _save_checkpoint(state_dir, pq, exported, enqueued)

    processed = len(exported)  # for display only

    try:
        while pq and processed - len(exported) < PROCESSED_LIMIT or pq:
            total_known = len(pq) + len(exported)
            pad = len(str(max(total_known, 1)))

            username, priority, profile_url = pq_pop(pq)
            if username in exported:
                continue

            processed += 1
            print(f"[{processed:0>{pad}}/{total_known}] {username} (priority={priority})")

            following_raw = export_user(username, users_dir)
            exported.add(username)

            # Immediately enqueue newly discovered nodes
            if following_raw:
                added = enqueue_new_from_iterable(
                    pq=pq,
                    items=following_raw,
                    enqueued=enqueued,
                    exported=exported,
                    max_new=MAX_NEW_PER_USER,
                )
                if added:
                    print(f"  ↳ queued {added} new profiles from {username}'s following")

            # Save checkpoint after each user
            _save_checkpoint(state_dir, pq, exported, enqueued)

    except KeyboardInterrupt:
        print("\nInterrupted by user. Saving checkpoint and exiting…")
        _save_checkpoint(state_dir, pq, exported, enqueued)
        sys.exit(130)

    # Finished normally
    _save_checkpoint(state_dir, pq, exported, enqueued)
    print("\nAll done!")
    print(f"  Total processed (completed on disk): {len(exported)}")
    print(f"  Still queued:                      : {len(pq)}")
    print(f"  Total time:                        : {time.time() - start:.2f}s")
    print(f"  Outputs at:                        : {build_click_url(users_dir)}")
    if resumed:
        print("  (This run resumed from a previous checkpoint.)")

if __name__ == "__main__":
    main()