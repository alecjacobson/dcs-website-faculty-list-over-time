#!/usr/bin/env python3
"""
scrape_profiles.py - Fetch personal faculty pages to improve stream classification.

For each faculty member whose stream is ambiguous (appeared only in the pre-h2
faculty_htm era where titles were often blank), this script:
  1. Extracts personal-page URLs from the raw HTML directory snapshots.
  2. Fetches those pages via the Wayback Machine.
  3. Scans the page text for "Teaching Stream" / "Lecturer" indicators.
  4. Writes data/profile_streams.json  {canonical_name: {stream, source_url}}.

match.py reads this file and applies teaching-stream upgrades.

Usage:
    python3 scrape_profiles.py              # only fetch pages not yet cached
    python3 scrape_profiles.py --refetch    # re-fetch all pages
    python3 scrape_profiles.py --delay 2    # polite rate limit (default 1s)
    python3 scrape_profiles.py --dry-run    # show what would be fetched
"""

import argparse
import json
import re
import time
from collections import defaultdict
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from rapidfuzz import fuzz, process

RAW_DIR        = Path("data/raw")
PARSED_DIR     = Path("data/parsed")
TIMELINE_CSV   = Path("data/faculty_timeline.csv")
PROFILE_CACHE  = Path("data/profile_cache")   # raw HTML of personal pages
PROFILE_OUT    = Path("data/profile_streams.json")

TEACHING_KEYWORDS = [
    "teaching stream",
    "senior lecturer",
    "lecturer emeritus",
    "i am a lecturer",
    "teaching professor",
    "professor, teaching",
    "associate professor, teaching",
    "assistant professor, teaching",
]
RESEARCH_KEYWORDS = [
    "canada research chair",
    "turing award",
    "fellow of the royal society",
]

SESSION = requests.Session()
SESSION.headers["User-Agent"] = (
    "faculty-change-over-time research project "
    "(https://github.com/alecjacobson/dcs-website-faculty-list-over-time)"
)

# ---------------------------------------------------------------------------
# Name normalisation (mirror of match.py)
# ---------------------------------------------------------------------------

_HYPHEN_RE = re.compile(r"-")
_PUNCT_RE  = re.compile(r"['\.\,]")
_SPACE_RE  = re.compile(r"\s+")
_TITLE_RE  = re.compile(r"^(dr|prof|professor|mr|ms|mrs)\s+", re.IGNORECASE)


def normalise(name: str) -> str:
    name = _TITLE_RE.sub("", name.lower())
    name = _HYPHEN_RE.sub(" ", name)
    name = _PUNCT_RE.sub("", name)
    return _SPACE_RE.sub(" ", name).strip()


# ---------------------------------------------------------------------------
# Step 1 – extract (raw_name → wayback_url) from raw HTML
# ---------------------------------------------------------------------------

def extract_profile_links(raw_html_path: Path) -> dict[str, str]:
    """Return {raw_name: wayback_personal_url} from a faculty directory HTML."""
    soup = BeautifulSoup(raw_html_path.read_bytes(), "lxml")
    links: dict[str, str] = {}

    for table in soup.find_all("table"):
        for row in table.find_all("tr")[1:]:
            cells = row.find_all(["td", "th"])
            if not cells:
                continue
            a = cells[0].find("a")
            if not a:
                continue
            href = a.get("href", "")
            name_raw = a.get_text(" ", strip=True)
            # Keep only links that go to a personal page (not nav/dept pages)
            if not href or "faculty-directory" in href or "faculty.htm" in href:
                continue
            # Must look like a name (2+ capitalised words)
            parts = name_raw.split()
            if len(parts) < 2 or not parts[0][0].isupper():
                continue
            links[name_raw] = href

    return links


# ---------------------------------------------------------------------------
# Step 2 – fetch a page through Wayback (already-wrapped URL)
# ---------------------------------------------------------------------------

def fetch_page(url: str, cache_path: Path, refetch: bool, delay: float) -> str | None:
    if cache_path.exists() and not refetch:
        return cache_path.read_text(errors="replace")
    try:
        resp = SESSION.get(url, timeout=15, allow_redirects=True)
        if resp.status_code == 200:
            cache_path.write_bytes(resp.content)
            time.sleep(delay)
            return resp.text
        print(f"    HTTP {resp.status_code}  {url[:80]}")
    except Exception as e:
        print(f"    Error fetching {url[:80]}: {e}")
    time.sleep(delay)
    return None


# ---------------------------------------------------------------------------
# Step 3 – detect stream from page text
# ---------------------------------------------------------------------------

def detect_stream(html: str) -> str | None:
    """Return 'teaching', 'research', or None (no signal)."""
    text = BeautifulSoup(html, "lxml").get_text(separator=" ").lower()
    # Check teaching first — a research prof won't have "teaching stream" on their page
    for kw in TEACHING_KEYWORDS:
        if kw in text:
            return "teaching"
    for kw in RESEARCH_KEYWORDS:
        if kw in text:
            return "research"
    return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Scrape personal pages for stream classification")
    parser.add_argument("--refetch",  action="store_true", help="Re-fetch already-cached pages")
    parser.add_argument("--delay",    type=float, default=1.0, help="Seconds between requests (default 1)")
    parser.add_argument("--dry-run",  action="store_true", help="Show what would be fetched, don't fetch")
    args = parser.parse_args()

    PROFILE_CACHE.mkdir(parents=True, exist_ok=True)

    # Load timeline to find people whose stream needs verification
    import csv
    timeline: dict[str, str] = {}   # canonical_name → current stream
    if TIMELINE_CSV.exists():
        with TIMELINE_CSV.open() as f:
            for row in csv.DictReader(f):
                timeline[row["canonical_name"]] = row["stream"]

    # Build normalised canonical name lookup
    canon_norms = {normalise(n): n for n in timeline}

    def resolve_canonical(raw_name: str) -> str | None:
        norm = normalise(raw_name)
        if norm in canon_norms:
            return canon_norms[norm]
        result = process.extractOne(norm, list(canon_norms.keys()), scorer=fuzz.token_sort_ratio)
        if result and result[1] >= 85:
            return canon_norms[result[0]]
        return None

    # Collect profile URLs: canonical_name → {url, timestamp} (keep most recent)
    profile_candidates: dict[str, dict] = defaultdict(dict)

    for raw_file in sorted(RAW_DIR.glob("*faculty_htm*.html")):
        links = extract_profile_links(raw_file)
        ts = raw_file.stem[:14]   # full timestamp
        for raw_name, url in links.items():
            canon = resolve_canonical(raw_name)
            if canon is None:
                continue
            # Only bother with people whose stream isn't confirmed by modern directory
            existing = profile_candidates[canon]
            if not existing or ts > existing.get("ts", ""):
                profile_candidates[canon] = {"url": url, "ts": ts, "raw_name": raw_name}

    # Load last_seen dates to skip people confirmed by post-2016 directory
    last_seen: dict[str, int] = {}
    if TIMELINE_CSV.exists():
        with TIMELINE_CSV.open() as f:
            for row in csv.DictReader(f):
                try:
                    last_seen[row["canonical_name"]] = int(str(row["last_seen"])[:6])
                except ValueError:
                    pass

    # h2 stream sections appeared Nov 2018; post-2016 directory is reliable enough
    # Only fetch personal pages for people who never appeared in a clear-stream snapshot
    to_fetch = {
        canon: info
        for canon, info in profile_candidates.items()
        if timeline.get(canon) != "teaching"       # skip already-confirmed teaching
        and last_seen.get(canon, 0) < 201700        # skip post-2016 (directory was clear)
    }

    print(f"Found {len(profile_candidates)} faculty with personal page links")
    print(f"Will fetch {len(to_fetch)} pages (skipping already-confirmed teaching stream)")

    if args.dry_run:
        for canon, info in sorted(to_fetch.items()):
            print(f"  {canon:40s}  {info['url'][:80]}")
        return

    # Load existing results so we don't lose manual overrides
    existing_results: dict = {}
    if PROFILE_OUT.exists():
        existing_results = json.loads(PROFILE_OUT.read_text())

    results: dict = dict(existing_results)

    fetched = skipped = found_teaching = found_research = no_signal = 0

    for canon, info in sorted(to_fetch.items()):
        url = info["url"]
        safe = re.sub(r"[^\w]", "_", canon)[:60]
        cache_path = PROFILE_CACHE / f"{safe}.html"

        if cache_path.exists() and not args.refetch:
            skipped += 1
            html = cache_path.read_text(errors="replace")
        else:
            print(f"  Fetching  {canon}")
            html = fetch_page(url, cache_path, args.refetch, args.delay)
            if html is None:
                continue
            fetched += 1

        stream = detect_stream(html)
        if stream == "teaching":
            results[canon] = {"stream": "teaching", "source_url": url}
            found_teaching += 1
            print(f"  TEACHING  {canon}")
        elif stream == "research":
            results[canon] = {"stream": "research", "source_url": url}
            found_research += 1
        else:
            no_signal += 1

    PROFILE_OUT.write_text(json.dumps(results, indent=2, sort_keys=True))

    print(f"\nDone: {fetched} fetched, {skipped} from cache")
    print(f"  Teaching confirmed : {found_teaching}")
    print(f"  Research confirmed : {found_research}")
    print(f"  No signal          : {no_signal}")
    print(f"Written {PROFILE_OUT}  ({len(results)} entries)")


if __name__ == "__main__":
    main()
