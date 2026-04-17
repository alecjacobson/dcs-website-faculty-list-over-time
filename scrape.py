#!/usr/bin/env python3
"""
scrape.py - Download Wayback Machine snapshots of UofT CS faculty pages.

Queries the CDX API for monthly snapshots of each known URL pattern and
downloads the HTML, caching it locally in data/raw/.

Usage:
    python scrape.py              # download all missing snapshots
    python scrape.py --dry-run    # show what would be downloaded
    python scrape.py --delay 2    # slower, more polite rate limit
"""

import argparse
import time
from pathlib import Path

import requests
from tqdm import tqdm

RAW_DIR = Path("data/raw")
CDX_API = "https://web.archive.org/cdx/search/cdx"
WAYBACK_BASE = "http://web.archive.org/web"

# Known URL patterns and their active date ranges.
# The faculty listing URL changed several times; we cover each era separately
# so the CDX API can find archived copies of each.
URL_PATTERNS = [
    {
        "url": "web.cs.toronto.edu/dcs/index.php?section=95",
        "from": "200711",
        "to":   "200902",
        "slug": "index_section95",
    },
    {
        "url": "web.cs.toronto.edu/people/faculty.htm",
        "from": "200901",
        "to":   "202012",
        "slug": "faculty_htm",
    },
    {
        "url": "web.cs.toronto.edu/contact-us/faculty-directory",
        "from": "202010",
        "to":   "202112",
        "slug": "contact_faculty_dir",
    },
    {
        "url": "web.cs.toronto.edu/people/faculty-directory",
        "from": "202111",
        "to":   "202612",
        "slug": "people_faculty_dir",
    },
]

LIVE_URL = "https://web.cs.toronto.edu/people/faculty-directory"


def get_cdx_snapshots(url: str, from_date: str, to_date: str, retries: int = 3) -> list[dict]:
    """Return one representative snapshot per calendar month via the CDX API."""
    params = {
        "url": url,
        "output": "json",
        "fl": "timestamp,original,statuscode",
        "collapse": "timestamp:6",   # one result per YYYYMM
        "from": from_date,
        "to": to_date,
        "filter": "statuscode:200",
        "limit": 300,
    }
    for attempt in range(retries):
        try:
            resp = requests.get(CDX_API, params=params, timeout=60)
            resp.raise_for_status()
            if not resp.text.strip():
                raise ValueError("empty response body")
            rows = resp.json()
            if not rows or len(rows) <= 1:
                return []
            headers = rows[0]
            return [dict(zip(headers, row)) for row in rows[1:]]
        except Exception as e:
            if attempt < retries - 1:
                wait = 5 * (attempt + 1)
                print(f"  CDX retry {attempt+1}/{retries} for {url}: {e} (waiting {wait}s)")
                time.sleep(wait)
            else:
                print(f"  CDX API error for {url}: {e}")
    return []


def download_wayback(timestamp: str, original_url: str, dest: Path) -> bool:
    """Download a single Wayback Machine snapshot."""
    url = f"{WAYBACK_BASE}/{timestamp}/{original_url}"
    try:
        resp = requests.get(url, timeout=45, allow_redirects=True)
        resp.raise_for_status()
        dest.write_bytes(resp.content)
        return True
    except Exception as e:
        print(f"  Failed {url}: {e}")
        return False


def download_live(dest: Path) -> bool:
    """Download the current live faculty page."""
    try:
        resp = requests.get(LIVE_URL, timeout=30)
        resp.raise_for_status()
        dest.write_bytes(resp.content)
        return True
    except Exception as e:
        print(f"  Failed to fetch live page: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Download Wayback Machine faculty page snapshots")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be downloaded without fetching")
    parser.add_argument("--delay", type=float, default=1.5, help="Seconds between requests (default: 1.5)")
    parser.add_argument("--skip-live", action="store_true", help="Skip fetching the current live page")
    args = parser.parse_args()

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    # Collect all Wayback snapshots across URL patterns
    all_snapshots: list[tuple[dict, Path]] = []

    print("Querying Wayback Machine CDX API...")
    for pattern in URL_PATTERNS:
        print(f"  {pattern['slug']}  ({pattern['from']}–{pattern['to']})")
        snapshots = get_cdx_snapshots(pattern["url"], pattern["from"], pattern["to"])
        for s in snapshots:
            dest = RAW_DIR / f"{s['timestamp']}_{pattern['slug']}.html"
            all_snapshots.append((s, dest, pattern["slug"]))
        time.sleep(0.5)

    already_cached = sum(1 for _, dest, _ in all_snapshots if dest.exists())
    to_download = [(s, dest, slug) for s, dest, slug in all_snapshots if not dest.exists()]

    print(f"\nFound {len(all_snapshots)} Wayback snapshots total")
    print(f"  {already_cached} already cached, {len(to_download)} to download")

    # Check live page
    from datetime import datetime
    live_dest = RAW_DIR / f"{datetime.now().strftime('%Y%m')}_live.html"
    fetch_live = not args.skip_live and not live_dest.exists()

    if args.dry_run:
        for _, dest, slug in to_download:
            print(f"  Would download: {dest.name}")
        if fetch_live:
            print(f"  Would fetch live page → {live_dest.name}")
        return

    ok = 0
    for s, dest, _ in tqdm(to_download, desc="Downloading snapshots"):
        if download_wayback(s["timestamp"], s["original"], dest):
            ok += 1
        time.sleep(args.delay)

    if fetch_live:
        print(f"Fetching current live page → {live_dest.name}")
        download_live(live_dest)

    print(f"\nDone: {ok}/{len(to_download)} Wayback snapshots downloaded")


if __name__ == "__main__":
    main()
