#!/usr/bin/env python3
"""
parse.py - Parse downloaded HTML snapshots to extract faculty data.

Handles three distinct page formats across the archive's date range:
  - old_php    : 2007-2008  index.php?section=95 (likely table-based)
  - faculty_htm: 2009-2020  /people/faculty.htm
  - modern     : 2021+      /people/faculty-directory  (h2-sectioned, confirmed structure)

Outputs one JSON file per snapshot into data/parsed/.

Usage:
    python parse.py                        # parse all snapshots
    python parse.py --inspect data/raw/X   # dump raw text of one file (for debugging)
    python parse.py --reparse              # overwrite existing parsed files
"""

import argparse
import json
import re
from pathlib import Path

from bs4 import BeautifulSoup, NavigableString, Tag
from tqdm import tqdm

RAW_DIR = Path("data/raw")
PARSED_DIR = Path("data/parsed")

# ---------------------------------------------------------------------------
# Stream / section classification
# ---------------------------------------------------------------------------

# Confirmed section header names from the live page (h2 tags).
# We match lowercased substrings so minor wording changes are handled.
RESEARCH_KEYWORDS  = {"research stream", "regular faculty", "tenure stream", "research faculty"}
TEACHING_KEYWORDS  = {"teaching stream", "lecturers", "teaching faculty", "educational developer"}
SKIP_KEYWORDS      = {
    "emerit",           # catches "Emeriti", "Emeritus", "Emerita"
    "cross-appoint",
    "cross appoint",
    "status-only",
    "status only",
    "adjunct",
    "clta",
    "visiting",
    "affiliated",
    "industrial",
}

SKIP_TITLE_KEYWORDS = {"emerit", "adjunct", "clta", "cross-appoint", "status-only"}


def classify_section(header_text: str) -> str:
    """Return 'research', 'teaching', or 'skip'."""
    t = header_text.lower()
    for kw in SKIP_KEYWORDS:
        if kw in t:
            return "skip"
    for kw in TEACHING_KEYWORDS:
        if kw in t:
            return "teaching"
    for kw in RESEARCH_KEYWORDS:
        if kw in t:
            return "research"
    return "unknown"


def skip_by_title(title: str) -> bool:
    t = title.lower()
    return any(kw in t for kw in SKIP_TITLE_KEYWORDS)


# ---------------------------------------------------------------------------
# Name utilities
# ---------------------------------------------------------------------------

NAME_STRIP_RE = re.compile(r"^(Dr\.?|Prof\.?|Professor)\s+", re.IGNORECASE)
PAREN_RE      = re.compile(r"\s*\([^)]*\)")   # strip "(Jan.08)", "(cross-appt)", etc.
WHITESPACE_RE = re.compile(r"\s+")

# Words that should never appear in a valid faculty name link
NON_NAME_WORDS = {
    "click", "here", "email", "phone", "office", "room", "lab",
    "research", "department", "university", "faculty", "more",
    "information", "contact", "view", "profile", "home", "page",
    "directory", "back", "top", "menu", "search", "login",
}


def clean_name(raw: str) -> str:
    raw = WHITESPACE_RE.sub(" ", raw).strip().rstrip(".,;:")
    raw = NAME_STRIP_RE.sub("", raw)
    raw = PAREN_RE.sub("", raw)
    return raw.strip()


def is_plausible_name(name: str) -> bool:
    if not name or len(name) < 4 or len(name) > 60:
        return False
    parts = name.split()
    if len(parts) < 2:
        return False
    # Reject if any word is a known non-name word
    if any(p.lower() in NON_NAME_WORDS for p in parts):
        return False
    # At least first and last part should start with a letter
    if not (parts[0][0].isalpha() and parts[-1][0].isalpha()):
        return False
    # Reject strings that look like sentences (too many lowercase words in a row)
    lowercase_words = sum(1 for p in parts if p[0].islower() and len(p) > 3)
    if lowercase_words > 1:
        return False
    return True


# ---------------------------------------------------------------------------
# Modern format parser (2021+)
# Confirmed structure:
#   <h2>Research Stream Faculty</h2>   ← or Teaching Stream / Emeriti / etc.
#   ... faculty entries ...
#   <h2>Next Section</h2>
#
# Each faculty entry: name as <a> link, rank as following text node or <p>.
# Teaching stream section uses a table layout.
# ---------------------------------------------------------------------------

def _get_title_near_link(a_tag: Tag) -> str:
    """Extract the title/rank text that follows a faculty name link."""
    # Check immediate next sibling text nodes
    for sib in a_tag.next_siblings:
        if isinstance(sib, NavigableString):
            t = sib.strip().strip("-–—").strip()
            if t:
                return t
        elif isinstance(sib, Tag):
            if sib.name in ("br", "span", "p", "div", "td"):
                t = sib.get_text(strip=True)
                if t and len(t) < 100:
                    return t
            break
    # Fallback: look inside the parent container
    parent = a_tag.parent
    if parent:
        full_text = parent.get_text(separator="|", strip=True)
        link_text = a_tag.get_text(strip=True)
        after = full_text.split(link_text, 1)[-1].lstrip("|").split("|")[0].strip()
        if after and len(after) < 80:
            return after
    return ""


def parse_modern_directory(soup: BeautifulSoup) -> list[dict]:
    """Parse 2021+ /people/faculty-directory format."""
    faculty: list[dict] = []
    seen_names: set[str] = set()

    h2_tags = soup.find_all("h2")
    if not h2_tags:
        return []

    for h2 in h2_tags:
        stream = classify_section(h2.get_text(strip=True))
        if stream == "skip":
            continue

        # Collect all tags between this h2 and the next h2
        section_tags = []
        for sibling in h2.find_next_siblings():
            if sibling.name == "h2":
                break
            section_tags.append(sibling)

        # Check for table layout (teaching stream uses a table; research stream may too)
        for sibling in section_tags:
            for row in sibling.find_all("tr") if isinstance(sibling, Tag) else []:
                cells = row.find_all(["td", "th"])
                if not cells:
                    continue
                a = cells[0].find("a")
                if not a:
                    continue
                name = clean_name(a.get_text(strip=True))
                if not is_plausible_name(name) or name in seen_names:
                    continue
                # Two layouts seen in the wild:
                # Layout A (Tenure Stream): <td><a>Name</a><br/>Professor</td>  — title after <br/>
                # Layout B (Teaching Stream): <td><a>Name</a></td><td>Professor</td> — title in cells[1]
                br = cells[0].find("br")
                if br:
                    title = "".join(
                        s.strip() for s in br.next_siblings if isinstance(s, NavigableString)
                    ).strip()
                    if not title:
                        # <br/> present but nothing after it; check cells[1]
                        title = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                else:
                    # No <br/> in name cell — try cells[1] if it looks like a rank
                    if len(cells) > 1:
                        candidate = cells[1].get_text(strip=True)
                        if len(candidate) < 80 and "@" not in candidate and not candidate[:3].isdigit():
                            title = candidate
                        else:
                            title = ""
                    else:
                        title = ""
                if skip_by_title(title):
                    continue
                s = stream if stream != "unknown" else (
                    "teaching" if "teach" in title.lower() or "lectur" in title.lower() else "research"
                )
                faculty.append({"name": name, "stream": s, "title": title})
                seen_names.add(name)

        # General link scan for non-table sections
        section_html = "".join(str(t) for t in section_tags)
        section_soup = BeautifulSoup(section_html, "lxml")
        for a in section_soup.find_all("a"):
            name = clean_name(a.get_text(strip=True))
            if not is_plausible_name(name) or name in seen_names:
                continue
            title = _get_title_near_link(a)
            if skip_by_title(title):
                continue
            s = stream if stream != "unknown" else (
                "teaching" if "teach" in title.lower() or "lectur" in title.lower() else "research"
            )
            faculty.append({"name": name, "stream": s, "title": title})
            seen_names.add(name)

    return faculty


# ---------------------------------------------------------------------------
# faculty.htm parser (2009–2020)
# Structure is less predictable; use multiple strategies in order.
# ---------------------------------------------------------------------------

TITLE_IN_CELL_RE = re.compile(
    r"\b(?:Full\s+)?(?:Associate\s+|Assistant\s+)?(?:University\s+)?(?:Professor|Lecturer|Instructor)\b.*$",
    re.IGNORECASE,
)


def _parse_last_first(raw: str) -> tuple[str, str]:
    """
    Convert 'Last, First [optional title suffix]' → (clean_name, title).

    Handles:
      'Abdelrahman, Tarek'                              → ('Tarek Abdelrahman', '')
      'Baumgartner, Gary Associate Professor, Teaching' → ('Gary Baumgartner', 'Associate Professor, Teaching')
    """
    raw = WHITESPACE_RE.sub(" ", raw).strip()
    if "," not in raw:
        # Might be "First Last" already — strip title if present
        m = TITLE_IN_CELL_RE.search(raw)
        name = raw[: m.start()].strip() if m else raw
        title = m.group(0).strip() if m else ""
        return clean_name(name), title

    last, rest = raw.split(",", 1)
    last = last.strip()
    rest = rest.strip()

    m = TITLE_IN_CELL_RE.search(rest)
    if m:
        first = rest[: m.start()].strip()
        title = m.group(0).strip()
    else:
        first = rest
        title = ""

    full_name = f"{first} {last}".strip() if first else last
    return clean_name(full_name), title


def _table_col_headers(table) -> list[str]:
    first_row = table.find("tr")
    return [c.get_text(strip=True) for c in first_row.find_all(["td", "th"])] if first_row else []


def parse_faculty_htm(soup: BeautifulSoup) -> list[dict]:
    """Parse 2009–2020 /people/faculty.htm format.

    Two sub-formats detected at runtime:

    Early (2009–~2016): Three plain tables, no h2 stream labels.
      Table 1 — NAME / PHONE / EMAIL / OFFICE      → research stream
      Table 2 — Name / Email / Department          → cross-appointed  → skip
      Table 3 — NAME / AFFILIATION                 → status-only      → skip

    Later (~2017–2019): h2 section labels precede each table.
      h2 'Tenure Stream Faculty'    → research
      h2 'Teaching Stream Faculty'  → teaching
      h2 'Limited Term Appointments'→ skip
    """
    faculty: list[dict] = []
    seen: set[str] = set()

    # Detect which sub-format: does any h2 carry a stream classification?
    h2_tags = soup.find_all("h2")
    has_stream_h2 = any(
        classify_section(h.get_text(strip=True)) in ("research", "teaching", "skip")
        for h in h2_tags
    )

    if has_stream_h2:
        # ── Later format: h2-sectioned tables ──────────────────────────────
        for h2 in h2_tags:
            stream = classify_section(h2.get_text(strip=True))
            if stream in ("skip", "unknown"):
                continue
            next_h2 = h2.find_next_sibling("h2") or h2.find_next("h2")
            table = h2.find_next("table")
            if not table:
                continue
            # Skip if the table appears after the next section header
            if next_h2 and next_h2.find_previous("table") is not table.find_previous("table"):
                pass  # accept; BeautifulSoup traversal already respects DOM order

            for row in table.find_all("tr")[1:]:  # skip header row
                cells = row.find_all(["td", "th"])
                if not cells:
                    continue
                raw = cells[0].get_text(" ", strip=True)
                name, title = _parse_last_first(raw)
                if not is_plausible_name(name) or name in seen:
                    continue
                if skip_by_title(title):
                    continue
                s = stream
                if "teaching" in title.lower() or "lectur" in title.lower():
                    s = "teaching"
                faculty.append({"name": name, "stream": s, "title": title})
                seen.add(name)

    else:
        # ── Early format: classify tables by column headers ─────────────────
        SKIP_COL_WORDS = {"affiliation", "department"}
        faculty_table_found = False

        for table in soup.find_all("table"):
            cols = _table_col_headers(table)
            if not cols:
                continue
            cols_lower = [c.lower() for c in cols]

            # Skip tables whose columns indicate cross-appointed / status-only
            if any(any(w in col for w in SKIP_COL_WORDS) for col in cols_lower):
                continue
            # Only process tables whose first column is "name" (case-insensitive)
            if "name" not in cols_lower[0]:
                continue

            for row in table.find_all("tr")[1:]:
                cells = row.find_all(["td", "th"])
                if not cells:
                    continue
                raw = cells[0].get_text(" ", strip=True)
                name, title = _parse_last_first(raw)
                if not is_plausible_name(name) or name in seen:
                    continue
                faculty.append({"name": name, "stream": "research", "title": title})
                seen.add(name)
                faculty_table_found = True

    return faculty


# ---------------------------------------------------------------------------
# Old PHP format parser (2007–2008)
# Likely table-based; fall back to faculty_htm parser if no table found.
# ---------------------------------------------------------------------------

def parse_old_php(soup: BeautifulSoup) -> list[dict]:
    """Parse 2007–2008 index.php?section=95 format.

    The page layout is identical to the early faculty.htm format (plain tables,
    'Last, First' names, no stream distinction), so we delegate there.
    """
    return parse_faculty_htm(soup)


# ---------------------------------------------------------------------------
# Format detection and dispatch
# ---------------------------------------------------------------------------

def detect_format(html_path: Path, soup: BeautifulSoup) -> str:
    name = html_path.stem
    if "index_section95" in name:
        return "old_php"
    # Modern format: /people/faculty-directory or /contact-us/faculty-directory
    # These pages use profile links; faculty_htm pages use plain-text tables.
    if "people_faculty_dir" in name or "contact_faculty_dir" in name or "live" in name:
        return "modern"
    # All other files are faculty.htm-era; parse_faculty_htm handles both its sub-formats.
    return "faculty_htm"


def parse_html(html_path: Path) -> list[dict]:
    try:
        html = html_path.read_bytes()
        soup = BeautifulSoup(html, "lxml")
    except Exception as e:
        print(f"  Read error {html_path.name}: {e}")
        return []

    fmt = detect_format(html_path, soup)
    if fmt == "old_php":
        raw = parse_old_php(soup)
    elif fmt == "modern":
        raw = parse_modern_directory(soup)
    else:
        raw = parse_faculty_htm(soup)

    # De-duplicate within this snapshot
    seen: dict[str, dict] = {}
    for f in raw:
        n = f["name"]
        if n and n not in seen:
            seen[n] = f
    return list(seen.values())


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Parse downloaded HTML snapshots into structured JSON")
    parser.add_argument("--inspect", metavar="FILE",
                        help="Dump plain text of an HTML file (for debugging parsers)")
    parser.add_argument("--reparse", action="store_true",
                        help="Re-parse all files, overwriting existing JSON")
    args = parser.parse_args()

    if args.inspect:
        p = Path(args.inspect)
        soup = BeautifulSoup(p.read_bytes(), "lxml")
        print(soup.get_text(separator="\n", strip=True)[:8000])
        return

    PARSED_DIR.mkdir(parents=True, exist_ok=True)

    html_files = sorted(RAW_DIR.glob("*.html"))
    if not html_files:
        print("No HTML files in data/raw/. Run scrape.py first.")
        return

    results = []
    for html_path in tqdm(html_files, desc="Parsing"):
        out_path = PARSED_DIR / f"{html_path.stem}.json"
        if out_path.exists() and not args.reparse:
            continue

        timestamp = html_path.stem[:6]  # YYYYMM
        faculty = parse_html(html_path)
        record = {
            "timestamp": timestamp,
            "source_file": html_path.name,
            "faculty_count": len(faculty),
            "faculty": faculty,
        }
        out_path.write_text(json.dumps(record, indent=2))
        results.append((timestamp, html_path.name, len(faculty)))

    if not results:
        print("All files already parsed. Use --reparse to force.")
        return

    print(f"\nParsed {len(results)} snapshots:")
    for ts, fname, count in results:
        flag = "  *** LOW ***" if count < 10 else ""
        print(f"  {ts}: {count:3d} faculty  ({fname}){flag}")

    low = [(ts, f, c) for ts, f, c in results if c < 10]
    if low:
        print(f"\nWARNING: {len(low)} snapshot(s) with < 10 faculty — parsing likely failed.")
        print("Inspect with:  python parse.py --inspect data/raw/<filename>")


if __name__ == "__main__":
    main()
