#!/usr/bin/env python3
"""
match.py - Fuzzy-match faculty names across all parsed snapshots.

Because the same person may appear as "John Smith" in one snapshot and
"J. Smith" or "John A. Smith" in another, we use rapidfuzz token-sort ratio
to decide whether two name strings refer to the same individual.

Outputs data/faculty_timeline.csv with one row per canonical faculty member.

Usage:
    python match.py                   # run with default threshold (85)
    python match.py --threshold 90    # stricter matching
    python match.py --verbose         # print each match/new decision
"""

import argparse
import csv
import json
import re
from dataclasses import dataclass, field
from pathlib import Path

from rapidfuzz import fuzz, process

PARSED_DIR = Path("data/parsed")
TIMELINE_CSV = Path("data/faculty_timeline.csv")

DEFAULT_THRESHOLD = 85  # 0–100; raise to reduce false merges


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class FacultyRecord:
    canonical_name: str
    stream: str                       # "research" | "teaching" | "unknown"
    titles: list[str] = field(default_factory=list)
    appearances: list[str] = field(default_factory=list)   # sorted YYYYMM strings


# ---------------------------------------------------------------------------
# Name normalisation for fuzzy comparison
# ---------------------------------------------------------------------------

HYPHEN_RE = re.compile(r"-")
PUNCT_RE  = re.compile(r"['\.\,]")
SPACE_RE  = re.compile(r"\s+")
TITLE_RE  = re.compile(r"^(dr|prof|professor|mr|ms|mrs)\s+", re.IGNORECASE)


def normalise(name: str) -> str:
    """Lower-case, replace hyphens with spaces, remove punctuation and honorifics."""
    name = TITLE_RE.sub("", name.lower())
    name = HYPHEN_RE.sub(" ", name)   # "Demke-Brown" → "Demke Brown" before punct strip
    name = PUNCT_RE.sub("", name)
    name = SPACE_RE.sub(" ", name).strip()
    return name


# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------

def best_match(name: str, records: dict[str, FacultyRecord], threshold: int) -> str | None:
    """
    Return the canonical_name of the best matching existing record, or None
    if no record scores above threshold.
    Uses token_sort_ratio so word order differences don't penalise the score.
    """
    if not records:
        return None
    norm_name = normalise(name)
    canonical_names = list(records.keys())
    norm_canonicals = [normalise(c) for c in canonical_names]
    result = process.extractOne(norm_name, norm_canonicals, scorer=fuzz.token_sort_ratio)
    if result and result[1] >= threshold:
        return canonical_names[result[2]]
    return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Fuzzy-match faculty across parsed snapshots")
    parser.add_argument("--threshold", type=int, default=DEFAULT_THRESHOLD,
                        help=f"Match score threshold 0-100 (default: {DEFAULT_THRESHOLD})")
    parser.add_argument("--verbose", action="store_true",
                        help="Print each match/new-entry decision")
    args = parser.parse_args()

    parsed_files = sorted(PARSED_DIR.glob("*.json"))
    if not parsed_files:
        print("No parsed JSON files found. Run parse.py first.")
        return

    print(f"Loading {len(parsed_files)} snapshots (threshold={args.threshold})...")

    records: dict[str, FacultyRecord] = {}    # canonical_name → FacultyRecord
    all_timestamps: list[str] = []

    for jf in parsed_files:
        data = json.loads(jf.read_text())
        timestamp = data["timestamp"]
        all_timestamps.append(timestamp)

        for f in data["faculty"]:
            name = f.get("name", "").strip()
            stream = f.get("stream", "unknown")
            title = f.get("title", "").strip()

            if not name:
                continue

            match = best_match(name, records, args.threshold)

            if match:
                rec = records[match]
                if timestamp not in rec.appearances:
                    rec.appearances.append(timestamp)
                if title and title not in rec.titles:
                    rec.titles.append(title)
                # Upgrade stream when we learn something more specific.
                # Pre-2018 pages had one undifferentiated table (all tagged "research"),
                # so if a later snapshot definitively says "teaching" we trust that.
                # Priority: teaching > research > unknown
                if stream == "teaching":
                    rec.stream = "teaching"
                elif stream == "research" and rec.stream == "unknown":
                    rec.stream = "research"
                if args.verbose and normalise(name) != normalise(match):
                    score = fuzz.token_sort_ratio(normalise(name), normalise(match))
                    print(f"  {timestamp}  MERGE  '{name}' → '{match}'  (score={score})")
            else:
                records[name] = FacultyRecord(
                    canonical_name=name,
                    stream=stream,
                    titles=[title] if title else [],
                    appearances=[timestamp],
                )
                if args.verbose:
                    print(f"  {timestamp}  NEW    '{name}'  ({stream})")

    latest = max(all_timestamps) if all_timestamps else ""

    # Apply profile-scraped stream overrides (teaching only — we never downgrade to research)
    profile_path = Path("data/profile_streams.json")
    if profile_path.exists():
        overrides = json.loads(profile_path.read_text())
        applied = 0
        for canon_name, info in overrides.items():
            match = best_match(canon_name, records, args.threshold)
            if match and info.get("stream") == "teaching":
                if records[match].stream != "teaching":
                    records[match].stream = "teaching"
                    applied += 1
                    if args.verbose:
                        print(f"  PROFILE OVERRIDE  '{match}' → teaching  ({info.get('source_url','')})")
        if applied:
            print(f"  Applied {applied} teaching stream overrides from {profile_path}")

    print(f"\nFound {len(records)} unique faculty members across {len(parsed_files)} snapshots")

    TIMELINE_CSV.parent.mkdir(parents=True, exist_ok=True)
    with TIMELINE_CSV.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "canonical_name", "stream", "first_seen", "last_seen",
            "months_active", "currently_listed", "titles",
        ])
        writer.writeheader()
        for rec in sorted(records.values(), key=lambda r: r.appearances[0] if r.appearances else ""):
            appr = sorted(set(rec.appearances))
            writer.writerow({
                "canonical_name":    rec.canonical_name,
                "stream":            rec.stream,
                "first_seen":        appr[0] if appr else "",
                "last_seen":         appr[-1] if appr else "",
                "months_active":     len(appr),
                "currently_listed":  appr[-1] == latest if appr else False,
                "titles":            " | ".join(rec.titles),
            })

    print(f"Written to {TIMELINE_CSV}")

    # Summary
    research = [r for r in records.values() if r.stream == "research"]
    teaching = [r for r in records.values() if r.stream == "teaching"]
    current_r = [r for r in research if r.appearances and r.appearances[-1] == latest]
    current_t = [r for r in teaching if r.appearances and r.appearances[-1] == latest]

    print(f"\nAll-time summary:")
    print(f"  Research stream : {len(research):3d} total  ({len(current_r)} currently listed)")
    print(f"  Teaching stream : {len(teaching):3d} total  ({len(current_t)} currently listed)")
    print(f"  Unknown stream  : {sum(1 for r in records.values() if r.stream == 'unknown'):3d} total")


if __name__ == "__main__":
    main()
