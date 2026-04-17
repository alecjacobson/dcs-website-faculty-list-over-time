#!/usr/bin/env python3
"""
pipeline.py - Run the full pipeline: scrape → parse → match → visualize.

Usage:
    python pipeline.py              # run all stages
    python pipeline.py --from parse # start from a specific stage
    python pipeline.py --only match # run only one stage

Stages: scrape, parse, match, visualize
"""

import argparse
import subprocess
import sys

STAGES = [
    ("scrape",    [sys.executable, "scrape.py"]),
    ("parse",     [sys.executable, "parse.py"]),
    ("match",     [sys.executable, "match.py"]),
    ("visualize", [sys.executable, "visualize.py"]),
]

STAGE_NAMES = [s for s, _ in STAGES]


def main():
    parser = argparse.ArgumentParser(description="Run the full faculty-analysis pipeline")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--from", dest="from_stage", choices=STAGE_NAMES, metavar="STAGE",
                       help="Start from this stage (inclusive)")
    group.add_argument("--only", choices=STAGE_NAMES, metavar="STAGE",
                       help="Run only this stage")
    args = parser.parse_args()

    if args.only:
        run_stages = [(n, c) for n, c in STAGES if n == args.only]
    elif args.from_stage:
        idx = STAGE_NAMES.index(args.from_stage)
        run_stages = STAGES[idx:]
    else:
        run_stages = STAGES

    for name, cmd in run_stages:
        banner = f"  {name.upper()}  "
        print(f"\n{'='*60}")
        print(f"{banner:^60}")
        print(f"{'='*60}")
        result = subprocess.run(cmd)
        if result.returncode != 0:
            print(f"\n[pipeline] Stage '{name}' failed. Stopping.")
            sys.exit(result.returncode)

    print("\n[pipeline] Done.")


if __name__ == "__main__":
    main()
