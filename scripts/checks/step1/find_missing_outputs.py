#!/usr/bin/env python3
"""
find_missing_outputs.py

Find gaps in pass3 step1 outputs for a calendar year, and compare them
against the pass2 known-bad list (data/pass2_bad.json).

This scans the processed outputs, not the source manifests:
    /data/exp/IceCube/<calyr>/filtered/pass3/step1/<MMDD>/*.zst

Output filenames look like
Pass3_Step1_PhysicsFiltering_Run00123615_Subrun00000000_00000001.i3.zst
so run number and file number are captured with regex, not string offsets.

A run's expected sequence is 0..max(present), so files before the first
present file (e.g. files 0-2) are caught as missing too.

Missing file numbers that ARE in the pass2 known-bad list are filtered out,
not faked into the file list. They are reported separately.

Usage:
    python3 find_missing_outputs.py <calyr> [month] [options]

Options:
    --step1-root PATH    base path of the output tree,
                         default /data/exp/IceCube
    --bad-list PATH      pass2 known-bad list, default data/pass2_bad.json
    --run-info PATH      run metadata for Started/Good,
                         default data/i3live-2026-07.json
    --verbose            print progress to stderr
"""

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

FILENAME_RE = re.compile(r"Run(\d+)_Subrun\d+_(\d+)\.")
REPO_ROOT = Path(__file__).resolve().parents[3]


def log_verbose(enabled, message):
    if enabled:
        print(message, file=sys.stderr)


def load_pass2_bad(bad_list_path, verbose):
    try:
        with open(bad_list_path, "r") as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error loading pass2 bad list '{bad_list_path}': {e}", file=sys.stderr)
        sys.exit(1)

    known = {
        (int(entry["run_id"]), int(entry["sub_run"]))
        for entry in data
        if "run_id" in entry and "sub_run" in entry
    }
    log_verbose(verbose, f"loaded {len(known)} (run_id, sub_run) pairs from {bad_list_path}")
    return known


def load_run_info(run_info_path, verbose):
    try:
        with open(run_info_path, "r") as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error loading run info '{run_info_path}': {e}", file=sys.stderr)
        sys.exit(1)

    run_info = {}
    for item in data:
        run_id = item.get("run_number")
        if run_id is not None:
            run_info[int(run_id)] = item
    log_verbose(verbose, f"loaded {len(run_info)} runs from {run_info_path}")
    return run_info


def scan_outputs(step1_root, calyr, months, verbose):
    # run -> (set of present file numbers, set of mmdd dirs where outputs live)
    run_files = defaultdict(set)
    run_dirs = defaultdict(set)
    scanned = 0
    skipped = 0

    for month in months:
        mm = f"{month:02d}"
        for file_path in sorted(step1_root.glob(f"{mm}[0-9][0-9]/*.zst")):
            scanned += 1
            if scanned % 1000 == 0:
                log_verbose(verbose, f"scanned {scanned} .zst files so far")
            match = FILENAME_RE.search(file_path.name)
            if not match:
                skipped += 1
                continue
            run = int(match.group(1))
            file_num = int(match.group(2))
            run_files[run].add(file_num)
            run_dirs[run].add(file_path.parent.name)

    log_verbose(verbose, f"scanned {scanned} .zst files, {skipped} did not match, {len(run_files)} runs")
    return run_files, run_dirs


def find_gaps(run_files):
    gaps = {}
    for run, files in run_files.items():
        max_file = max(files)
        missing = sorted(set(range(0, max_file + 1)) - files)
        if missing:
            gaps[run] = missing
    return gaps


def main():
    parser = argparse.ArgumentParser(
        description="Find gaps in pass3 step1 outputs and compare against the pass2 known-bad list."
    )
    parser.add_argument("calyr", help="calendar year, e.g. 2014")
    parser.add_argument("month", nargs="?", type=int,
                        help="single month (1-12); default: all months")
    parser.add_argument("--step1-root", default="/data/exp/IceCube",
                        help="base path of the output tree, default /data/exp/IceCube")
    parser.add_argument("--bad-list", default=REPO_ROOT / "data" / "pass2_bad.json",
                        help="pass2 known-bad JSON list, default data/pass2_bad.json")
    parser.add_argument("--run-info", default=REPO_ROOT / "data" / "i3live-2026-07.json",
                        help="run metadata JSON list, default data/i3live-2026-07.json")
    parser.add_argument("--verbose", action="store_true",
                        help="print progress to stderr")

    args = parser.parse_args()

    calyr = args.calyr
    if args.month is not None and not (1 <= args.month <= 12):
        print(f"Error: month must be 1-12, got {args.month}.", file=sys.stderr)
        sys.exit(1)
    months = [args.month] if args.month is not None else list(range(1, 13))

    step1_root = Path(args.step1_root) / calyr / "filtered" / "pass3" / "step1"
    if not step1_root.is_dir():
        print(f"Error: the directory '{step1_root}' does not exist.", file=sys.stderr)
        sys.exit(1)

    bad_list_path = Path(args.bad_list)
    if not bad_list_path.is_file():
        print(f"Error: the bad list '{bad_list_path}' does not exist.", file=sys.stderr)
        sys.exit(1)

    run_info_path = Path(args.run_info)
    if not run_info_path.is_file():
        print(f"Error: the run info '{run_info_path}' does not exist.", file=sys.stderr)
        sys.exit(1)

    log_verbose(args.verbose, f"scanning step1 outputs in {step1_root}")

    known = load_pass2_bad(bad_list_path, args.verbose)
    run_info = load_run_info(run_info_path, args.verbose)
    run_files, run_dirs = scan_outputs(step1_root, calyr, months, args.verbose)
    gaps = find_gaps(run_files)
    log_verbose(args.verbose, f"found gaps in {len(gaps)} runs")

    new_by_run = {}
    known_by_run = {}
    for run in sorted(gaps):
        new_missing = []
        known_missing = []
        for file_num in gaps[run]:
            if (run, file_num) in known:
                known_missing.append(file_num)
            else:
                new_missing.append(file_num)
        if new_missing:
            new_by_run[run] = new_missing
        if known_missing:
            known_by_run[run] = known_missing

    if not gaps:
        print("All runs have perfectly continuous output files!")
        return

    if new_by_run:
        total_new = sum(len(nums) for nums in new_by_run.values())
        print(f"Found {total_new} missing files across {len(new_by_run)} unique Run IDs:\n")
        for run in sorted(new_by_run):
            details = run_info.get(run, {})
            start_date = details.get("start", "Unknown Date")
            good_run = details.get("latest_snapshot", {}).get("good_i3", False)
            print(f"Run {run} (Started: {start_date}, Good: {good_run}) is missing files! "
                  f"Missing file numbers: {new_by_run[run]}")
            print(f"  dirs: {sorted(run_dirs.get(run, []))}")
    else:
        print("All missing files were already in the pass2 known-bad list.")

    if known_by_run:
        total_known = sum(len(nums) for nums in known_by_run.values())
        print(f"\nAlready in the pass2 known-bad list "
              f"({total_known} files across {len(known_by_run)} runs), excluded above:")
        for run in sorted(known_by_run):
            print(f"  Run {run}: {known_by_run[run]} ({len(known_by_run[run])} files)")
            print(f"  dirs: {sorted(run_dirs.get(run, []))}")


if __name__ == "__main__":
    main()
