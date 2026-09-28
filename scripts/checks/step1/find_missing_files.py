#!/usr/bin/env python3
"""
find_missing_files.py

Find gaps in PFRaw manifests and compare them against the pass2 known-bad
list (data/pass2_bad.json).

The manifests are JSON files with a "files" list. Each entry has a
"fileName" that looks like
PFRaw_..._Run00123456_Subrun00000000_00000123.tar.gz.
The manifests live in MMDD subfolders per year, so the search is recursive.

A run's expected sequence is 0..max(present), so files before the first
present file (e.g. files 0-2) are caught as missing too.

Missing file numbers that are NOT in the pass2 known-bad list are printed in
the same format as scripts/checks/step1/check_bad_files.py. The ones that ARE
in the list are printed in a separate section.

Usage:
    python3 find_missing_files.py /path/to/manifests [options]

Options:
    --file-ending EXT   manifest file ending, default ".json"
    --bad-list PATH     pass2 known-bad list, default data/pass2_bad.json
    --run-info PATH     run metadata for Started/Good, default data/i3live-2026-07.json
    --verbose           print progress to stderr
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


def map_manifests(target_dir, file_ending, verbose):
    run_mapping = defaultdict(set)
    manifest_count = 0
    entry_count = 0

    for file_path in sorted(target_dir.rglob(f"*{file_ending}")):
        if not file_path.is_file():
            continue
        manifest_count += 1
        if manifest_count % 100 == 0:
            log_verbose(verbose, f"read {manifest_count} manifests, {entry_count} file entries so far")
        try:
            with open(file_path, "r") as f:
                data = json.load(f)
        except json.JSONDecodeError:
            print(f"Warning: invalid JSON in -> {file_path}", file=sys.stderr)
            continue

        for entry in data.get("files", []):
            match = FILENAME_RE.search(entry.get("fileName", ""))
            if match:
                entry_count += 1
                run_mapping[int(match.group(1))].add(int(match.group(2)))

    log_verbose(verbose, f"read {manifest_count} manifests, {entry_count} file entries, {len(run_mapping)} runs")
    return run_mapping


def find_gaps(run_mapping):
    gaps = {}
    for run, files in run_mapping.items():
        if not files:
            continue
        max_file = max(files)
        expected = set(range(0, max_file + 1))
        missing = sorted(expected - files)
        if missing:
            gaps[run] = missing
    return gaps


def main():
    parser = argparse.ArgumentParser(
        description="Find gaps in PFRaw manifests and compare against the pass2 known-bad list."
    )
    parser.add_argument("directory", help="directory to search recursively for manifest files")
    parser.add_argument("--file-ending", default=".json",
                        help="manifest file ending, default .json")
    parser.add_argument("--bad-list", default=REPO_ROOT / "data" / "pass2_bad.json",
                        help="pass2 known-bad JSON list, default data/pass2_bad.json")
    parser.add_argument("--run-info", default=REPO_ROOT / "data" / "i3live-2026-07.json",
                        help="run metadata JSON list, default data/i3live-2026-07.json")
    parser.add_argument("--verbose", action="store_true",
                        help="print progress to stderr")

    args = parser.parse_args()

    file_ending = args.file_ending
    if not file_ending.startswith("."):
        file_ending = f".{file_ending}"

    target_dir = Path(args.directory)
    if not target_dir.is_dir():
        print(f"Error: the directory '{args.directory}' does not exist.", file=sys.stderr)
        sys.exit(1)

    bad_list_path = Path(args.bad_list)
    if not bad_list_path.is_file():
        print(f"Error: the bad list '{bad_list_path}' does not exist.", file=sys.stderr)
        sys.exit(1)

    run_info_path = Path(args.run_info)
    if not run_info_path.is_file():
        print(f"Error: the run info '{run_info_path}' does not exist.", file=sys.stderr)
        sys.exit(1)

    log_verbose(args.verbose, f"searching for '*{file_ending}' in {target_dir.resolve()}")

    known = load_pass2_bad(bad_list_path, args.verbose)
    run_info = load_run_info(run_info_path, args.verbose)
    run_mapping = map_manifests(target_dir, file_ending, args.verbose)
    gaps = find_gaps(run_mapping)
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
        print("All runs have perfectly continuous file numbers!")
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
    else:
        print("All missing files were already in the pass2 known-bad list.")

    if known_by_run:
        total_known = sum(len(nums) for nums in known_by_run.values())
        print(f"\nAlready in the pass2 known-bad list "
              f"({total_known} files across {len(known_by_run)} runs), excluded above:")
        for run in sorted(known_by_run):
            print(f"  Run {run}: {known_by_run[run]} ({len(known_by_run[run])} files)")


if __name__ == "__main__":
    main()
