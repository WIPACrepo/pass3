#!/usr/bin/env python3
"""
find_missing_runs.py

Find good runs that are entirely missing from pass3 step1 outputs,
by comparing the outputs against the good run list (GRL).

The gap checks (find_missing_outputs.py, find_missing_files.py) only
see runs that have at least one output file. A run with zero outputs
is invisible to them. This script catches those: it scans the same
output tree as find_missing_outputs.py
    /data/exp/IceCube/<calyr>/filtered/pass3/step1/<MMDD>/*.zst
and reports every GRL-good run of that year with no .zst outputs.

A run is "good" if good_i3 is true, matching get_grl() in
run_step1.py. A run belongs to a year via its good_tstart date
(no good run spans a year boundary in the current GRLs).

Runs in the temp-bad-runs list (good in the GRL but known to fail,
skipped deliberately by run_step1.py) are excluded and reported
separately.

Missing runs come in two flavors:
    never processed - no output and no raw copy anywhere in the tree
    errored         - a PFRaw copy sits in the output dir but no .zst
                      output was written (same meaning as the errored
                      files in find_missing_outputs.py)

Usage:
    python3 find_missing_runs.py <calyr> [options]

Note: only the <calyr> tree is scanned. If a run's files were archived
under a different year's MMDD dirs, they are not seen here.

Options:
    --step1-root PATH     base path of the output tree,
                          default /data/exp/IceCube
    --grl PATH            i3live good run list JSON,
                          default data/grl-new.json
    --temp-bad-runs PATH  good-but-failing runs to skip,
                          default data/temp_bad_runs_in_grl
    --verbose             print progress to stderr
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


def load_good_runs(grl_path, verbose):
    try:
        with open(grl_path, "r") as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error loading GRL '{grl_path}': {e}", file=sys.stderr)
        sys.exit(1)

    good = {}
    for record in data.get("runs", []):
        run = record.get("run")
        if run is not None and bool(record.get("good_i3", False)):
            good[int(run)] = record
    log_verbose(verbose, f"loaded {len(good)} good runs from {grl_path}")
    return good


def load_temp_bad_runs(path, verbose):
    # optional; a missing file means nothing is excluded
    runs = set()
    if not path.exists():
        log_verbose(verbose, f"no temp bad runs file at {path}, excluding nothing")
        return runs
    try:
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    runs.add(int(line))
    except Exception as e:
        print(f"Error reading temp bad runs '{path}': {e}", file=sys.stderr)
        sys.exit(1)
    log_verbose(verbose, f"loaded {len(runs)} temp bad runs from {path}")
    return runs


def scan_outputs(step1_root, verbose):
    # mirrors scan_outputs() in find_missing_outputs.py, but only
    # tracks what this check needs:
    #   run_zst_dirs: run -> mmdd dirs with .zst outputs
    #   run_raw_dirs: run -> mmdd dirs with PFRaw failure copies
    run_zst_dirs = defaultdict(set)
    run_raw_dirs = defaultdict(set)
    scanned = 0

    for file_path in sorted(step1_root.glob("[0-9][0-9][0-9][0-9]/*.zst")):
        scanned += 1
        if scanned % 1000 == 0:
            log_verbose(verbose, f"scanned {scanned} .zst files so far")
        match = FILENAME_RE.search(file_path.name)
        if not match:
            continue
        run_zst_dirs[int(match.group(1))].add(file_path.parent.name)

    for file_path in sorted(step1_root.glob("[0-9][0-9][0-9][0-9]/*.tar.gz")):
        if "PFRaw" not in file_path.name:
            continue
        match = FILENAME_RE.search(file_path.name)
        if not match:
            continue
        run_raw_dirs[int(match.group(1))].add(file_path.parent.name)

    log_verbose(verbose, f"scanned {scanned} .zst files, "
                         f"{len(run_zst_dirs)} runs with outputs, "
                         f"{len(run_raw_dirs)} runs with raw copies")
    return run_zst_dirs, run_raw_dirs, scanned


def main():
    parser = argparse.ArgumentParser(
        description="Find GRL-good runs entirely missing from pass3 step1 outputs."
    )
    parser.add_argument("calyr", help="calendar year, e.g. 2014")
    parser.add_argument("--step1-root", default="/data/exp/IceCube",
                        help="base path of the output tree, default /data/exp/IceCube")
    parser.add_argument("--grl", default=REPO_ROOT / "data" / "grl-new.json",
                        help="i3live good run list JSON, default data/grl-new.json")
    parser.add_argument("--temp-bad-runs", default=REPO_ROOT / "data" / "temp_bad_runs_in_grl",
                        help="good-but-failing runs to skip, default data/temp_bad_runs_in_grl")
    parser.add_argument("--verbose", action="store_true",
                        help="print progress to stderr")

    args = parser.parse_args()

    calyr = args.calyr
    if len(calyr) != 4 or not calyr.isdigit():
        print(f"Error: '{calyr}' is not a 4-digit calendar year.", file=sys.stderr)
        sys.exit(1)

    step1_root = Path(args.step1_root) / calyr / "filtered" / "pass3" / "step1"
    if not step1_root.is_dir():
        print(f"Error: the directory '{step1_root}' does not exist.", file=sys.stderr)
        sys.exit(1)

    grl_path = Path(args.grl)
    if not grl_path.is_file():
        print(f"Error: the GRL '{grl_path}' does not exist.", file=sys.stderr)
        sys.exit(1)

    log_verbose(args.verbose, f"scanning step1 outputs in {step1_root}")

    good = load_good_runs(grl_path, args.verbose)
    temp_bad = load_temp_bad_runs(Path(args.temp_bad_runs), args.verbose)
    run_zst_dirs, run_raw_dirs, scanned = scan_outputs(step1_root, args.verbose)

    year_runs = {
        run: record for run, record in good.items()
        if str(record.get("good_tstart", ""))[:4] == calyr
    }
    log_verbose(args.verbose, f"{len(year_runs)} good runs started in {calyr}")

    if not year_runs:
        print(f"The GRL has no good runs starting in {calyr}.")
        return

    if scanned == 0:
        print(f"Warning: no .zst output files found under {step1_root}.")

    missing = {
        run: record for run, record in year_runs.items()
        if run not in run_zst_dirs
    }

    excluded = sorted(run for run in missing if run in temp_bad)
    never = sorted(run for run in missing if run not in temp_bad and run not in run_raw_dirs)
    errored = sorted(run for run in missing if run not in temp_bad and run in run_raw_dirs)

    if not missing:
        print(f"All {len(year_runs)} good runs of {calyr} have step1 outputs!")
    else:
        print(f"Found {len(missing)} good runs of {calyr} entirely missing "
              f"from the outputs:\n")
        if never:
            print(f"Never processed ({len(never)} runs, no outputs, no raw copies):")
            for run in never:
                record = year_runs[run]
                print(f"  Run {run} (Started: {record.get('good_tstart', 'Unknown Date')}, "
                      f"Good: True) has no output files.")
        if errored:
            print(f"\nErrored ({len(errored)} runs, raw copy present, no output written):")
            for run in errored:
                record = year_runs[run]
                print(f"  Run {run} (Started: {record.get('good_tstart', 'Unknown Date')}, "
                      f"Good: True) has no output files.")
                print(f"  raw dirs: {sorted(run_raw_dirs[run])}")

    if excluded:
        print(f"\nTemp bad runs (good in the GRL, skipped by the pipeline) "
              f"also missing ({len(excluded)}): {excluded}")


if __name__ == "__main__":
    main()
