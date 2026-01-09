#!/usr/bin/env python3
"""
Summarize all files for a given run number from UUID-based accounting files.

Searches through <uuid>.json (output accounting) and <uuid>*.ndjson (input accounting)
files and accumulates information for a specific run, creating separate summaries for:
- PFRaw files (input files from NDJSON manifests)
- Online_Pass3/Pass3_Step1 files (output files from JSON accounting)

Usage:
    python3 summarize_run_files.py <run_number> <search_directory> [--search-days DAYS]

The script searches the given directory and +/- N days (default: 1) for UUID files.
"""

import json
import sys
import re
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Set, Optional, Tuple
from collections import defaultdict


def extract_run_and_file_number(filename: str) -> Optional[Tuple[int, int]]:
    """
    Extract run number and file number from filename.
    
    Pattern: Run00XXXXXX_Subrun00000000_YYYYYYY
    Returns (run_number, file_number) or None if not found.
    """
    match = re.search(r'Run(\d+)_Subrun(\d+)_(\d+)', filename)
    if match:
        run_num = int(match.group(1))
        file_num = int(match.group(3))
        return (run_num, file_num)
    return None


def parse_date_from_path(path: Path) -> Optional[datetime]:
    """
    Try to extract date from path components (e.g., YYYY/MMDD or YYYY-MM-DD).
    Returns datetime object or None if not parseable.
    """
    parts = path.parts
    for i in range(len(parts) - 1):
        # Try YYYY/MMDD format
        try:
            year = int(parts[i])
            if 2000 <= year <= 2100:
                mmdd = parts[i + 1]
                if len(mmdd) == 4:
                    month = int(mmdd[:2])
                    day = int(mmdd[2:])
                    return datetime(year, month, day)
        except (ValueError, IndexError):
            pass
    return None


def get_search_paths(base_dir: Path, search_days: int = 1, year: Optional[int] = None, month: Optional[int] = None) -> List[Path]:
    """
    Generate list of directories to search based on base_dir and +/- search_days.
    If base_dir contains a date pattern (YYYY/MMDD), search nearby dates.
    If year and month are provided, search all MMDD directories for that month.
    Otherwise, just return base_dir and recursively search.
    """
    # If year and month are provided, search all days in that month
    if year is not None and month is not None:
        import calendar
        search_paths = []
        _, num_days = calendar.monthrange(year, month)
        
        for day in range(1, num_days + 1):
            mmdd = f"{month:02d}{day:02d}"
            # Try to construct path with year/mmdd
            candidate_path = base_dir / str(year) / mmdd
            if candidate_path.exists():
                search_paths.append(candidate_path)
        
        if search_paths:
            return search_paths
        # Fallback if no matching paths found
        return [base_dir]
    
    # If year is provided (but not month), search entire year
    if year is not None:
        search_paths = []
        year_path = base_dir / str(year)
        if year_path.exists():
            # Recursively search all subdirectories under year
            for mmdd_path in sorted(year_path.glob("*")):
                if mmdd_path.is_dir():
                    search_paths.append(mmdd_path)
        if search_paths:
            return search_paths
        return [base_dir]
    
    # Original logic: date-based search with +/- days
    base_date = parse_date_from_path(base_dir)
    
    if base_date is None:
        # No date pattern found; search recursively from base_dir
        return [base_dir]
    
    search_paths = []
    for delta in range(-search_days, search_days + 1):
        target_date = base_date + timedelta(days=delta)
        # Reconstruct path with new date
        date_str_mmdd = f"{target_date.month:02d}{target_date.day:02d}"
        year_str = str(target_date.year)
        
        # Try to replace date components in path
        new_parts = []
        replaced = False
        parts = base_dir.parts
        for i, part in enumerate(parts):
            if not replaced:
                try:
                    if int(part) == base_date.year and i + 1 < len(parts):
                        new_parts.append(year_str)
                        replaced = True
                        continue
                except ValueError:
                    pass
            if replaced and len(new_parts) > 0 and new_parts[-1] == year_str and len(part) == 4:
                # This should be the MMDD part
                new_parts.append(date_str_mmdd)
                replaced = False  # Mark that we've done the replacement
            else:
                new_parts.append(part)
        
        candidate_path = Path(*new_parts) if new_parts else base_dir
        if candidate_path.exists():
            search_paths.append(candidate_path)
    
    # Fallback: just return base_dir if date parsing/reconstruction failed
    if not search_paths:
        search_paths = [base_dir]
    
    return search_paths


def find_uuid_files(search_dirs: List[Path]) -> Tuple[List[Path], List[Path]]:
    """
    Find all <uuid>.json and <uuid>*.ndjson files in search directories.
    
    Returns:
        (json_files, ndjson_files)
    """
    json_files = []
    ndjson_files = []
    
    for search_dir in search_dirs:
        if not search_dir.exists():
            continue
        
        # Find all .json files (UUID accounting for output)
        for json_file in search_dir.glob("*.json"):
            # Skip if it looks like a summary/comparison file
            if any(x in json_file.name for x in ["summary", "comparison", "contents"]):
                continue
            json_files.append(json_file)
        
        # Find all .ndjson files (UUID manifests for input)
        for ndjson_file in search_dir.glob("*.ndjson"):
            ndjson_files.append(ndjson_file)
    
    return json_files, ndjson_files


def extract_pfraw_files_for_run(ndjson_files: List[Path], run_number: int) -> List[Dict]:
    """
    Extract PFRaw file records for a given run from NDJSON manifest files.
    
    Returns list of file records matching the run.
    """
    pfraw_files = []
    
    for ndjson_file in ndjson_files:
        try:
            with open(ndjson_file) as f:
                for line_num, line in enumerate(f, start=1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                        logical_name = record.get("logical_name", "")
                        filename = Path(logical_name).name
                        
                        result = extract_run_and_file_number(filename)
                        if result:
                            file_run, file_num = result
                            if file_run == run_number:
                                # Add source information
                                record["_source_manifest"] = str(ndjson_file)
                                pfraw_files.append(record)
                    except json.JSONDecodeError as e:
                        print(f"Warning: Failed to parse JSON in {ndjson_file} at line {line_num}: {e}", 
                              file=sys.stderr)
                        continue
        except Exception as e:
            print(f"Warning: Failed to read {ndjson_file}: {e}", file=sys.stderr)
            continue
    
    return pfraw_files


def extract_pass3_files_for_run(json_files: List[Path], run_number: int) -> List[Dict]:
    """
    Extract Pass3/Online_Pass3 output file records for a given run from UUID JSON accounting files.
    
    Returns list of file records matching the run.
    """
    pass3_files = []
    
    for json_file in json_files:
        try:
            with open(json_file) as f:
                data = json.load(f)
            
            # The JSON format is: { "bundle_path": [ {file_record}, ... ], ... }
            for bundle_path, files in data.items():
                for file_record in files:
                    logical_name = file_record.get("logical_name", "")
                    filename = Path(logical_name).name
                    
                    result = extract_run_and_file_number(filename)
                    if result:
                        file_run, file_num = result
                        if file_run == run_number:
                            # Add source information
                            file_record["_source_accounting"] = str(json_file)
                            file_record["_bundle_path"] = bundle_path
                            pass3_files.append(file_record)
        except Exception as e:
            print(f"Warning: Failed to read {json_file}: {e}", file=sys.stderr)
            continue
    
    return pass3_files


def extract_all_runs(ndjson_files: List[Path], json_files: List[Path]) -> Set[int]:
    """
    Extract all unique run numbers from both NDJSON and JSON files.
    """
    runs = set()
    
    for ndjson_file in ndjson_files:
        try:
            with open(ndjson_file) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                        logical_name = record.get("logical_name", "")
                        filename = Path(logical_name).name
                        result = extract_run_and_file_number(filename)
                        if result:
                            runs.add(result[0])
                    except json.JSONDecodeError:
                        continue
        except Exception:
            pass
    
    for json_file in json_files:
        try:
            with open(json_file) as f:
                data = json.load(f)
            for bundle_path, files in data.items():
                for file_record in files:
                    logical_name = file_record.get("logical_name", "")
                    filename = Path(logical_name).name
                    result = extract_run_and_file_number(filename)
                    if result:
                        runs.add(result[0])
        except Exception:
            pass
    
    return runs


def write_summary(output_path: Path, files: List[Dict], file_type: str, run_number: int):
    """
    Write summary JSON file for collected files.
    """
    summary = {
        "run_number": run_number,
        "file_type": file_type,
        "total_files": len(files),
        "files": sorted(files, key=lambda x: extract_run_and_file_number(Path(x.get("logical_name", "")).name)[1] 
                       if extract_run_and_file_number(Path(x.get("logical_name", "")).name) else 0)
    }
    
    with open(output_path, "w") as f:
        json.dump(summary, f, indent=2)
    
    print(f"Wrote {len(files)} {file_type} files to {output_path}", file=sys.stderr)


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Summarize files for a run or all runs in a time period from UUID accounting files"
    )
    parser.add_argument("search_directory", type=Path, 
                       help="Base directory to search for UUID files")
    parser.add_argument("--run", type=int, default=None,
                       help="Run number to search for (optional; if not provided, summarize all runs)")
    parser.add_argument("--year", type=int, default=None,
                       help="Year to search for (e.g., 2020); summarizes all runs in that year")
    parser.add_argument("--month", type=int, default=None,
                       help="Month to search for (1-12); requires --year")
    parser.add_argument("--search-days", type=int, default=1,
                       help="Number of days +/- to search from base directory date (default: 1)")
    parser.add_argument("--output-dir", type=Path, default=None,
                       help="Directory to write summary files (default: search_directory)")
    
    args = parser.parse_args()
    
    if not args.search_directory.exists():
        print(f"ERROR: Search directory {args.search_directory} does not exist", file=sys.stderr)
        sys.exit(1)
    
    if args.month is not None and args.year is None:
        print("ERROR: --month requires --year", file=sys.stderr)
        sys.exit(1)
    
    output_dir = args.output_dir if args.output_dir else args.search_directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Get search paths (pass year/month if provided)
    search_paths = get_search_paths(args.search_directory, args.search_days, args.year, args.month)
    print(f"Search paths: {search_paths}", file=sys.stderr)
    
    # Find UUID files
    json_files, ndjson_files = find_uuid_files(search_paths)
    print(f"Found {len(json_files)} UUID JSON files and {len(ndjson_files)} NDJSON manifest files", 
          file=sys.stderr)
    
    # Determine which runs to process
    if args.run is not None:
        # Single run mode
        runs_to_process = [args.run]
        print(f"Searching for run {args.run} files...", file=sys.stderr)
    else:
        # Multi-run mode: extract all runs, optionally filter by year/month
        all_runs = extract_all_runs(ndjson_files, json_files)
        print(f"Found {len(all_runs)} unique runs", file=sys.stderr)
        
        if args.year is not None:
            # Filter by year and optionally month
            if args.month is not None:
                print(f"Filtering for year {args.year}, month {args.month}", file=sys.stderr)
            else:
                print(f"Filtering for year {args.year}", file=sys.stderr)
        
        runs_to_process = sorted(all_runs)
        print(f"Processing {len(runs_to_process)} runs", file=sys.stderr)
    
    # Process each run
    total_pfraw = 0
    total_pass3 = 0
    
    for run_num in runs_to_process:
        print(f"\nProcessing run {run_num:06d}...", file=sys.stderr)
        
        # Extract PFRaw files for this run
        pfraw_files = extract_pfraw_files_for_run(ndjson_files, run_num)
        total_pfraw += len(pfraw_files)
        
        # Extract Pass3 output files for this run
        pass3_files = extract_pass3_files_for_run(json_files, run_num)
        total_pass3 += len(pass3_files)
        
        # Write summaries
        pfraw_output = output_dir / f"Run{run_num:06d}_PFRaw_summary.json"
        pass3_output = output_dir / f"Run{run_num:06d}_Pass3_summary.json"
        
        write_summary(pfraw_output, pfraw_files, "PFRaw", run_num)
        write_summary(pass3_output, pass3_files, "Pass3_Step1", run_num)
        
        print(f"  PFRaw: {len(pfraw_files)} files", file=sys.stderr)
        print(f"  Pass3: {len(pass3_files)} files", file=sys.stderr)
    
    print("\n" + "="*60, file=sys.stderr)
    if args.run is not None:
        print(f"Summary for Run {args.run:06d}", file=sys.stderr)
    elif args.year is not None:
        if args.month is not None:
            print(f"Summary for Year {args.year}, Month {args.month}", file=sys.stderr)
        else:
            print(f"Summary for Year {args.year}", file=sys.stderr)
    else:
        print("Summary for All Runs", file=sys.stderr)
    print("="*60, file=sys.stderr)
    print(f"Total PFRaw files: {total_pfraw}", file=sys.stderr)
    print(f"Total Pass3 output files: {total_pass3}", file=sys.stderr)
    print(f"Runs processed: {len(runs_to_process)}", file=sys.stderr)
    print("="*60, file=sys.stderr)


if __name__ == "__main__":
    main()
