#!/usr/bin/env python3
"""
Compare input and output accounting files to verify 1:1 file mapping.

Usage:
    python3 compare_accounting.py <output_json> <input_ndjson>

The output_json is formatted as:
    {
        "/path/to/bundle.zip": [
            {
                "logical_name": "/data/...",
                "checksum": {...},
                ...
            },
            ...
        ],
        ...
    }

The input_ndjson is newline-delimited JSON, each line a file record:
    {"uuid": "...", "logical_name": "/data/...", ...}
    {"uuid": "...", "logical_name": "/data/...", ...}
    ...

Extracts run number and file number from filenames (pattern: Run00XXXXXX_Subrun00000000_YYYYYYY)
and verifies 1:1 mapping between input and output files.
"""

import json
import sys
import re
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Set, Tuple


def extract_run_and_file_number(filename: str) -> Tuple[int, int] | None:
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


def load_output_accounting(output_json_path: Path) -> Tuple[Dict[Tuple[int, int], Dict], Dict[Tuple[int, int], str]]:
    """
    Load output accounting JSON and build map of (run, file_num) -> file_record.
    Also extract UUIDs from bundle paths.
    
    Returns:
        (file_map, uuid_map) where uuid_map is (run, file_num) -> uuid
    """
    with open(output_json_path) as f:
        output_data = json.load(f)
    
    output_map = {}
    uuid_map = {}
    for bundle_path, files in output_data.items():
        # Extract UUID from bundle path (e.g., ".../ec281704b55511eb8013bedaff42a7c6.zip")
        bundle_uuid = Path(bundle_path).stem  # removes .zip extension
        
        for file_record in files:
            logical_name = file_record.get("logical_name", "")
            # Extract just filename from logical_name (e.g., "Pass3_Step1_PhysicsFiltering_Run00133910_Subrun00000000_00000090.i3.zst")
            filename = Path(logical_name).name
            result = extract_run_and_file_number(filename)
            if result:
                run_num, file_num = result
                key = (run_num, file_num)
                if key in output_map:
                    print(f"WARNING: Duplicate (run, file_num) in output: {key}", file=sys.stderr)
                output_map[key] = file_record
                uuid_map[key] = bundle_uuid
    
    return output_map, uuid_map


def load_input_accounting(input_ndjson_path: Path) -> Tuple[Dict[Tuple[int, int], Dict], Dict[Tuple[int, int], str]]:
    """
    Load input accounting NDJSON and build map of (run, file_num) -> file_record.
    Also extract UUIDs from records.
    
    Returns:
        (file_map, uuid_map) where uuid_map is (run, file_num) -> uuid
    """
    input_map = {}
    uuid_map = {}
    with open(input_ndjson_path) as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"ERROR: Failed to parse JSON at line {line_num}: {e}", file=sys.stderr)
                continue
            
            logical_name = record.get("logical_name", "")
            # Extract just filename (e.g., "PFRaw_PhysicsFiltering_Run00133910_Subrun00000000_00000090.tar.gz")
            filename = Path(logical_name).name
            result = extract_run_and_file_number(filename)
            if result:
                run_num, file_num = result
                key = (run_num, file_num)
                if key in input_map:
                    print(f"WARNING: Duplicate (run, file_num) in input: {key}", file=sys.stderr)
                input_map[key] = record
                file_uuid = record.get("uuid")
                if file_uuid:
                    uuid_map[key] = file_uuid
    
    return input_map, uuid_map


def compare_accounting(output_map: Dict, output_uuid_map: Dict, 
                      input_map: Dict, input_uuid_map: Dict) -> Tuple[bool, Dict]:
    """
    Compare input and output accounting maps, including UUID verification.
    
    Returns:
        (is_valid, summary_dict)
    """
    output_keys = set(output_map.keys())
    input_keys = set(input_map.keys())
    
    summary = {
        "total_input_files": len(input_keys),
        "total_output_files": len(output_keys),
        "run_numbers": sorted(set(run for run, _ in input_keys | output_keys)),
        "max_file_number_per_run": {},
        "uuid_mismatches": [],
        "files_by_uuid": {},
    }
    
    # Compute max file number per run
    for run_num, file_num in input_keys | output_keys:
        if run_num not in summary["max_file_number_per_run"]:
            summary["max_file_number_per_run"][run_num] = file_num
        else:
            summary["max_file_number_per_run"][run_num] = max(summary["max_file_number_per_run"][run_num], file_num)
    
    # Check for files only in input
    only_in_input = input_keys - output_keys
    if only_in_input:
        print("ERROR: Files only in input (not in output):", file=sys.stderr)
        for run_num, file_num in sorted(only_in_input):
            logical_name = input_map[(run_num, file_num)].get("logical_name", "")
            uuid = input_uuid_map.get((run_num, file_num), "N/A")
            print(f"  Run {run_num:06d}, File {file_num:07d}, UUID {uuid}: {logical_name}", file=sys.stderr)
        summary["files_only_in_input"] = sorted(only_in_input)
    
    # Check for files only in output
    only_in_output = output_keys - input_keys
    if only_in_output:
        print("ERROR: Files only in output (not in input):", file=sys.stderr)
        for run_num, file_num in sorted(only_in_output):
            logical_name = output_map[(run_num, file_num)].get("logical_name", "")
            uuid = output_uuid_map.get((run_num, file_num), "N/A")
            print(f"  Run {run_num:06d}, File {file_num:07d}, UUID {uuid}: {logical_name}", file=sys.stderr)
        summary["files_only_in_output"] = sorted(only_in_output)
    
    # Check UUID matches for common files
    common_keys = input_keys & output_keys
    uuid_mismatches = []
    for run_num, file_num in common_keys:
        input_uuid = input_uuid_map.get((run_num, file_num))
        output_uuid = output_uuid_map.get((run_num, file_num))
        
        # Build key for grouping by UUID
        key_uuid = input_uuid or output_uuid or "unknown"
        if key_uuid not in summary["files_by_uuid"]:
            summary["files_by_uuid"][key_uuid] = {
                "count": 0,
                "mismatches": 0,
                "files": []
            }
        summary["files_by_uuid"][key_uuid]["count"] += 1
        summary["files_by_uuid"][key_uuid]["files"].append((run_num, file_num))
        
        if input_uuid != output_uuid:
            uuid_mismatches.append({
                "run_number": run_num,
                "file_number": file_num,
                "input_uuid": input_uuid,
                "output_uuid": output_uuid,
                "input_logical_name": input_map[(run_num, file_num)].get("logical_name", ""),
                "output_logical_name": output_map[(run_num, file_num)].get("logical_name", ""),
            })
            print(f"ERROR: UUID mismatch for Run {run_num:06d}, File {file_num:07d}: input={input_uuid}, output={output_uuid}", file=sys.stderr)
            summary["files_by_uuid"][key_uuid]["mismatches"] += 1
    
    if uuid_mismatches:
        summary["uuid_mismatches"] = uuid_mismatches
    
    is_valid = (len(only_in_input) == 0) and (len(only_in_output) == 0) and (len(uuid_mismatches) == 0)
    return is_valid, summary


def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <output_json> <input_ndjson>", file=sys.stderr)
        sys.exit(1)
    
    output_json_path = Path(sys.argv[1])
    input_ndjson_path = Path(sys.argv[2])
    
    if not output_json_path.exists():
        print(f"ERROR: Output JSON file not found: {output_json_path}", file=sys.stderr)
        sys.exit(1)
    
    if not input_ndjson_path.exists():
        print(f"ERROR: Input NDJSON file not found: {input_ndjson_path}", file=sys.stderr)
        sys.exit(1)
    
    print(f"Loading output accounting from {output_json_path}...", file=sys.stderr)
    output_map, output_uuid_map = load_output_accounting(output_json_path)
    print(f"Loaded {len(output_map)} output files", file=sys.stderr)
    
    print(f"Loading input accounting from {input_ndjson_path}...", file=sys.stderr)
    input_map, input_uuid_map = load_input_accounting(input_ndjson_path)
    print(f"Loaded {len(input_map)} input files", file=sys.stderr)
    
    print("Comparing accounting...", file=sys.stderr)
    is_valid, summary = compare_accounting(output_map, output_uuid_map, input_map, input_uuid_map)
    
    print("\n" + "="*60, file=sys.stderr)
    print("SUMMARY", file=sys.stderr)
    print("="*60, file=sys.stderr)
    print(f"Mapping is valid: {is_valid}", file=sys.stderr)
    print(f"Total input files: {summary['total_input_files']}", file=sys.stderr)
    print(f"Total output files: {summary['total_output_files']}", file=sys.stderr)
    print(f"Run numbers: {summary['run_numbers']}", file=sys.stderr)
    print(f"\nMax file number per run:", file=sys.stderr)
    for run_num in sorted(summary['max_file_number_per_run'].keys()):
        max_file = summary['max_file_number_per_run'][run_num]
        print(f"  Run {run_num:06d}: {max_file:07d}", file=sys.stderr)
    
    if "files_only_in_input" in summary:
        print(f"\nFiles only in input: {len(summary['files_only_in_input'])}", file=sys.stderr)
    if "files_only_in_output" in summary:
        print(f"Files only in output: {len(summary['files_only_in_output'])}", file=sys.stderr)
    
    print("="*60, file=sys.stderr)
    
    # Write per-UUID comparison summary files
    output_json_dir = output_json_path.parent
    written_files = []
    for uuid, uuid_data in summary["files_by_uuid"].items():
        uuid_summary = {
            "uuid": uuid,
            "file_count": uuid_data["count"],
            "uuid_mismatches": uuid_data["mismatches"],
            "files": sorted(uuid_data["files"]),
            "is_valid": uuid_data["mismatches"] == 0,
        }
        
        uuid_output_path = output_json_dir / f"{uuid}.filecount.comparison.json"
        with open(uuid_output_path, "w") as f:
            json.dump(uuid_summary, f, indent=2)
        written_files.append(str(uuid_output_path))
        print(f"Wrote {uuid_output_path}", file=sys.stderr)
    
    print("="*60, file=sys.stderr)
    
    # Output summary as JSON to stdout
    print(json.dumps(summary, indent=2))
    
    sys.exit(0 if is_valid else 1)


if __name__ == "__main__":
    main()
