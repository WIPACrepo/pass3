import argparse
import json
import os
import re
import sys
from collections import defaultdict


def load_run_info(file3_path):
    """Loads run information (like start date) from File 3.
    
    Assumes File 3 is:
    - An array of objects: [{'run_number': 124695, 'start': '2026-07-15'}, ...]
    """
    try:
        with open(file3_path, "r") as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error reading or parsing run info JSON file '{file3_path}': {e}", file=sys.stderr)
        sys.exit(1)

    run_info_map = {}
    for item in data:
        run_id = item.get("run_number")
        if run_id is not None:
            run_info_map[int(run_id)] = item
    return run_info_map


def find_missing_items(file1_path, file2_path):
    # 1. Parse File 2 (JSON) and load into a set of (run_id, sub_run) tuples
    try:
        with open(file2_path, "r") as f:
            file2_data = json.load(f)
    except Exception as e:
        print(f"Error reading or parsing JSON file '{file2_path}': {e}", file=sys.stderr)
        sys.exit(1)

    existing_runs = {
        (int(item["run_id"]), int(item["sub_run"])) for item in file2_data
    }

    # 2. Parse File 1 (Text File)
    line_pattern = re.compile(
        r"Run\s+(\d+)\s+is missing files!\s+Missing file numbers:\s+\[(.*?)\]"
    )

    # Use a defaultdict of lists to group missing sub_runs by run_id automatically
    missing_by_run = defaultdict(list)

    try:
        with open(file1_path, "r") as f:
            for line_line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue

                match = line_pattern.search(line)
                if not match:
                    print(
                        f"Warning: Line {line_line_num} did not match pattern: {line}",
                        file=sys.stderr
                    )
                    continue

                run_id = int(match.group(1))
                sub_runs_str = match.group(2).strip()
                if not sub_runs_str:
                    continue 

                sub_runs = [int(x.strip()) for x in sub_runs_str.split(",")]

                # Check which sub-runs from File 1 do NOT exist in File 2
                for sub_run in sub_runs:
                    if (run_id, sub_run) not in existing_runs:
                        missing_by_run[run_id].append(sub_run)
                        
    except Exception as e:
        print(f"Error reading text file '{file1_path}': {e}", file=sys.stderr)
        sys.exit(1)

    return missing_by_run


def main():
    # Set up the argument parser
    parser = argparse.ArgumentParser(
        description="Find runs/sub-runs in a text file (File 1) that are missing from a JSON file (File 2)."
    )
    
    parser.add_argument(
        "-f1", "--file1",
        required=True,
        help="Path to the text file containing the missing runs/sub-runs list (File 1)"
    )
    parser.add_argument(
        "-f2", "--file2",
        required=True,
        help="Path to the JSON file containing existing runs (File 2)"
    )
    parser.add_argument(
        "-f3", "--file3",
        required=True,
        help="Path to the JSON file containing run details and start dates (File 3)"
    )

    args = parser.parse_args()

    # Validate that all three files exist
    for filepath in (args.file1, args.file2, args.file3):
        if not os.path.isfile(filepath):
            print(f"Error: The file '{filepath}' does not exist.", file=sys.stderr)
            sys.exit(1)

    # 1. Load run details (dates) from File 3
    run_info = load_run_info(args.file3)

    # Run the comparison logic (returns a dict: {run_id: [sub_runs]})
    results = find_missing_items(args.file1, args.file2)

    # Output the results grouped by run_id
    if results:
        total_missing_count = sum(len(sub_runs) for sub_runs in results.values())
        print(f"Found {total_missing_count} missing files across {len(results)} unique Run IDs:\n")
        
        # Sort by run_id so the output is chronological/sequential
        for run_id in sorted(results.keys()):
            # Sort the sub-runs so they display in order (e.g., [11, 12] instead of [12, 11])
            sorted_sub_runs = sorted(results[run_id])

            # Retrieve the start date from the File 3 metadata dictionary
            run_details = run_info.get(run_id, {})
            start_date = run_details.get("start", "Unknown Date")
            good_run = run_details.get("latest_snapshot", {}).get('good_i3', False)

            print(f"Run {run_id} (Started: {start_date}, Good: {good_run}) is missing files! Missing file numbers: {sorted_sub_runs}")
    else:
        print("All runs and files from File 1 were found in File 2.")


if __name__ == "__main__":
    main()