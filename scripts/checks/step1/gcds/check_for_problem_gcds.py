"""
This script takes the summary gcd diff files and checks if there are any unexpected changes to the GCD. It looks at the "cal" section of the summary and checks the "changed" field. If there are more than a certain number of changes (default is 0), it flags it as a problem. The results are saved to a summary file in JSON format, which includes lists of runs with no problems and runs with problems.
"""

import json
import argparse
import pathlib


def main():
    parser = argparse.ArgumentParser(description="Check charge peak LLH delta")
    parser.add_argument("--files", nargs='+', type=pathlib.Path, help="Path to the data file (JSON)")
    parser.add_argument("--summary-file", type=pathlib.Path, default="gcd_summary.txt", help="Path to the summary output file")
    parser.add_argument("--change-limit", type=int, default=0, help="Limit ")
    args = parser.parse_args()

    no_problems = []
    problems = []

    for fn in args.files:
        print(f"Checking file: {fn}")
        # Example file path
        # /data/ana/Calibration/Pass3_Monitoring/online/charge_correction_atwd_fadc/2019/133574/Run133574.fadc_atwd_charge_comparison_results.json
        # This is mean for python3.9
        run_number = fn.parents[0].name
        with open(fn, 'r') as f:
            data = json.load(f)
            if len(data["cal"]["changed"]) > args.change_limit:
                print(f"Unexpected changed to GCD for file: {fn}")
                problems.append((run_number, data["cal"]["changed"]))
            else:
                print(f"No unexpected changes to GCD for file: {fn}")
                no_problems.append((run_number, data["cal"]["changed"]))

    with open(args.summary_file, 'w') as f:
        out = {
            "no_problems": no_problems,
            "problems": problems
        }
        json.dump(out, f, indent=4)
        
if __name__ == "__main__":
    main()    
