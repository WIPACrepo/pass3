"""
This script takes the charge peak LLH delta files and checks if delta LLH is less than a certain value. If it is, it flags it as a problem. The results are saved to a summary file in JSON format, which includes lists of runs with no problems and runs with problems.
"""

import json
import argparse
import pathlib


def main():
    parser = argparse.ArgumentParser(description="Check charge peak LLH delta")
    parser.add_argument("--files", nargs='+', type=pathlib.Path, help="Path to the data file (JSON)")
    parser.add_argument("--summary-file", type=pathlib.Path, default="charge_peak_delta_summary.txt", help="Path to the summary output file")
    parser.add_argument("--llh-limit", type=float, default=100., help="Limit ")
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
            if data["delta_logL"] <= args.llh_limit:
                print(f"Charge peak LLH delta is outside the acceptable range: {data['delta_logL']} from file: {fn}")
                problems.append((run_number, data['delta_logL']))
            else:
                print(f"Charge peak LLH delta is within the acceptable range: {data['delta_logL']} from file: {fn}")
                no_problems.append((run_number, data['delta_logL']))

        with open(args.summary_file, 'w') as f:
            out = {
                "no_problems": no_problems,
                "problems": problems
            }
            json.dump(out, f, indent=4)

if __name__ == "__main__":
    main()