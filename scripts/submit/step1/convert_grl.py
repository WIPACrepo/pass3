import csv
import argparse

from pathlib import Path

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--infile",
                        help="input csv file from i3live",
                        type=Path,
                        required=True)
    parser.add_argument("--outfile",
                        help="output file",
                        type=Path,
                        required=True)
    args=parser.parse_args()

    with open(args.infile, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=",")
        runs = [int(row[0]) for row in reader if row[-3] == "GOOD"]
        
    with open(args.outfile, "w") as f:
        for r in runs:
            f.write(f"{r}\n")
