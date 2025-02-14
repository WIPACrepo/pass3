import argparse
import subprocess
import concurrent.futures
import hashlib
import time
import datetime
import sys
import os

from pathlib import Path, PosixPath





def stampede3_slurm_header(file: str, 
                           queue: str,
                           jobname: str):
    if queue not in ["skx", "spr", "icx"]:
        raise Exception
    file.write(f"#SBATCH -p {queue}\n")
    file.write(f"#SBATCH -J {jobname}\n")
    file.write(f"#SBATCH -N 1\n")


def get_day_range(year: int, group: int) -> list[str]:
    day_range = 73
    all_days = [(datetime.date(args.year, 1, 1)
            + datetime.timedelta(days=x)).strftime('%m%d') 
    for x in range((datetime.date(args.year, 12, 31) 
                    - datetime.date(args.year, 1, 1)).days + 1)]
    
    day_groups = []

    # Break up year into 73 day chunks (5 groups)
    for i in range(0, len(all_days), day_range):
        day_groups.append(all_days[i:i + day_range])

    days_to_process = day_groups[args.group - 1]

    return days_to_process

def get_ranch_dirs(year: int, group: int) -> list[Path]:
    year_dir_ranch = Path(
        f"/stornext/ranch_01/ranch/projects/TG-PHY150040/data/exp/IceCube/{args.year}/unbiased/PFRaw/")

    days = get_day_range(args.year, args.group)

    ranch_dirs = [ year_dir_ranch / day for day in days ]

    return ranch_dirs

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", 
                        help="Year to be processed", 
                        type=int,
                        required=True)
    parser.add_argument("--group",
                        help="Given total number of slots (76), we will split a year into 5 groups of dates.",
                        type=int,
                        choices=[1, 2, 3, 4, 5],
                        required=True)
    args=parser.parse_args()

    max_jobs = {
        "skx": 40,
        "spr": 24,
        "icx": 12,
    }

