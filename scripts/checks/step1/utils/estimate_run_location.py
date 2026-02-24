#!/usr/bin/env python3
"""
Utility to estimate run location (date) from run number using allruns.csv.
"""

import csv
from datetime import datetime
from pathlib import Path
from typing import Optional

class RunLocationEstimator:
    def __init__(self, csv_path: Optional[Path] = None):
        if csv_path is None:
            # Default: ../../../../data/allruns.csv relative to this file
            # Current file is scripts/checks/step1/utils/estimate_run_location.py
            self.csv_path = Path(__file__).resolve().parent.parent.parent.parent.parent / "data" / "allruns.csv"
        else:
            self.csv_path = Path(csv_path)

    def get_run_date(self, run_number: int) -> Optional[datetime]:
        """
        Get the start date for a given run number.
        Returns datetime object (with time 00:00:00) or None if run not found.
        """
        if not self.csv_path.exists():
            return None
            
        try:
            with open(self.csv_path, 'r', encoding='utf-8', errors='replace') as f:
                reader = csv.DictReader(f)
                
                for row in reader:
                    try:
                        # Clean up keys if necessary (sometimes BOM or whitespace)
                        # We iterate keys to find "Run number" and "Start time" if exact match fails?
                        # But standard DictReader should work if headers are clean.
                        
                        run_num_str = row.get("Run number")
                        if not run_num_str:
                            continue
                            
                        if int(run_num_str) == run_number:
                            start_time_str = row.get("Start time")
                            if start_time_str:
                                # Format is typically "YYYY-MM-DD HH:MM:SS..."
                                # We only need the date part usually.
                                try:
                                    date_str = start_time_str.split(' ')[0]
                                    return datetime.strptime(date_str, "%Y-%m-%d")
                                except ValueError:
                                    return None
                            return None
                    except (ValueError, IndexError):
                        continue
                        
        except Exception as e:
            print(f"Error reading allruns.csv: {e}")
            return None
        
        return None

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        run = int(sys.argv[1])
        est = RunLocationEstimator()
        date = est.get_run_date(run)
        if date:
            print(f"Run {run} was on {date.strftime('%Y-%m-%d')}")
        else:
            print(f"Run {run} not found or date unknown")
    else:
        print("Usage: python3 estimate_run_location.py <run_number>")
