# SPDX-FileCopyrightText: 2025 The IceTray Contributors
#
# SPDX-License-Identifier: BSD-2-Clause

"""Support functions and tray segements for subtracting extra leap second in pass3step2 GCD files"""

'''
HOW TO RUN IT:
python leapfix_gcdfile.py -i /data/ana/IceCube/2017/filtered/OfflinePass3.0/0301/Run00129239/OfflinePass3_IC86.2016_data_Run00129239_0301_90_299_GCD.i3.zst -o test_GCD.i3.zst
'''


import copy
import re
import argparse
from icecube.icetray import I3Tray
from icecube.icetray import I3Units
from icecube import icetray, dataclasses, dataio  #noqa: F401
from icecube.icetray import logging

## ------------------- HELPER FUNCTIONS --------------------------------
## For parsing filenames to get the Run Number. 
## This is stolen from GITHUB/wg-cosmic-rays/IceTop_Level4/filename_utils.py
def get_run_from_filename(input_file):
    result = None
    m = re.search("Run([0-9]+)", input_file)
    if not m:
        raise ValueError("cannot parse %s for Run number" % input_file)
    return int(m.group(1))

def subtract_leap_second(frame, key=None):
    grt = frame[key]
    old_grt = frame[key]
    grt -= I3Units.second
    del frame[key]
    frame[key] = grt
    frame["Old" + key] = old_grt
    return True

## ------------------- THE TRAY --------------------------------
def runme(infile, outfile, *, testmode=False, **kwargs):
    """Procedure to subtract 1 second from GoodRunStartTime and GoodRunEndTime in a GCD file."""

    # Pipe the log stuff somewhere?
    if kwargs["logfile"]:
        logging.rotating_files(kwargs["logfile"])

    # Set log level
    logging.set_level("INFO")

    logging.log_info("Reading from: %s"%infile)
    # Did we give it a run number?  If not, figure it out from the input filename
    if not kwargs["run_id"]:
        run_id = get_run_from_filename(infile)
    else:
        run_id = kwargs["run_id"]
        
    run_id_min = 129004 # 2017-01-01 first run of calendar yeqar
    run_id_max = 129519 # 2017-05-18 last run of IC86-2016
    if (run_id < run_id_min or run_id > run_id_max):
        raise Exception(f"This run number {run_id} is outside the range {run_id_min} to {run_id_max}")
        
    # Set up the Tray.
    tray = I3Tray()

    tray.AddModule("I3Reader", "readme", Filename=infile)

    tray.Add(subtract_leap_second,
             key="GoodRunStartTime",
             Streams=[icetray.I3Frame.DetectorStatus]
             )

    tray.Add(subtract_leap_second,
             key="GoodRunEndTime",
             Streams=[icetray.I3Frame.DetectorStatus]
             )
    
    # Write output
    logging.log_info("Writing to: %s"%outfile)
    streams = [icetray.I3Frame.Geometry, icetray.I3Frame.Calibration,
               icetray.I3Frame.TrayInfo,
               icetray.I3Frame.DetectorStatus]
    icetray.logging.set_level_for_unit("I3Module", "WARN") # Prevents printing usage stats.
    tray.Add("I3Writer", filename=outfile, Streams=streams)

    tray.Execute()

    tray.Finish()
    del tray
    

## ------------------- THE EXECUTABLE ----------------------
def main():
    """Run setup arguments and run main function"""
    description = """Update a GCD file for pass3."""

    # Parse the arguments
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("-i", "--infile", type=str, required=True,
                        help="Input GCD i3 filename")
    parser.add_argument("-o", "--outfile", type=str, required=True,
                        help="Output GCD i3 filename")
    parser.add_argument("-r", "--run", type=int, dest = "run_id", required=False,
                        help="Run Number")
    parser.add_argument("--log-file", dest = "logfile",
                        type=str, help = "File where logs will be written")
    args = parser.parse_args()

    # Run it!
    runme(**vars(args))

if __name__ == "__main__":
    main()
