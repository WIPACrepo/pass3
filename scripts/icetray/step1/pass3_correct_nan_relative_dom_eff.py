#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py3-v4.4.2/icetray-start
#METAPROJECT: icetray/v1.17.0
"""Script to check GCD files for nan relative DOM effiency and correct it if necessary."""
from icecube import icetray
from icecube.icetray import I3ConditionalModule
from icecube.icetray import I3Tray
from icecube import dataio
import math
import argparse
from typing import Union, Optional, Set
from pathlib import Path
import hashlib

from pass3_check_gcd import CheckPass3GCDI3Module

icetray.set_log_level_for_unit('I3Tray', icetray.I3LogLevel.LOG_TRACE)

class CorrectPass3RelDOMeffGCDI3Module(I3ConditionalModule):
    """Sanity checker for GCD file used in Pass 3"""
    def __init__(self, context):
        I3ConditionalModule.__init__(self, context)

    def Configure(self):
        pass

    def Geometry(self, frame):
        self.PushFrame(frame)

    def DetectorStatus(self, frame):
        self.PushFrame(frame)

    def Calibration(self, frame):
        calitem = frame["I3Calibration"]
        cal_o = calitem.dom_cal 
        nan_doms = []
        # Checking
        for key, item in cal_o.items():
            if math.isnan(item.relative_dom_eff):
                print(f"DOM {key} has nan relative DOM efficiency")
                nan_doms.append(key)
                item.relative_dom_eff = 1.0
        if len(nan_doms) > 0:
            calitem.dom_cal = cal_o # type: ignore[attr-defined]
            frame.Delete("I3Calibration")
            frame["I3Calibration"] = calitem
        self.PushFrame(frame)

# Taken from LTA
# Adapted from: https://stackoverflow.com/a/44873382
def get_sha512sum(filename: Union[str, Path]) -> str:
    """Compute the SHA512 hash of the data in the specified file."""
    print(f"Getting sha512sum for {filename}")
    h = hashlib.sha512()
    b = bytearray(8192 * 1024)
    mv = memoryview(b)
    with open(str(filename), 'rb', buffering=0) as f:
        for n in iter(lambda: f.readinto(mv), 0):
            h.update(mv[:n])
    return h.hexdigest()

def compare_files(file1: Union[str, Path], file2: Union[str, Path]) -> bool:
    """Compare the contents of two files by computing their SHA512 hashes."""
    return get_sha512sum(file1) == get_sha512sum(file2)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingcd",
                        dest="inGCD",
                        required=True,
                        help="GCD File to be used for unpacking")
    parser.add_argument("--outgcd",
                        dest="outGCD",
                        required=True,
                        help="GCD File to be used for unpacking")
    parser.add_argument("--corrections",
                        dest="corr",
                        required=True,
                        help="FADC gain correction JSON")
    args = parser.parse_args()

    tray = I3Tray()

    # icetray.set_log_level_for_unit('I3Tray', icetray.I3LogLevel.LOG_TRACE)

    tray.Add(dataio.I3Reader, "reader", Filename=args.inGCD)

    tray.Add("Dump")

    tray.Add(CorrectPass3RelDOMeffGCDI3Module, 
            "dom_rel_eff_nan_checker")
    
    tray.Add(CheckPass3GCDI3Module, "gcd_checker",
             fadc_gain_correction_json=args.corr,
             old_fadc_gain_key="Original_FADC_Gain")

    tray.Add("I3Writer", "writer", Filename=args.outGCD)

    tray.Execute()

    if compare_files(args.inGCD, args.outGCD):
        print(f"Files {args.inGCD} and {args.outGCD} are identical.")
    else:
        print(f"Files {args.inGCD} and {args.outGCD} differ.")
