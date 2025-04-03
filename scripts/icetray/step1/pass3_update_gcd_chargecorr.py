"""Update GCD for pass3 processing, including

* set SPE correction factors to 1.0 for ATWD and FADC charge corrections
-- set domcal.mean_atwd_charge = 1.0
-- set domcal.mean_fadc_charge = 1.0

Usage:  python pass3_update_gcd_chargecorr.py <ingcd> <outgcd>
"""

import math
import sys
from icecube import icetray, dataclasses, dataio #NOQA: F401

icetray.logging.console()

if len(sys.argv) !=3:
    icetray.logging.log_error("pass3_update_gcd_chargecorr.py <ingcd> <outgcd>")
    sys.exit(0)

infile = sys.argv[1]
outfile = sys.argv[2]

icetray.logging.log_warn(f"Fixing GCD input file for Pass3: {infile}")
icetray.logging.log_warn(f"Writing GCD: {outfile} ")


gcdfile_in = dataio.I3File(infile)

gcdfile_out = dataio.I3File(outfile, "w")

old_atdw_cal = []
old_fadc_cal = []

while gcdfile_in.more():
    frame = gcdfile_in.pop_frame()
    if frame.Stop == icetray.I3Frame.Calibration:  # Got the calibration
        calitem = frame["I3Calibration"]
        cal_o = calitem.dom_cal  # type: ignore[attr-defined]
        for key, item in cal_o.items():
            old_atdw_cal.append(item.mean_atwd_charge)
            if not math.isnan(item.mean_atwd_charge):
                item.mean_atwd_charge = 1.0
            old_fadc_cal.append(item.mean_fadc_charge)
            if not math.isnan(item.mean_fadc_charge):
                item.mean_fadc_charge = 1.0
            cal_o[key] = item
        calitem.dom_cal = cal_o  # type: ignore[attr-defined]
        frame.Delete("I3Calibration")
        frame["I3Calibration"] = calitem
    gcdfile_out.push(frame)

gcdfile_in.close()
gcdfile_out.close()
