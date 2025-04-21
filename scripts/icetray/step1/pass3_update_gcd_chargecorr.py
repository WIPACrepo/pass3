"""Update GCD for pass3 processing, including

* Run audit for pass3 on input GCD file, written to file <ingcd_audit>
* Parse warning messages in audit to get DOMs with nan value for ATWD or FADC charge corrections.
  These DOMs had no other audit error, so the correction will be set to 1 in output GCD file. 
* Set SPE correction factors to 1.0 for ATWD and FADC charge corrections
-- set domcal.mean_atwd_charge = 1.0
-- set domcal.mean_fadc_charge = 1.0
* Run audit for pass3 on output GCD file, written to file <outgcd_audit>

Usage:  python pass3_update_gcd_chargecorr.py <ingcd> <outgcd> <ingcd_audit> <outgcd_audit>
"""

import math
import sys
from icecube import icetray, dataclasses, dataio #NOQA: F401
from icecube.offline_filterscripts.gcd_generation import get_nan_doms, run_gcd_audit_pass3

icetray.logging.console()

if len(sys.argv) !=5:
    icetray.logging.log_error(f"{sys.argv[0]} <ingcd> <outgcd> <ingcd_audit> <outgcd_audit>")
    sys.exit(0)

infile = sys.argv[1]
outfile = sys.argv[2]
infile_audit = sys.argv[3]
outfile_audit = sys.argv[4]

icetray.logging.log_warn(f"Fixing GCD input file for Pass3: {infile}")
icetray.logging.log_warn(f"Writing GCD: {outfile} ")

icetray.logging.set_level("INFO")
# Input GCD file audit
icetray.logging.rotating_files(infile_audit)
run_gcd_audit_pass3(infile)

# Get the list of doms that have a warning message from GCD audit for invalid mean charge correction value.
# Such a dom therefore has no other audit error so the nan came from a failed fit.
atwd, fadc = get_nan_doms(infile_audit)
icetray.logging.console()
icetray.logging.log_info(f"len(atwd) {len(atwd)} atwd {atwd}")
icetray.logging.log_info(f"len(fadc) {len(fadc)} fadc {fadc}")

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
            # Keep nan value for dom with audit warning
            if key in atwd or not math.isnan(item.mean_atwd_charge):
                item.mean_atwd_charge = 1.0
            old_fadc_cal.append(item.mean_fadc_charge)
            # Keep nan value for dom with audit warning
            if key in fadc or not math.isnan(item.mean_fadc_charge):
                item.mean_fadc_charge = 1.0
            cal_o[key] = item
        calitem.dom_cal = cal_o  # type: ignore[attr-defined]
        frame.Delete("I3Calibration")
        frame["I3Calibration"] = calitem
    gcdfile_out.push(frame)

gcdfile_in.close()
gcdfile_out.close()

# Output GCD file audit
icetray.logging.rotating_files(outfile_audit)
run_gcd_audit_pass3(outfile)
out_atwd, out_fadc = get_nan_doms(outfile_audit)
icetray.logging.log_info(f"len(out_atwd) {len(out_atwd)} out_atwd {out_atwd}")
icetray.logging.log_info(f"len(out_fadc) {len(out_fadc)} out_fadc {out_fadc}")
if len(out_atwd) or len(out_fadc):
    icetray.logging.log_error(f"Expected no warning messages in GCD audit for DOMs with invalid charge correction")
    sys.exit(1)
