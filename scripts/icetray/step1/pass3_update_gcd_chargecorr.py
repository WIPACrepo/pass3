"""Update GCD for pass3 step 1 processing, including

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
# Input GCD file audit, look for DOMs with nan error
icetray.logging.rotating_files(infile_audit)
rc = run_gcd_audit_pass3(infile, nan_error = True, not1_error = False)

# Get the list of doms that have an error message from GCD audit for invalid mean charge correction value.
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
        geo = frame["I3Geometry"]
        geo_o = geo.omgeo
        for key, item in cal_o.items():
            # Consider only InIce DOMs
            if geo.omgeo[key].omtype == dataclasses.I3OMGeo.IceCube:
                old_atdw_cal.append(item.mean_atwd_charge)
                # Change nan value for dom with audit error
                if key in atwd or not math.isnan(item.mean_atwd_charge):
                    item.mean_atwd_charge = 1.0
                old_fadc_cal.append(item.mean_fadc_charge)
                # Change nan value for dom with audit error
                if key in fadc or not math.isnan(item.mean_fadc_charge):
                    item.mean_fadc_charge = 1.0
                cal_o[key] = item
        calitem.dom_cal = cal_o  # type: ignore[attr-defined]
        frame.Delete("I3Calibration")
        frame["I3Calibration"] = calitem

    elif frame.Stop == icetray.I3Frame.DetectorStatus:    
        bdl = "BadDomsListSLC"
        if bdl in frame:
            icetray.logging.log_info(f"len({bdl}) {len(frame[bdl])} {bdl} {frame[bdl]}")

    gcdfile_out.push(frame)

gcdfile_in.close()
gcdfile_out.close()

# Output GCD file audit, also look for DOMs with charge correction not = 1
icetray.logging.rotating_files(outfile_audit)
rc = run_gcd_audit_pass3(outfile, nan_error = True, not1_error = True)
if not rc == 0:
    icetray.logging.log_error(f"Unexpected error {rc} running GCD audit for file {outfile}")
    sys.exit(2)
out_atwd, out_fadc = get_nan_doms(outfile_audit)
icetray.logging.log_info(f"len(out_atwd) {len(out_atwd)} out_atwd {out_atwd}")
icetray.logging.log_info(f"len(out_fadc) {len(out_fadc)} out_fadc {out_fadc}")
if len(out_atwd) or len(out_fadc):
    icetray.logging.log_error("Expected no error messages in GCD audit for DOMs with invalid charge correction")
    sys.exit(1)
