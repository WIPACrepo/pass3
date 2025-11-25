"""Update GCD for pass3 step 1 processing, including

* Run audit for pass3 on input GCD file, written to file <inaudit>
* Parse error messages in audit to get DOMs with nan value for ATWD or FADC charge correction.
  These DOMs had no other audit error, so the correction will be set to 1 in output GCD file. 
* Set SPE correction factors to 1.0 for ATWD and FADC charge corrections
-- set domcal.mean_atwd_charge_correction = 1.0
-- set domcal.mean_fadc_charge_correction = 1.0
* Run audit for pass3 on output GCD file, written to file <outgcd_audit>

"""

import math
import sys
import argparse
import json
from icecube import icetray, dataclasses, dataio #NOQA: F401
from icecube.offline_filterscripts.gcd_generation import get_nan_doms, run_gcd_audit_pass3


icetray.logging.console()

icetray.logging.set_level("INFO")

# Get the list of doms that have an error message from GCD audit for invalid mean charge correction value.
# Such a dom therefore has no other audit error so the nan came from a failed fit.
# atwd, fadc = get_nan_doms(infile_audit)
# icetray.logging.console()
# icetray.logging.log_info(f"len(atwd) {len(atwd)} atwd {atwd}")
# icetray.logging.log_info(f"len(fadc) {len(fadc)} fadc {fadc}")


def correct_gcd_file(infile: str,
                     outfile: str,
                     fadc_corrections: dict,
                     fadc_db: dict,
                     mean_atwd_charge = 1.,
                     mean_fadc_charge = 1.):

    gcdfile_in = dataio.I3File(infile, "r")

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
                if key in geo.omgeo and geo.omgeo[key].omtype == dataclasses.I3OMGeo.IceCube:
                    old_atdw_cal.append(item.mean_atwd_charge_correction)
                    # Change nan value for dom with audit error
                    if not math.isnan(item.mean_atwd_charge_correction) and item.mean_atwd_charge_correction != mean_atwd_charge:
                        print(f"correct ATWD charge correction for DOM {key}")
                        item.mean_atwd_charge_correction = mean_atwd_charge 
                    old_fadc_cal.append(item.mean_fadc_charge_correction)
                    # Change nan value for dom with audit error
                    if not math.isnan(item.mean_fadc_charge_correction) and item.mean_fadc_charge_correction != mean_fadc_charge:
                        print(f"correct FADC charge correction for DOM {key}")
                        item.mean_fadc_charge_correction = mean_fadc_charge 
                    if math.isnan(item.relative_dom_eff):
                        print(f"correct relative DOM efficiency for DOM {key}")
                        item.relative_dom_eff = 1.0
                    if ("Original_FADC_Gain" not in frame) or (key not in frame["Original_FADC_Gain"].keys()):
                        key_str = f"{key.string},{key.om}"
                        print(f"FADC gain file  {item.fadc_gain} for DOM {key}")
                        print(f"FADC gain server {fadc_db[key_str]} for DOM {key}")
                        print(f"{(item.fadc_gain/fadc_db[key_str] - 1.)*100. }")
                        if "Original_FADC_Gain" not in frame:
                            frame["Original_FADC_Gain"] = dataclasses.I3MapKeyDouble({key: item.fadc_gain})
                        else:
                            frame["Original_FADC_Gain"].update({key: item.fadc_gain})
                        item.fadc_gain = item.fadc_gain/(fadc_corrections['FADC_gain_correction'][f"{key.string},{key.om}"])
                        print(f"AFTER CORRECTION FADC gain {item.fadc_gain} for DOM {key}")
                    cal_o[key] = item
            calitem.dom_cal = cal_o  # type: ignore[attr-defined]
            frame.Delete("I3Calibration")
            frame["I3Calibration"] = calitem

        elif frame.Stop == icetray.I3Frame.DetectorStatus:    
            bdl = "BadDomsList"
            if bdl in frame:
                icetray.logging.log_info(f"len({bdl}) {len(frame[bdl])} {bdl} {frame[bdl]}")

        gcdfile_out.push(frame)

    gcdfile_in.close()
    gcdfile_out.close()

# Output GCD file audit, also look for DOMs with charge correction not = 1
# icetray.logging.rotating_files(outfile_audit)
# rc = run_gcd_audit_pass3(outfile, nan_error = True, not1_error = True)
# if not rc == 0:
#     icetray.logging.log_error(f"Unexpected error {rc} running GCD audit for file {outfile}")
#     sys.exit(2)
# out_atwd, out_fadc = get_nan_doms(outfile_audit)
# icetray.logging.log_info(f"len(out_atwd) {len(out_atwd)} out_atwd {out_atwd}")
# icetray.logging.log_info(f"len(out_fadc) {len(out_fadc)} out_fadc {out_fadc}")
# if len(out_atwd) or len(out_fadc):
#     icetray.logging.log_error("Expected no error messages in GCD audit for DOMs with invalid charge correction")
# sys.exit(1)

def parse_json(file: str) -> dict:
    with open(file, 'r') as f:
        data = json.load(f)
    return data

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='dump_muonitron_output')
    parser.add_argument("-i", "--infile", dest="infile", type=str, required=True)
    parser.add_argument("-o", "--outfile", dest="outfile", type=str, required=True)
    parser.add_argument("--inaudit", dest="inaudit", type=str, required=True)
    parser.add_argument("--outaudit", dest="outaudit", type=str, required=True)
    parser.add_argument("--fadc-correction", dest="fadc_corr", type=str, required=True)
    parser.add_argument("--fadc-gcddb", dest="fadc_gcddb", type=str, required=True)
    args = parser.parse_args()


    icetray.logging.log_warn(f"Fixing GCD input file for Pass3: {args.infile}")
    icetray.logging.log_warn(f"Writing GCD: {args.outfile}")
    icetray.logging.log_warn(f"Using Correction file: {args.fadc_corr}")
    icetray.logging.log_warn(f"Using database file: {args.fadc_gcddb}")

    fadc_corr = parse_json(args.fadc_corr)
    fadc_gcddb = parse_json(args.fadc_gcddb)

    icetray.logging.rotating_files(args.inaudit)
    # Input GCD file audit, look for DOMs with nan error
    rc = run_gcd_audit_pass3(args.infile, nan_error = True, not1_error = False)

    correct_gcd_file(args.infile, args.outfile, fadc_corr, fadc_gcddb)

    icetray.logging.rotating_files(args.outaudit)
    rc = run_gcd_audit_pass3(args.outfile, nan_error = True, not1_error = True)
