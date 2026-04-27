"""Utility to check GCD files for known changes for Pass3."""
from icecube import icetray
from icecube.icetray import I3ConditionalModule
from icecube.icetray import I3Tray
<<<<<<< HEAD
from icecube import dataioi, dataclasses
=======
from icecube import dataio, dataclasses
>>>>>>> 2bdec7b1cb148f58ab569f074b4dddfd59de53bc
import math
import json
import argparse

icetray.set_log_level_for_unit('I3Tray', icetray.I3LogLevel.LOG_TRACE)

class CheckPass3GCDI3Module(I3ConditionalModule):
    """Sanity checker for GCD file used in Pass 3"""
    def __init__(self, context):
        I3ConditionalModule.__init__(self, context)
        self.AddParameter("fadc_gain_correction_json", "file path to the json file with the fadc gain correction factor", None)
        self.AddParameter("old_fadc_gain_key", "key in GCD file for the old FADC gain. should be a map of OMKey to double", "Original_FADC_Gain" )
        self.geo = None

    def Configure(self):
        fadc_corr_json = self.GetParameter("fadc_gain_correction_json")
        self.fadc_gain_key = self.GetParameter("old_fadc_gain_key")
        with open(fadc_corr_json, "r") as f:
            self.fadc_corrs = json.load(f)["FADC_gain_correction"]

    def Geometry(self, frame):
        self.PushFrame(frame)

    def DetectorStatus(self, frame):
        self.PushFrame(frame)

    def Calibration(self, frame):
        cal = frame["I3Calibration"]
        geo = frame["I3Geometry"]
        if self.fadc_gain_key not in frame:
            raise Exception(f"Old FADC gain key {self.fadc_gain_key} not found in frame")
        old_fadc_gains = frame[self.fadc_gain_key]
        # Checking
        cal_o = cal.dom_cal 
        for key, item in cal_o.items():
            if not (key in geo.omgeo and geo.omgeo[key].omtype == dataclasses.I3OMGeo.IceCube):
                # only checking items in the geometry and in-ice; igonoring IceTop and other non-in-ice pieces
                continue
            if item.mean_atwd_charge_correction != 1.0 and not math.isnan(item.mean_atwd_charge_correction):
                raise ValueError(f"mean ATWD charge is not for DOM {key}. Set to {item.mean_atwd_charge_correction}")
            if item.mean_fadc_charge_correction != 1.0 and not math.isnan(item.mean_fadc_charge_correction):
                raise ValueError(f"mean FADC charge is not 1 for DOM {key}. set to {item.mean_fadc_charge_correction}")
            if math.isnan(item.relative_dom_eff):
                raise ValueError(f"relative DOM efficiency is nan for DOM {key}")
            if key in old_fadc_gains:
                corr_applied = item.fadc_gain - (
                    old_fadc_gains[key]/(self.fadc_corrs[
                        f"{key.string},{key.om}"]))
                if  corr_applied != 0 and not math.isnan(corr_applied):
                    error_number = old_fadc_gains[key]/(self.fadc_corrs[
                        f"{key.string},{key.om}"])
                    raise ValueError(f"FADC gain correction incorrect applied. OLD FADC gain value without {old_fadc_gains[key]} and with  {error_number} correction and CORRECTED FADC gain value {item.fadc_gain}. Delta between values {corr_applied}" )
        self.PushFrame(frame)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-g","--gcd",
                        dest="GCD",
                        required=True,
                        help="GCD File to be used for unpacking")
    parser.add_argument("--corrections",
                        dest="corr",
                        required=True,
                        help="FADC gain correction JSON")
    args = parser.parse_args()

    tray = I3Tray()

    # icetray.set_log_level_for_unit('I3Tray', icetray.I3LogLevel.LOG_TRACE)

    tray.Add(dataio.I3Reader, "reader", FilenameList=[args.GCD])

    tray.Add(CheckPass3GCDI3Module, "gcd_checker",
             fadc_gain_correction_json=args.corr,
             old_fadc_gain_key="Original_FADC_Gain")

    tray.Execute()
