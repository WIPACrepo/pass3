"""Utility to calculate filter rates from filtered output files."""
from icecube.icetray import I3ConditionalModule


class CheckPass3GCDI3Module(I3ConditionalModule):
    """Sanity checker for GCD file used in Pass 3"""
    def __init__(self, context):
        I3ConditionalModule.__init__(self, context)

    def Configure(self):
        pass

    def Geometry(self):
        pass

    def DetectorStatus(self):
        pass

    def Calibration(self):
        cal = frame["I3Calibration"]
        # Checking
        cal_o = cal.dom_cal 
        for key, item in cal_o.items():
            if item.mean_atwd_charge != 1.0:
                raise ValueError("mean ATWD charge is not 1")
            if item.mean_fadc_charge != 1.0:
                raise ValueError("mean FADC charge is not 1")