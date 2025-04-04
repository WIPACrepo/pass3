#!/usr/bin/env python3
"""Check InIce DOMs that have values different than 1 for mean ATWD or FADC charge in GCD file."""
from argparse import ArgumentParser

from icecube import dataio, icetray

from icecube.icetray import I3Tray
from icecube.icetray import I3ConditionalModule

# handling of command line arguments
parser = ArgumentParser(
    prog="pass3_check_gcd",
    description="Stand alone script to check GCD file for pass3")
parser.add_argument("-i", "--input", action="store", default=None,
                    dest="INPUT", help="Input i3 file to process",
                    required=True)
args = parser.parse_args()

# from online_filterscripts/python/base_segments/onlinecalibration.py#L15-L22
from icecube.icetray import OMKey
online_bad_doms = [OMKey(1, 46),     # Dovhjort
            OMKey(7, 34),     # Grover
            OMKey(7, 44),     # Ear_Muffs
            OMKey(22, 49),    # Les_Arcs
            OMKey(38, 59),    # Blackberry
            OMKey(60, 55),    # Schango
            OMKey(68, 42),     # Krabba
]

class CheckPass3GCDI3Module(I3ConditionalModule):
    """Sanity checker for GCD file used in Pass 3"""
    def __init__(self, context):
        I3ConditionalModule.__init__(self, context)

    def Configure(self):
        pass

    def Geometry(self, frame):
        pass

    def DetectorStatus(self, frame):        
        #pass
        #for omkey in frame["BadDomsList"]:
            #print(omkey, "BadDomsList")

        if "BadDomsList" not in frame:
            print("missing BadDomsList")

        if "BadDomsListSLC" not in frame:
            print("missing BadDomsListSLC")

        if "GRLSnapshotId" not in frame:
            print("missing GRLSnapshotId")

        if "GoodRunEndTime" not in frame:
            print("missing GoodRunEndTime")

        if "GoodRunStartTime" not in frame:
            print("missing GoodRunStartTime")

        if "IceTopBadDOMs" not in frame:
            print("missing IceTopBadDOMs")

        if "IceTopBadTanks" not in frame:
            print("missing IceTopBadTanks")

        if "IceTop_ATWDCrossoverMap" not in frame:
            print("missing IceTop_ATWDCrossoverMap")

        if "OfflineProductionVersion" not in frame:
            print("missing OfflineProductionVersion")




        cal = frame["I3Calibration"]
        # Checking
        cal_o = cal.dom_cal 
        for key, item in cal_o.items():
            # Check only InIce DOMs
            if key[0] > 0 and key[1] < 61:
                if item.mean_atwd_charge != 1.0:
                    #raise ValueError(key, "mean ATWD charge is not 1", item.mean_atwd_charge)
                    #print(key, "mean ATWD charge is not 1", item.mean_atwd_charge)
                    if "BadDomsList" not in frame:
                        print(key, "no BadDomsList and mean ATWD charge is not 1", item.mean_atwd_charge)
                    elif key not in frame["BadDomsList"]:
                        print(key, "is not in BadDomsList and mean ATWD charge is not 1", item.mean_atwd_charge)
                    if "BadDomsListSLC" not in frame:
                        print(key, "no BadDomsListSLC and mean ATWD charge is not 1", item.mean_atwd_charge)
                    elif key not in frame["BadDomsListSLC"]:
                        print(key, "is not in BadDomsListSLC and mean ATWD charge is not 1", item.mean_atwd_charge)
                    if key not in online_bad_doms:
                        print(key, "is not in online_bad_doms and mean ATWD charge is not 1", item.mean_atwd_charge)
                if item.mean_fadc_charge != 1.0:
                    #raise ValueError(key, "mean FADC charge is not 1", item.mean_fadc_charge)
                    #print(key, "mean FADC charge is not 1", item.mean_fadc_charge)
                    if "BadDomsList" not in frame:
                        print(key, "no BadDomsList and mean FADC charge is not 1", item.mean_fadc_charge)
                    elif key not in frame["BadDomsList"]:
                        print(key, "is not in BadDomsList and mean FADC charge is not 1", item.mean_fadc_charge)
                    if "BadDomsListSLC" not in frame:
                        print(key, "no BadDomsListSLC and mean FADC charge is not 1", item.mean_fadc_charge)
                    elif key not in frame["BadDomsListSLC"]:
                        print(key, "is not in BadDomsListSLC and mean FADC charge is not 1", item.mean_fadc_charge)
                    if key not in online_bad_doms:
                        print(key, "is not in online_bad_doms and mean FADC charge is not 1", item.mean_fadc_charge)
    def Calibration(self, frame):
        #pass

        if "I3IceTopSLCCalibrationCollection" not in frame:
            print("missing I3IceTopSLCCalibrationCollection")        


tray = I3Tray()
tray.Add(dataio.I3Reader, "reader", filenamelist=[args.INPUT])
tray.Add(CheckPass3GCDI3Module)

tray.Execute()
