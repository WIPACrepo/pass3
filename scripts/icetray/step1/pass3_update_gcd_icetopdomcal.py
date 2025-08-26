"""Update GCD for pass3 step 1 processing:

* Replace bad IceTop DOMCals with good IceTop DOMCals from a nearby run.
  This is something necessary for certain runs in IC86.2011

Usage:  python pass3_update_gcd_icetopdomcal.py <ingcd> <stealfromgcd> <outgcd>

HOW TO TEST IT:
----------------
python pass3_update_gcd_icetopdomcal.py /data/exp/IceCube/2011/filtered/level2pass2a/1227/Run00119189/Level2pass2_IC86.2011_data_Run00119189_1227_1_20_GCD.i3.zst /data/exp/IceCube/2011/filtered/level2pass2a/0513/Run00118175/Level2pass2_IC86.2011_data_Run00118175_0513_1_20_GCD.i3.zst testme_119183_GCD.i3.bz2 1-61 1-62 1-63 1-64

"""

#import math
import sys
from icecube import icetray, dataclasses, dataio #NOQA: F401
from icecube.offline_filterscripts.gcd_generation import get_nan_doms, run_gcd_audit_pass3


## --------------------------------------------
## ------ THE MODULE: -------------------------
## --------------------------------------------

class ReplaceIceTopDOMCals_fromOtherGCD(icetray.I3Module):
    """
    Replace the IceTop DOMCal's for a particular DOM with the values from a different GCD file.
    """

    def __init__(self, ctx):
        super().__init__(ctx)
        self.AddParameter('StealFromFile', 'GCD file containing good IceTop DOMCals to steal from', ""),
        self.AddParameter('OMKeyList', 'List of OMKeys to steal (empty = all of them)', [])
        self.AddOutBox('OutBox')

    def Configure(self):
        self.stealfile = self.GetParameter('StealFromFile')
        self.omkeylist = self.GetParameter('OMKeyList')
        if len(self.omkeylist) == 0:
            ## If the list is empty, it means we'll do the entire array
            for s in range(1,82):
                for om in range(61,65):
                    if (s != 39) or (om != 61):  # exclude the dead one
                        self.omkeylist.append(icetray.OMKey(s, om))

    # All the action takes place in the C-frame!
    def Calibration(self, frame):
        # The original object
        calitem = frame['I3Calibration']

        # Grab the geometry too, for error-checking
        geo = frame["I3Geometry"]
        geo_o = geo.omgeo

        # Find the OTHER C-frame to steal from 
        i3f = dataio.I3File(self.stealfile)
        otherCframe = None
        while (otherCframe == None):
            f = i3f.pop_frame()
            if f.Stop == icetray.I3Frame.Calibration:
                otherCframe = f
        othercal = otherCframe["I3Calibration"].dom_cal

        # Go find each DOM to steal
        for k in self.omkeylist:
            # Make sure it's an IceTop DOM
            if k not in geo.omgeo:
                raise Exception("Hey, this DOM isn't in the Geometry!", k)
            if geo.omgeo[k].omtype != dataclasses.I3OMGeo.IceTop:
                raise Exception("Hey, this DOM is not an IceTop DOM!", k)

            # Fetch it
            cdom = othercal[k]
            
            # Copy it
            icetray.i3logging.log_info(f"Copying DOMCal for DOM: {k}")
            calitem.dom_cal[k] = cdom


        # Replace the original with the new one in the
        frame.Delete("I3Calibration")
        frame["I3Calibration"] = calitem
        self.PushFrame(frame)


## --------------------------------------------
## ------ SET INPUT AND OUTPUT: ---------------
## --------------------------------------------
## For testing
'''
infile = "/data/exp/IceCube/2011/filtered/level2pass2a/1227/Run00119189/Level2pass2_IC86.2011_data_Run00119189_1227_1_20_GCD.i3.zst" 
stealfile = "/data/exp/IceCube/2011/filtered/level2pass2a/0513/Run00118175/Level2pass2_IC86.2011_data_Run00118175_0513_1_20_GCD.i3.zst"
outfile = "testme_119183_GCD.i3.bz2"
#domliststrs = ["1-61", "1-62", "1-63", "1-64"]  # some of them
domliststrs = []  # all of them

'''

## User arguments: 
## -- Source GCD file
## -- GCD file to steal from
## -- Output GCD
## -- (optional) List of OMKeys (default = all of them in IceTop) such as "7-61"
if len(sys.argv) < 4:
    icetray.logging.log_error(f"{sys.argv[0]} <ingcd> <stealgcd> <outgcd> [optional: particular DOMs, such as '1-61']")
    sys.exit(0)

infile = sys.argv[1]
stealfile = sys.argv[2]
outfile = sys.argv[3]
domliststrs = sys.argv[4:]  # the rest of them, as strings

# Parse the list of DOM's input by the user as strings -> icetray OMKeys
domlist = [icetray.OMKey(int(x.split("-")[0]),int(x.split("-")[1])) for x in domliststrs]


## --------------------------------------------
## ------ EXECUTE IT: -------------------------
## --------------------------------------------
## Try actually running it!
## ===========================

icetray.logging.log_warn(f"Fixing GCD input file for Pass3: {infile}")
icetray.logging.log_warn(f"Stealing from GCD: {stealfile} ")
icetray.logging.log_warn(f"Writing GCD: {outfile} ")

icetray.logging.set_level("INFO")


# Set up the Tray.
tray = icetray.I3Tray()

# Read input
icetray.i3logging.log_info("Reading from: %s"%infile)
tray.AddModule("I3Reader", "readme", Filename=infile)

#tray.AddModule("Dump", "dumpme")   # for testing!

tray.Add(ReplaceIceTopDOMCals_fromOtherGCD, "replacetest",
         StealFromFile = stealfile,
         OMKeyList = domlist)
 
# Write output
icetray.i3logging.log_info("Writing to: %s"%outfile)
streams = [icetray.I3Frame.TrayInfo, 
           icetray.I3Frame.Geometry, 
           icetray.I3Frame.Calibration,
           icetray.I3Frame.DetectorStatus]
tray.Add("I3Writer", filename=outfile, Streams=streams)

# Execute!
tray.Execute()

