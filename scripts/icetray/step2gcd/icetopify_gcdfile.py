# SPDX-FileCopyrightText: 2025 The IceTray Contributors
#
# SPDX-License-Identifier: BSD-2-Clause

"""Support functions and tray segements for adding IceTop calibration objects to pass3step1 GCD files"""

'''
HOW TO RUN IT:
python icetopify_gcdfile.py -i /data/ana/IceCube/2022/filtered/OnlinePass3.6/GCD/OnlinePass3_IC86.2021_data_Run00136124_81_647_GCD.i3.zst -o test_GCD.i3 --log-file test_GCD.log
'''



import re
import argparse
from icecube.icetray import I3Tray, OMKey
from icecube import icetray, dataclasses, dataio  #noqa: F401
from icecube.icetray import logging
from icecube.offline_filterscripts.icetop_GCDmodification.overwrite_snowheights import ChangeSnowHeights_FromDB
from icecube.offline_filterscripts.icetop_GCDmodification.add_ATWDcrossovers_to_Dframe import Add_ATWDCrossoverMap_Dframe
from AddSLCCalibration_fromjson import AddSLCCalibrationCollection_fromjson

## ------------------- HELPER FUNCTIONS --------------------------------
## For parsing filenames to get the Run Number. 
## This is stolen from GITHUB/wg-cosmic-rays/IceTop_Level4/filename_utils.py
def get_run_from_filename(input_file):
    result = None
    m = re.search("Run([0-9]+)", input_file)
    if not m:
        raise ValueError("cannot parse %s for Run number" % input_file)
    return int(m.group(1))

## Create a ATWDCrossoverMap full of simple placeholder values
def placeholder_for_Dframe(frame, objectname="IceTop_ATWDCrossoverMap"):
    newobject = dataclasses.I3MapKeyVectorDouble()
    # Loop through all the things in DetectorStatus (dead dom's will not be in there!)
    ds = frame["I3DetectorStatus"].dom_status
    for k, v in ds.items():
        if (k.om >= 61) and (k.om <= 64):
            # We found an IceTop DOM
            #print(k, v.dom_gain_type)
            # Pick some "typical" values, depending on whether it's HG or LG
            if v.dom_gain_type == dataclasses.I3DOMStatus.DOMGain.High:
                newobject[k] = [85, 750]
            elif v.dom_gain_type == dataclasses.I3DOMStatus.DOMGain.Low:
                newobject[k] = [4800, 48000]
            else:
                raise Exception("IceTop DOM with unknown gain type %s"%v.dom_gain_type)
    # Put the new object in the frame
    frame[objectname] = newobject

## ------------------- THE TRAY --------------------------------
def runme(infile, outfile, *, testmode=True, **kwargs): #, *, skip_audit=False, production_version=-1, **kwargs):
    """Run procedure for adding IceTop calibration objects to a GCD file."""

    # Pipe the log stuff somewhere?
    if kwargs["logfile"]:
        logging.rotating_files(kwargs["logfile"])

    # Set log level
    logging.set_level("INFO")

    # Did we give it a run number?  If not, figure it out from the input filename
    if not kwargs["run_id"]:
        run_id = get_run_from_filename(infile)

    # Set up the Tray.
    tray = I3Tray()

    # Read input
    logging.log_info("Reading from: %s"%infile)
    tray.AddModule("I3Reader", "readme", Filename=infile)

    tray.AddModule("Dump", "dumpme")   # for testing!

    # Deal with SNOW
    tray.Add(ChangeSnowHeights_FromDB, "updateSnowHeights", Run = run_id)

    # Deal with VEMCal
    # <insert me when the time comes!!>

    # Deal with SLCCal
    ## This bit is temporary, for pass3step2 TESTING ONLY.  If Kath hasn't computed ITSLC calibration constants yet,
    ## then we'll create some fake ones, designed to be really obvious (if looked at) that they are fake.
    if testmode:
        ## Create the C-frame object, using a placeholder .json file containing rounded-off numbers
        if run_id >= 136442:  # Orc is dead
            placeholder_filename = "placeholder_IceTopSLCCal_deadOrc.json"
        else:  # Orc is alive
            placeholder_filename = "placeholder_IceTopSLCCal_liveOrc.json"
        logging.log_warn("I'm going to create a fake set of IceTop SLC constants, from %s"%placeholder_filename)
        tray.Add(AddSLCCalibrationCollection_fromjson, 'add_Cframe_placeholder',
             SLCCalibFile   = placeholder_filename,
             Provenance = dataclasses.ITSLCCalProvenance.Placeholder  # This will tell future people that it's fake  
         )
        ## Create the D-frame object, with more simple placeholder values, so that future people will recognize it as fake
        tray.Add(placeholder_for_Dframe, "add_Dframe_placeholder",
                 Streams=[icetray.I3Frame.DetectorStatus])
    else:
        ## TO-DO: Create the C-frame object for realsies!
        ## Create the D-frame object, for realsies.
        tray.Add(Add_ATWDCrossoverMap_Dframe, "makeCrossoverMap")

    # Write output
    logging.log_info("Writing to: %s"%outfile)
    streams = [icetray.I3Frame.Geometry, icetray.I3Frame.Calibration,
               icetray.I3Frame.TrayInfo,
               icetray.I3Frame.DetectorStatus]
    tray.Add("I3Writer", filename=outfile, Streams=streams)

    # Execute!
    tray.Execute()

    #tray.PrintUsage(fraction=1.0)
    tray.Finish()
    del tray
    

## ------------------- THE EXECUTABLE ----------------------
def main():
    """Run setup arguments and run main function"""
    description = """Update a step1 GCD file with juicy IceTop goodness for step2."""

    # Parse the arguments
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("-i", "--infile", type=str, required=True,
                        help="Input GCD i3 filename")
    parser.add_argument("-o", "--outfile", type=str, required=True,
                        help="Output GCD i3 filename")
    parser.add_argument("-r", "--run", type=int, dest = "run_id", required=False,
                        help="Run Number")
    '''
    parser.add_argument("--skip_audit", default=False, action="store_true",
                        help="Skip auditor")
    parser.add_argument("--production-version", dest = "production_version",
                        type=int, default = -1,
                        help="Set production version")
    '''
    parser.add_argument("--log-file", dest = "logfile",
                        type=str, help = "File where logs will be written")
    args = parser.parse_args()

    # Run it!
    runme(**vars(args))

if __name__ == "__main__":
    main()
