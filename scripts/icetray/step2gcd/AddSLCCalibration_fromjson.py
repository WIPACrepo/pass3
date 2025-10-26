##
# SPDX-FileCopyrightText: 2024 The IceTray Contributors
# SPDX-License-Identifier: BSD-2-Clause
#
# A module which fills an "I3IceTopSLCCalibrationCollection" structure in a C-frame,
# This is a stopgap measure, for until this is done automatically during "new processing"
# or for historical (i.e. pass3) data.
# ..... SHOULD WE MOVE THIS TO INTO ICETRAY?
##

#from datetime import datetime
import json
#import numpy as np
#import re

from icecube import dataclasses, icetray
from icecube.icetray.i3logging import log_debug, log_info, log_warn, log_fatal

class AddSLCCalibrationCollection_fromjson(icetray.I3Module):
    """Take a GCD file and modify it according to a json file (of the kind that would be injected into the DB)
    
    Adds a I3IceTopSLCCalibrationCollection frame object to the C frame with the SLC calibrations constants for all IceTop DOMs
    (also for the "chip unknown" case).
    """

    def __init__(self, ctx):
        super().__init__(ctx)
        self.AddParameter('SLCCalibFile', 'SLC calibration .json (of the kind that would be injected into the DB')
        self.AddParameter('Provenance', 'What enum is the Provenance for these numbers (where are they from)?', dataclasses.ITSLCCalProvenance.Placeholder)
        self.AddParameter('CollectionName', 'What the frame object should be named', "I3IceTopSLCCalibrationCollection")
        self.AddOutBox('OutBox')

    def Configure(self):
        self.slccalibfile = self.GetParameter('SLCCalibFile')
        self.prov = self.GetParameter('Provenance')
        self.collection_name = self.GetParameter('CollectionName')

    def Calibration(self, frame):
        # The new object
        calibration_collection = dataclasses.I3IceTopSLCCalibrationCollection()

        # Load the big dictionary
        with open(self.slccalibfile) as f:  # the default is read "r" mode
            j = json.load(f)

        # Fill the I3FrameObject structure
        calibration_collection.start_run = j["StartRun"]
        calibration_collection.end_run = j["EndRun"]
        d = j["data"]
        for m in d:  # Loop through all the maps: one per DOM
            omkey = icetray.OMKey(m["StringId"], m["OmId"])
            thisvalue = dataclasses.I3IceTopSLCCalibration()
            thisvalue.SetIntercept(0, 0, m['Intercept_C0A0'])
            thisvalue.SetIntercept(0, 1, m['Intercept_C0A1'])
            thisvalue.SetIntercept(0, 2, m['Intercept_C0A2'])
            thisvalue.SetIntercept(1, 0, m['Intercept_C1A0'])
            thisvalue.SetIntercept(1, 1, m['Intercept_C1A1'])
            thisvalue.SetIntercept(1, 2, m['Intercept_C1A2'])
            thisvalue.SetIntercept(-1, 0, m['Intercept_CunkA0'])
            thisvalue.SetIntercept(-1, 1, m['Intercept_CunkA1'])
            thisvalue.SetIntercept(-1, 2, m['Intercept_CunkA2'])
            thisvalue.SetSlope(0, 0, m['Slope_C0A0'])
            thisvalue.SetSlope(0, 1, m['Slope_C0A1'])
            thisvalue.SetSlope(0, 2, m['Slope_C0A2'])
            thisvalue.SetSlope(1, 0, m['Slope_C1A0'])
            thisvalue.SetSlope(1, 1, m['Slope_C1A1'])
            thisvalue.SetSlope(1, 2, m['Slope_C1A2'])
            thisvalue.SetSlope(-1, 0, m['Slope_CunkA0'])
            thisvalue.SetSlope(-1, 1, m['Slope_CunkA1'])
            thisvalue.SetSlope(-1, 2, m['Slope_CunkA2'])

            calibration_collection.it_slc_cal[omkey] = thisvalue

        calibration_collection.provenance = self.prov

        # Add the I3IceTopSLCCalibrationCollection to the frame...
        # ---------------------------------------------------
        # First, check to see if it already exists. 
        # We have to do this because is someone invokes gcdserver to make this object, and there is no DB info for the run yet,
        # gcdserver will create an object, but it'll be empty.
        if frame.Has(self.collection_name):
            # It does exist! Check if it's empty.  (It should be!)
            c = frame[self.collection_name]
            if len(c.it_slc_cal) != 0:
                log_fatal("I found a preexisting %s, of non-zero length %d"%(self.collection_name, len(c.it_slc_cal)))
            if (c.start_run != 0) or (c.end_run != 0):
                log_fatal("I found a preexisting %s, with non-zero start/end runs %d %d"%(c.start_run, c.end_run))
            if (c.provenance != 0):
                log_fatal("I found a preexisting %s, with non-zero provenance %d"%c.prov)
            # If we survived all that checking, then the existint object is empty, and we can go ahead and delete it from the frame.
            log_info("So you know, I found an empty %s and I'm deleting it to make a new one."%self.collection_name)
            frame.Delete(self.collection_name)
        # Now, put the new awesome one in there!
        frame.Put(self.collection_name, calibration_collection)
        self.PushFrame(frame)       # push the C-frame

