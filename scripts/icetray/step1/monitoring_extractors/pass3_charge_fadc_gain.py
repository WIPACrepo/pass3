import logging
import numpy as np
import sys
import unittest
from icecube import dataclasses, dataio, icetray
from icecube import DomTools

class PulseChargeFilterHarvester(icetray.I3ConditionalModule):
    """A simple I3Module to gather SPE pulse charges for testing.
    Taken from the pass3 filter scripts. """
    def __init__(self, context):
        """Initialize the I3Module."""
        icetray.I3ConditionalModule.__init__(self, context)
        self.logger = logging.getLogger("PulseChargeFilterHarvester")

        self.AddParameter("PulseSeriesMapKey",
                          "Pulses to monitor",
                          "I3SuperDST")

        self.AddParameter("OutputFilename",
                          "The path and name of the output file for this module as a string.",
                          "./PulseCharge_testvars.npz")

    def Configure(self):
        """Do any preliminary setup."""
        self.output_filename = self.GetParameter("OutputFilename")
        self.psm_key = self.GetParameter("PulseSeriesMapKey")
        # Choose bins to match the charge discretization from SuperDST
        # https://docs.icecube.aq/icetray/main/projects/dataclasses/superdst.html#charge-stamps-inice
        self.charge_binsize = 0.1
        self.charge_bins = np.arange(0, 5 + self.charge_binsize, self.charge_binsize)
        self.atwd_charges = {}
        self.fadc_charges = {}

        self.nframes = 0

    def DetectorStatus(self, frame):
        """Set initial vlaues for every good DOM"""
        #------------------------------
        # Grab the bad doms from both the standard and the SLC list
        #------------------------------
        baddoms = set(list(frame["BadDomsList"]) + list(frame["BadDomsListSLC"]))
        for omkey in frame["I3DetectorStatus"].dom_status.keys():
            self.atwd_charges[omkey] = np.zeros(len(self.charge_bins))
            self.fadc_charges[omkey] = np.zeros(len(self.charge_bins))

        self.PushFrame(frame)
        return

    def DAQ(self, frame):
        """Grab information from Q-frames for testing."""
        if self.psm_key in frame:
            pulsemap = dataclasses.I3RecoPulseSeriesMap.from_frame(frame,  self.psm_key)
            for omkey, pulses in pulsemap.items():
                if len(pulses) == 0: continue
                q_atwd, q_fadc = 0, 0
                for pulse in pulses:
                    if pulse.flags & pulse.PulseFlags.ATWD:
                        q_atwd += pulse.charge
                    else:
                        q_fadc += pulse.charge
                if (self.charge_bins[0] < q_atwd) & (q_atwd < self.charge_bins[-1]):
                    atwd_bin = int(q_atwd/self.charge_binsize)
                    self.atwd_charges[omkey][atwd_bin] += 1
                if (self.charge_bins[0] < q_fadc) & (q_fadc < self.charge_bins[-1]):
                    fadc_bin = int(q_fadc/self.charge_binsize)
                    self.fadc_charges[omkey][fadc_bin] += 1

            self.nframes += 1
        self.PushFrame(frame)
        return
        
    def Finish(self):
        """Write any output files you'll need for testing."""
        strings, oms, atwd, fadc = [], [], [], []
        for omkey in self.atwd_charges:
            strings.append(omkey.string)
            oms.append(omkey.om)
            atwd.append(self.atwd_charges[omkey])
            fadc.append(self.fadc_charges[omkey])
            
        np.savez(self.output_filename,
                 string= np.array(strings),
                 om =    np.array(oms),
                 atwd =  np.array(atwd),
                 fadc =  np.array(fadc),
                 bins =  self.charge_bins,
                 allow_pickle = False)

        self.logger.warning(f"PulseChargeFilterHarvester: Found and wrote ATWD and FADC mean charges"
                            f" to {self.output_filename}.")
