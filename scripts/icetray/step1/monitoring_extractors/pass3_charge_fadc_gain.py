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

        self.AddParameter("PeakFitBounds",
                          "Bounds to ignore when calculating meaning charge",
                          [0.8, 1.2])

    def Configure(self):
        """Do any preliminary setup."""
        self.output_filename = self.GetParameter("OutputFilename")
        self.psm_key = self.GetParameter("PulseSeriesMapKey")
        self.peak_fit_bounds = self.GetParameter("PeakFitBounds")
        # Choose bins to match the charge discretization from SuperDST
        # https://docs.icecube.aq/icetray/main/projects/dataclasses/superdst.html#charge-stamps-inice
        self.charge_binsize = 0.025
        self.charge_bins = np.arange(0, 5 + self.charge_binsize, self.charge_binsize)
        self.charge_bins_center = self.charge_bins[:-1] + np.diff(self.charge_bins)
        self.atwd_charges = {}
        self.fadc_charges = {}
        self.bin_mask = ((self.peak_fit_bounds[0] <= self.charge_bins)
                          & (self.charge_bins <= self.peak_fit_bounds[1]))[:-1]

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
        self._write_histogram()
        self._compare_charge_peaks()

    def _compare_charge_peaks(self):
        with open(self.output_filename + ".comparison", "w") as f:
            for omkey in self.atwd_charges:
                atwd_mean = self._estimate_peak(self.atwd_charges[omkey])
                fadc_mean = self._estimate_peak(self.fadc_charges[omkey])
                mean = (atwd_mean+fadc_mean)/2
                ratio = np.abs(atwd_mean-fadc_mean)/mean
                f.write(f"OMKey {omkey}, ATWD: {atwd_mean}, FADC: {fadc_mean}, ratio: {ratio}\n")
                if ratio > 0.01:
                    self.logger.warning(f"PulseChargeFilterHarvester: ATWD and FADC mean charge ratio for OMKey {omkey} is > 1%")

    def _estimate_peak(self, histogram):
        yvals = histogram * self.charge_bins_center
        mean = yvals[self.bins_mask].sum()/histogram[self.bins_mask].sum()
        return mean

    def _write_histogram(self):
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
        self.logger.warning(f"PulseChargeFilterHarvester: Found " +
                            "and wrote ATWD and FADC mean charges" +
                            f" to {self.output_filename}.")