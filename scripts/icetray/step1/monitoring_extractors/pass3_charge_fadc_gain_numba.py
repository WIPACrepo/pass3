import logging
import numpy as np
from icecube import dataclasses, icetray

from .numba_charge_histogram import pulsemap_to_histograms

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
        self.shape = (87, 61, len(self.charge_bins)-1)
        self.atwd_histograms = np.zeros(self.shape, dtype=np.float32)
        self.fadc_histograms = np.zeros(self.shape, dtype=np.float32)
        self.bin_mask = ((self.peak_fit_bounds[0] <= self.charge_bins)
                          & (self.charge_bins <= self.peak_fit_bounds[1]))[:-1]
        self.start_time = None
        self.nframes = 0

    def DAQ(self, frame):
        """Grab information from Q-frames for testing."""
        if (self.start_time is None) and "I3EventHeader" in frame.keys():
            header = frame["I3EventHeader"]
            self.start_time = np.datetime64(header.start_time.date_time)
        if self.psm_key in frame:
            pulsemap = dataclasses.I3RecoPulseSeriesMap.from_frame(frame,  self.psm_key)
            if len(pulsemap.keys()) > 0:
                pulsemap_to_histograms(np.asarray(pulsemap),
                                       self.charge_bins,
                                       self.atwd_histograms,
                                       self.fadc_histograms)
            self.nframes += 1
        self.PushFrame(frame)
        return

    def Finish(self):
        self._write_histogram()
        self._compare_charge_peaks()

    def _compare_charge_peaks(self):
        with open(self.output_filename + ".comparison", "w") as f:
            for omkey in np.ndindex(self.shape[:-1]):
                if (omkey[0]==0) or (omkey[1]==0):
                    continue
                if ((self.atwd_histograms[omkey].sum() == 0)
                    and (self.fadc_histograms[omkey].sum() == 0)):
                    continue
                atwd_mean = self._estimate_peak(self.atwd_histograms[omkey])
                fadc_mean = self._estimate_peak(self.fadc_histograms[omkey])
                mean = (atwd_mean+fadc_mean)/2
                percent_diff = np.abs(atwd_mean-fadc_mean)/mean
                print(f"OMKey {omkey[0]}-{omkey[1]}, ATWD: {atwd_mean}, FADC: {fadc_mean}, percent diff: {percent_diff}")
                f.write(f"OMKey {omkey[0]}-{omkey[1]}, ATWD: {atwd_mean}, FADC: {fadc_mean}, percent diff: {percent_diff}\n")
                if percent_diff > 0.01:
                    self.logger.warning(f"PulseChargeFilterHarvester: ATWD and FADC mean charge differ for OMKey {omkey[0]}-{omkey[1]} by > 1%")

    def _estimate_peak(self, histogram):
        yvals = histogram * self.charge_bins_center
        mean = yvals[...,self.bin_mask].sum(axis=-1) / histogram[...,self.bin_mask].sum(axis=-1)
        return mean

    def _write_histogram(self):
        """Write any output files you'll need for testing."""
        np.savez(self.output_filename,
                 start     = self.start_time,
                 atwd      = self.atwd_histograms,
                 atwd_peak = self._estimate_peak(self.atwd_histograms),
                 fadc      = self.fadc_histograms,
                 fadc_peak = self._estimate_peak(self.fadc_histograms),
                 bins      = self.charge_bins,
                 allow_pickle = False)
        self.logger.warning(f"PulseChargeFilterHarvester: Found " +
                            "and wrote ATWD and FADC mean charges" +
                            f" to {self.output_filename}.")
