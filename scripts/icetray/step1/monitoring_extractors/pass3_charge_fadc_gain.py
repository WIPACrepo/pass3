import logging
import numpy as np
from icecube import dataclasses, icetray

from numba import njit

@njit
def pulsemap_to_histograms(pulsemap_np, bins, atwd_hists, fadc_hists):
    """ A numba njit'ed function to histogram an entire I3RecoPulseSeriesMap.

    The pulsemap_np is a np.asarray of an I3RecoPulseSeriesMap.
     This is a 2d array with columns (String, OM, PMT, Time, Charge, Width).
     We're going to be adding this to atwd_hists and fadc_hists, which are
     3d histograms of [String, OM] -> Charge.

    Note that pulsemap_np doesn't include digitizer information. We're going to
    follow the example from https://github.com/icecube/icetray/pull/3017 and the
    low energy group by using the pulse width as a proxy flag for ATWD/FADC."""
    assert atwd_hists.shape==(87, 61, len(bins)-1)
    assert fadc_hists.shape==(87, 61, len(bins)-1)
    
    for row in pulsemap_np:
        string, om, pmt, t, charge, width = row
        charge_bin = int((charge-bins[0])/(bins[1]-bins[0]))
        if charge_bin > len(bins)-1:
            continue

        # We need to push ATWD and FADC pulses to separate histograms,
        # but the np.asarray interface doesn't include the pulse flags.
        # Use the pulse widths (given in ns) as a proxy.
        if width < 6:
            atwd_hists[int(string), int(om), int(charge_bin)] += 1
        else:
            fadc_hists[int(string), int(om), int(charge_bin)] += 1
    return

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
