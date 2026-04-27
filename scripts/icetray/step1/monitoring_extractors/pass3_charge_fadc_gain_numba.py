import logging
import numpy as np
from scipy.stats import norm as gaus
from scipy.optimize import minimize
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
                          [0.6, 1.4])

        self.AddParameter("ChargeBinsize",
                          "Bin size to use for charge histograms. Default 0.1 PE.",
                          0.1)
        
        self.AddParameter("ChargeBinMin",
                          "Minimum charge to histogram. Default 0 PE.",
                          0.0)

        self.AddParameter("ChargeBinMax",
                          "Maximum charge to histogram. Default 5 PE.",
                          5.0)

    def Configure(self):
        """Do any preliminary setup."""
        self.output_filename = self.GetParameter("OutputFilename")
        self.psm_key = self.GetParameter("PulseSeriesMapKey")
        self.peak_fit_bounds = self.GetParameter("PeakFitBounds")
        # Choose bins to match the charge discretization from SuperDST
        # https://docs.icecube.aq/icetray/main/projects/dataclasses/superdst.html#charge-stamps-inice
        self.charge_binsize = self.GetParameter("ChargeBinsize")
        self.charge_binmin = self.GetParameter("ChargeBinMin")
        self.charge_binmax = self.GetParameter("ChargeBinMax")

        self.charge_bins = np.arange(
            self.charge_binmin,
            self.charge_binmax + self.charge_binsize,
            self.charge_binsize)

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
                atwd_mean, atwd_sigma = self._estimate_peak(self.atwd_histograms[omkey])
                fadc_mean, fadc_sigma = self._estimate_peak(self.fadc_histograms[omkey])

                percent_diff = np.abs(atwd_mean-fadc_mean)/atwd_mean
                print(f"OMKey {omkey[0]}-{omkey[1]}, ATWD: {atwd_mean}, FADC: {fadc_mean}, percent diff: {percent_diff}")
                f.write(f"OMKey {omkey[0]}-{omkey[1]}, ATWD: {atwd_mean}, FADC: {fadc_mean}, percent diff: {percent_diff}\n")
                if percent_diff > 0.01:
                    self.logger.warning(f"PulseChargeFilterHarvester: ATWD and FADC mean charge differ for OMKey {omkey[0]}-{omkey[1]} by > 1%")

    def _estimate_peak(self, histogram):
        xvals = self.charge_bins_center[self.bin_mask]
        yvals = histogram[..., self.bin_mask]
        mean = (xvals * yvals).sum(axis=-1) / yvals.sum(axis=-1)
        variance = (xvals**2 * yvals).sum(axis=-1) / yvals.sum(axis=-1) - mean**2

        gaus_mean, gaus_sigma = np.zeros_like(mean), np.zeros_like(mean)

        # Get the bins edges associated with the masked bin centers
        bins = np.unique([self.charge_bins[:-1][self.bin_mask],
                          self.charge_bins[1:][self.bin_mask]])

        for idx in np.ndindex(mean.shape):
            # We're using 0-indexed arrays with empty string=0 rows and om=0 columns.
            # Skip those cases.
            if np.sum(yvals[idx]) == 0:
                continue
            min_opts = {'maxiter': 10000, 'gtol': 1e-6, 'disp': False}
            seed = (mean[idx], np.sqrt(variance[idx]))
            min_bds = ((seed[0] - 0.5, seed[0] + 0.5),
                       (seed[1] * 0.5, seed[1] * 2))
            def chi2(params, summed=True):
                mean, sigma = params
                cdf = gaus.cdf(bins, loc=mean, scale=sigma)
                expected = np.diff(cdf)
                expected *= yvals[idx].sum() / expected.sum()

                perbin = (yvals[idx]-expected)**2 / expected#yvals[idx]
                if summed:
                    total = np.nansum(perbin)
                    if total == 0:
                        # This is only likely to happen when the fit wanders off. Penalize it.
                        return np.finfo(np.float64).max
                    return total
                else:
                    return perbin

            method = "Nelder-Mead"
            result = minimize(chi2, x0=seed, method=method,
                              bounds=min_bds, options=min_opts)
            if not result.success:
                seed1 = [sv * 0.9 for sv in seed]
                result = minimize(chi2, x0=seed1, method=method,
                                  bounds=min_bds, options=min_opts)
                if not result.success:
                    seed1 = [sv * 1.1 for sv in seed]
                    result = minimize(chi2, x0=seed1, method=method,
                                      bounds=min_bds, options=min_opts)
            gaus_mean[idx] = result.x[0]
            gaus_sigma[idx] = result.x[1]
        return gaus_mean, gaus_sigma

    def _write_histogram(self):
        """Write any output files you'll need for testing."""
        atwd_mean, atwd_sigma = self._estimate_peak(self.atwd_histograms)
        fadc_mean, fadc_sigma = self._estimate_peak(self.fadc_histograms)
        np.savez(self.output_filename,
                 bounds    = self.peak_fit_bounds,
                 start     = self.start_time,
                 atwd      = self.atwd_histograms,
                 atwd_mean = atwd_mean,
                 atwd_sigma = atwd_sigma,
                 fadc      = self.fadc_histograms,
                 fadc_mean = fadc_mean,
                 fadc_sigma = fadc_sigma,
                 bins      = self.charge_bins,
                 allow_pickle = False)
        self.logger.warning(f"PulseChargeFilterHarvester: Found " +
                            "and wrote ATWD and FADC mean charges" +
                            f" to {self.output_filename}.")
