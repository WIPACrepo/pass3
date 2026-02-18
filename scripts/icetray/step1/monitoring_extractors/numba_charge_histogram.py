import numpy as np
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

    qtot_atwd = np.zeros((87, 61), dtype=np.float32)
    qtot_fadc = np.zeros((87, 61), dtype=np.float32)

    for row in pulsemap_np:
        string, om, pmt, t, charge, width = row

        # We need to push ATWD and FADC pulses to separate histograms,
        # but the np.asarray interface doesn't include the pulse flags.
        # Use the pulse widths (given in ns) as a proxy.
        if width < 6:
            qtot_atwd[int(string), int(om)] += charge
        else:
            qtot_fadc[int(string), int(om)] += charge

    for charge_hist, is_atwd in [(qtot_atwd, True), (qtot_fadc, False)]:
        for idx in np.ndindex((86, 61)):
            qtot = charge_hist[idx]
            if qtot == 0:
                continue
            
            charge_bin = int((qtot-bins[0])/(bins[1]-bins[0]))
            if charge_bin > len(bins)-1:
                continue
            
            if is_atwd:
                atwd_hists[*idx, charge_bin] += 1
            else:
                fadc_hists[*idx, charge_bin] += 1
    return
