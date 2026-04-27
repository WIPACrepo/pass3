#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py3-v4.4.2/icetray-start
#METAPROJECT /data/user/briedel/pass3/icetray/v1.17.0/build/

""""Sanity checks on output of Pass3 Step 1 script"""
import argparse

from icecube.icetray import I3Tray
from icecube import dataio
from icecube import icetray
from monitoring_extractors.pass3_charge_fadc_gain_numba import PulseChargeFilterHarvester


parser = argparse.ArgumentParser(
    description='Histogram and dump to a pickle file.')
parser.add_argument("-o","--output-filename",
                    dest = "OUTPUT_FILENAME",
                    required=True,
                    help = "output file name.")
parser.add_argument("-i","--infiles",
                    dest = "INFILES",
                    nargs="+",
                    required=True,
                    help = "files to get charge histogram.")
parser.add_argument("-g","--gcd",
                    dest="GCD",
                    required=True,
                    help="GCD File to be used for unpacking")
parser.add_argument("-q", "--qify",
                    dest="qify", action='store_true', default=False,
                    help="Convert P-frame PFFilt output to Q-frames.")
parser.add_argument("--filter-rates",
                    dest="filter_rates", action='store_true', default=False,
                    help="Calculate and log filter rates.")
args = parser.parse_args()

icetray.set_log_level_for_unit('I3Tray', icetray.I3LogLevel.LOG_INFO)
icetray.set_log_level_for_unit('I3Reader', icetray.I3LogLevel.LOG_INFO)


tray = I3Tray()

tray.Add(dataio.I3Reader, "reader", FilenameList=[args.GCD] + args.INFILES)

if args.qify:
    tray.AddModule("QConverter", "qify", WritePFrame=False)

tray.Add(PulseChargeFilterHarvester, "charge_harvester",
         PulseSeriesMapKey = "I3SuperDST",
         OutputFilename = args.OUTPUT_FILENAME + ".fadc_atwd_charge.npz"
         )

if args.filter_rates:
    from monitoring_extractors.pass3_calc_filter_rate import FilterRateMonitorI3Module

    tray.Add(FilterRateMonitorI3Module, "filter_rate_monitor",
            eventheader_key = "I3EventHeader",
            filtermask_key = "OnlineFilterMask",
            output_file = args.OUTPUT_FILENAME + ".filter_rates.txt"
            )

tray.Execute()
