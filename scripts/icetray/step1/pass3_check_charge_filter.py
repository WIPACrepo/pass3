""""Sanity checks on output of Pass3 Step 1 script"""
import argparse

from icecube.icetray import I3Tray
from icecube import dataio
from icecube import icetray
from monitoring_extractors.pass3_charge_monitor import ChargeMonitorI3Module
from monitoring_extractors.pass3_calc_filter_rate import FilterRateMonitorI3Module
from monitoring_extractors.pass3_charge_fadc_gain import PulseChargeFilterHarvester
from icecube.offline_filterscripts.read_superdst_files import read_superdst_files

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
args = parser.parse_args()

icetray.set_log_level_for_unit('I3Tray', icetray.I3LogLevel.LOG_TRACE)

tray = I3Tray()

tray.Add(dataio.I3Reader, "reader", FilenameList=[args.GCD] + args.INFILES)

tray.Add(ChargeMonitorI3Module, "charge_histogram",
         input_key = "I3SuperDST",
         output_file_path = args.OUTPUT_FILENAME + ".npz")

tray.Add(FilterRateMonitorI3Module, "filter_rates",
         output_file = args.OUTPUT_FILENAME + ".txt")

tray.Add(PulseChargeFilterHarvester, "charge_harvester",
         PulseSeriesMapKey = "I3SuperDST",
         OutputFilename = args.OUTPUT_FILENAME + ".fadc_atwd_charge.npz"
         )

tray.Execute()
