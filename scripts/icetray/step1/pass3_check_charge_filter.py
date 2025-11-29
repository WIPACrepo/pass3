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
 
tray = I3Tray()

# tray.Add(read_superdst_files, "_read_superdst_files",
#          input_files=args.INFILES,
#          input_gcd=args.GCD,
#          qify_input=True)

tray.Add(dataio.I3Reader, "reader", FilenameList=[args.GCD] + args.INFILES)


tray.Add(ChargeMonitorI3Module, "charge_histogram",
         input_key = "I3SuperDST",
         output_file_path = args.OUTPUT_FILENAME + ".npz")

tray.Add("Dump")

tray.Add(FilterRateMonitorI3Module, "filter_rates",
         output_file = args.OUTPUT_FILENAME + ".txt")

tray.Add(PulseChargeFilterHarvester, "charge_harvester",
         PulseSeriesMapKey = "I3SuperDST",
         OutputFilename = args.OUTPUT_FILENAME + "fadc_atwd_charge.npz"
         )

tray.AddModule("I3Writer", "EventWriter", filename=args.OUTPUT_FILENAME,
                   Streams=[icetray.I3Frame.DAQ,
                            icetray.I3Frame.Physics,
                            icetray.I3Frame.TrayInfo,
                            icetray.I3Frame.Simulation,
                            icetray.I3Frame.Stream("M")])

tray.Execute()
