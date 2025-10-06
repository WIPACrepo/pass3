#!/usr/bin/env python3
"""
pass3_reprocess_PFRaw: A standalone script to reprocess PFRaw files to PPFilt files ahead of offline refiltering.

This script processes input i3 files using specified GCD files and applies various offline filters. It supports
command-line arguments to specify input/output files, logging levels, and other options.

Usage:
    python pass3_reprocess_PFRaw.py -i <input_file> -o <output_file> -g <gcd_file> [options]

Options:
    -i, --input                Input i3 file(s) to process, separated by spaces (required)
    -o, --output               Output i3 file (required)
    -g, --gcd                  GCD file for input i3 file (required)
    --qify                     Apply QConverter if input file contains only P frames
    -n, --num                  Number of frames to process (default: -1 = all frames)
    -s, --sim                  Input data is simulation

Example:  python pass3_process_PFRaw --qify -g PFGCD_Run00137496_Subrun00000000_pass3.i3.gz -i \
    ./PFRaw_PhysicsFiltering_Run00137496_Subrun00000000_00000000.tar.gz -o test.i3
"""

import os
import sys
import time
import copy

from argparse import ArgumentParser

from icecube import dataio, icetray

from icecube.icetray import I3Tray
from icecube.icetray import logging as log
from icecube.online_filterscripts.pole_base_processing import pole_base_package_output, pole_base_processing_and_filter
from icecube.phys_services import I3GSLRandomService

# handling of command line arguments
parser = ArgumentParser(
    prog="PFRaw_to_DST",
    description="Stand alone example to simulate pole filtering")
parser.add_argument("-i", "--input", action="store", default=None,
                    dest="INPUT", help="Input i3 file to process",
                    required=True)
parser.add_argument("-o", "--output", action="store",
                    default=None, dest="OUTPUT", help="Output i3 file",
                    required=True)
parser.add_argument("-g", "--gcd", action="store", default=None,
                    dest="GCD", help="GCD file for input i3 file",
                    required=True)
parser.add_argument("--qify", action="store_true", default=False, dest="QIFY",
                    help="Apply QConverter, use if input file is only P frames")
parser.add_argument("-s", "--sim", action="store_true", default=False,
                    dest="SIM", help="Input data is simulation.")
parser.add_argument("-n", "--nframes", action="store", default=-1, type=int, dest="NFRAMES",
                    help="Number of frames to process. Use a small number for testing")
args = parser.parse_args()

# prep the logging
icetray.logging.console()
icetray.I3Logger.global_logger.set_level(icetray.I3LogLevel.LOG_WARN)

start_time = time.asctime()

gcd_path, gcd_filename = os.path.split(args.GCD)
log.log_warn(f"GCD path and file: {gcd_path} {gcd_filename}")

if args.SIM:
    log.log_info("Processing Simulalation data")
    simulation = True
    InfileSkip = []
    if args.QIFY:
        icetray.logging.log_fatal("Running QIFY with simulation doesn't make sense... Breaking.")
else:
    log.log_info("Processing raw data")
    simulation = False
    InfileSkip = ["I3EventHeader", "JEBEventInfo"]

tray = I3Tray()

tray.Add(dataio.I3Reader, "reader", filenamelist=[args.GCD, args.INPUT],
         SkipKeys=InfileSkip)

# Save the original "Pole L1 filter results"
# tray.AddModule("Rename", "filtermaskmover",
#                Keys=["QFilterMask", "Pass1/QFilterMask"])
# OR
# Delete original QFilterMask
tray.AddModule("Delete", "scrub_old_qfilter",
               keys=["QFilterMask"])

# If needed, move P Frame to Q Frame...
# Note: most PFRaw from pole files are P frame only
if args.QIFY:
    tray.AddModule("QConverter", "qify", WritePFrame=False)

# Random number service for applying filter prescales, as used in pfclient online
# Started with a generated relatively random seed for it
random_srvc = I3GSLRandomService(int.from_bytes(os.urandom(4), sys.byteorder))


filter_file = os.path.expandvars("$I3_SRC/online_filterscripts/resources/filter_config.json")
# core... base processing and filter...
tray.Add(pole_base_processing_and_filter, "Base",
         simulation=simulation,
         needs_trimmer=True,
         do_vemcal=True,
         do_icetopslccal=True,
         pole_wavedeform_bypass=False,
         omit_GCD_diff=True,
         filter_definition_file=filter_file,
         random_service=random_srvc,
         )

# package the output for "PFFilt" format for sending via satellite.
tray.Add(pole_base_package_output, "Clean",
         simulation=simulation)


# Pretend to be the "PFFiltWriter" in PnF:
# clean up raw data as directed by filters.
if not simulation:
    tray.Add("Delete", "Delete_DAQTrimmer_cleanallwfs",
             keys=["I3DAQData"],
             If=lambda f: ("KeepAllWaveforms_MinBias" in f) and \
             (f["KeepAllWaveforms_MinBias"].value is False))
    tray.Add("Delete", "Delete_DAQTrimmer_cleanitwfs",
             keys=["I3DAQDataIceTop"],
             If=lambda f: ("KeepIceTopWaveforms" in f) and \
             (f["KeepIceTopWaveforms"].value is False))
    # If we're keeping all waveforms, drop the seatbelts, IT waveforms
    tray.Add("Delete", "Delete_DAQTrimmer_cleanseatbelts",
             keys=["I3DAQDataTrimmed", "I3DAQDataIceTop"],
             If=lambda f: ("KeepAllWaveforms_MinBias" in f) and \
             (f["KeepAllWaveforms_MinBias"].value is True))
else:
    tray.Add("Delete", "Delete_DAQTrimmer_cleanallwfs",
             keys=["CleanRawData"],
             If=lambda f: ("KeepAllWaveforms_MinBias" in f) and \
             (f["KeepAllWaveforms_MinBias"].value is False))
    # If we're keeping all waveforms, drop the seatbelts
    tray.Add("Delete", "Delete_DAQTrimmer_cleanseatbelts",
             keys=["SimTrimmer_Seatbelts"],
             If=lambda f: ("KeepAllWaveforms_MinBias" in f) and \
             (f["KeepAllWaveforms_MinBias"].value is True))

q_drop_trigger_filt = ["DSTTriggers",
                       "DrivingTime",
                       "I3DAQDataTrimmed",
                       "I3SuperDST",
                       "I3EventHeader",
                       "OnlineFilterMask"]


def clean_q_trigger_filt(frame, droplist=()):
    """Create a simple Q frame cleaner."""
    for item in droplist:
        if item in frame:
            frame.Delete(item)


# Clean out the Q-Frame to just SuperDST if we don't pass
#  any of Keep_SuperDST/KeepAllWaveforms_MinBias
tray.Add(clean_q_trigger_filt, "q_drop_trigger_filt",
         droplist=q_drop_trigger_filt,
         Streams=[icetray.I3Frame.DAQ],
         If=lambda frame: frame["Keep_SuperDST_23"].value is False and \
         frame["KeepAllWaveforms_MinBias"].value is False)

last_garbage = ["Keep_SuperDST_23",
                "KeepIceTopWaveforms",
                "KeepAllWaveforms_MinBias"]

tray.Add("Delete", "Delete_final_cleanup",
         keys=last_garbage)

output_file_qkeys = ["DSTTriggers",
                     "I3DAQDataIceTop",
                     "I3DAQDataTrimmed",
                     "I3DAQData",
                     "I3DST22Header",
                     "I3DST22_InIceSplit0",
                     "I3DST22_InIceSplit1",
                     "SoftwareTrigFilt_23_InIceSplit0",
                     "SoftwareTrigFilt_23_InIceSplit1",
                     "I3EventHeader",
                     "I3SuperDST",
                     "Pass1/QFilterMask",
                     "I3VEMCalData",
                     "I3Calibration",  # Items from GCD
                     "I3Geometry",
                     "I3DetectorStatus",
                     "IceTopBadDOMs",
                     "I3FlasherSubrunMap",
                     "IceTopBadTanks",
                     "GoodRunStartTime",
                     "GoodRunEndTime",
                     "BadDomsListSLC",
                     "GRLSnapshotId",
                     "BadDomsList",
                     "OfflineProductionVersion",
                     "I3ITSLCCalData",
                     ]

def check_q_frame_keys(frame, keylist):
    """Check that the output frame for unexpected keys."""
    for item in frame.keys():
        if item not in keylist:
            icetray.logging.log_error(f"Unexpected key in output file: {item}")


##############################
#  ICETRAY PROCESSING BELOW  #
##############################
# add one second to event header to compensate for missed leap second adjustment
# https://internal-apps.icecube.wisc.edu/reports/data/icecube/2018/12/001/icecube_201812001_v1.pdf
leap_tmin = dataclasses.I3Time()
leap_tmax = dataclasses.I3Time()
leap_tmin.set_utc_cal_date(2012,6,30,3,24,03,0.)
leap_tmax.set_utc_cal_date(2015,5,18,0,59,04,0.)


def fix_leap_second(frame, start_date=None, end_date=None):
    if (header.start_time >= start_date and header.start_time < end_date):
        header = frame['I3EventHeader']
        original_header = copy.deepcopy(frame['I3EventHeader'])
        header.end_time += I3Units.second
        header.start_time += I3Units.second
        del frame['I3EventHeader']
        frame['I3EventHeader'] = header
        frame['I3EventHeader_uncorrected_leap_second'] = original_header
    return True

tray.AddModule(fix_leap_second,
              start_date=leap_tmin,
              end_date=leap_tmax)


# Write the physics and DAQ frames
tray.AddModule("I3Writer", "EventWriter", filename=args.OUTPUT,
                   Streams=[icetray.I3Frame.DAQ,
                            icetray.I3Frame.TrayInfo,
                            icetray.I3Frame.Simulation,
                            icetray.I3Frame.Stream("M")])

if args.NFRAMES > 0:
    tray.Execute(args.NFRAMES)
else:
    tray.Execute()

stop_time = time.asctime()
log.log_warn(f"Started: {start_time}")
log.log_warn(f"Ended: {stop_time}")
