#!/usr/bin/env python3

import time
from argparse import ArgumentParser

from icecube.icetray import I3Tray
from icecube import icetray


from icecube import (  # noqa: F401  # noqa: F401
    common_variables,
    dataclasses,
    dataio,
    ddddr,
    filter_tools,
    paraboloid,
    topeventcleaning,
    trigger_splitter,
)
from icecube.full_event_followup.followup_expand_saved_pframe import expand_saved_pframe
from icecube.online_filterscripts.base_segments.superdst import dst_mask_maker
from icecube.phys_services.which_split import which_split
from icecube.icetray import I3Units
from icecube.offline_filterscripts.base_segments.icetop_pulse_extract import ExtractIceTopTankPulses

from monitoring_extractors.pass3_charge_monitor import ChargeMonitorI3Module
from monitoring_extractors.pass3_calc_filter_rate import FilterRateMonitorI3Module

start_time = time.asctime()
print('Started:', start_time)


# handling of command line arguments
parser = ArgumentParser(
    prog='UnpackDST',
    description='Stand alone example to simulate pole filtering')
parser.add_argument("-i", "--input", action="store", default=None,
                    dest="INPUT", help="Input i3 file to process", required=True)
parser.add_argument("-o", "--output", action="store", default=None,
                    dest="OUTPUT", help="Output i3 file", required=True)
parser.add_argument("-g", "--gcd", action="store", default=None,
                    dest="GCD", help="GCD file for input i3 file", required=True)
parser.add_argument("--qify", action="store_true", default=False, dest="QIFY",
                    help="Apply QConverter, use if input file is only P frames")
parser.add_argument("-p", "--prettyprint", action="store_true",
                    dest="PRETTY", help="Do nothing other than big tray dump")
args = parser.parse_args()

# Prep the logging hounds.
icetray.logging.console()   # Make python logging work
icetray.I3Logger.global_logger.set_level(icetray.I3LogLevel.LOG_WARN)

tray = I3Tray()

# offline_filterscripts/python/read_superdst_files does file reading, drops non-SMT12-filtered events,
#    and splits back into QP frames.  Here, applying Qify (move input P frames to Q...)
print('Processing: ', args.GCD, args.INPUT)

tray.Add(dataio.I3Reader, "reader", FilenameList=[args.GCD, args.INPUT])
if args.QIFY:
    tray.AddModule("QConverter", "qify", WritePFrame=False)

#shim bad doms
def shim_bad_doms(frame):
    if not frame.Has("IceTopBadDOMs"):
        frame['IceTopBadDOMs'] = dataclasses.I3VectorOMKey()
        frame['IceTopBadTanks'] = dataclasses.I3VectorTankKey()
    return True

tray.AddModule(shim_bad_doms, "shim_bad_doms",
                Streams=[icetray.I3Frame.DetectorStatus])

def FrameDropper(frame):
    # save only filtered frames (traditional filters + SuperDST filter)
    # Require I3SuperDST, I3EventHeader, and DSTTriggers; delete the rest
    return (frame.Has("I3SuperDST") and frame.Has("DSTTriggers") and frame.Has("I3EventHeader"))

tray.AddModule(FrameDropper, "_framedropper",
               Streams=[icetray.I3Frame.DAQ, icetray.I3Frame.Physics])

def PassSDST(frame):
    # save only Keep_SuperDST_23 filter mask passes
    # delete the rest
    filtermask = "OnlineFilterMask"
    if frame.Has(filtermask):
        filt_mask = frame.Get(filtermask)
        return filt_mask["Keep_SuperDST_23"].prescale_passed
    return False

tray.AddModule(PassSDST, "_Check_Keep_SuperDST_23",
               Streams = [icetray.I3Frame.DAQ])

# Set up masks for normal inice + icetop pulse access
tray.AddModule(dst_mask_maker, "_superdst_aliases",
               Input="I3SuperDST",
               Output="DSTPulses",
               Streams=[icetray.I3Frame.DAQ])

# I3TriggerSplitter.
#  This is a splitter module Q-> P frames based on triggers
#   in the I3TriggerHierarchy.  It splits looking for
#   'quiet' times between triggers
# Params:  Take input triggerHeirarchy in Q frame: QTriggerHierarchy
#   results in an 'I3TriggerHierarchy' in each P frame.
#   input pulses come from wavedeform:  WavedeformPulses
#   output pulses in frame as WavedeformSplitPulses in P frame (as mask)
trigger_configs_inicesplit = [1011,   # Deep Core TriggerID
                              1006,   # InIce SMT8
                              1007,   # InIce String
                              21001,  # InIce Volume
                              ]
tray.AddModule("I3TriggerSplitter", "_InIceSplit",
               SubEventStreamName="InIceSplit",
               TrigHierName="DSTTriggers",
               TriggerConfigIDs=trigger_configs_inicesplit,
               InputResponses=["InIceDSTPulses"],
               OutputResponses=["SplitInIcePulses"],
               WriteTimeWindow=True)
# IceTop pulse processing and splitting
# -------------------------------------
#  First, prepare the IceTop Tank responses from SuperDST
#  Note: this requires a GCD file with the "I3IceTopSLCCalibrationCollection" in it.
#  If you're getting a log-fatal complaining that it's missing, but don't care and just want to proceed,
#  change "bypassSLCcal" to True in this module below.
tray.Add(ExtractIceTopTankPulses, "_extract_tank_pulses",
         IceTopPulses="IceTopDSTPulses",
         bypassSLCcal=True)

# I3TopHLCClusterCleaning is the IceTop cluster trigger splitter
tray.AddModule("I3TopHLCClusterCleaning", "_IceTopSplit",
               SubEventStreamName="IceTopSplit",
               InputPulses="IceTopHLCTankPulses",
               OutputPulses="CleanedIceTopHLCTankPulses",
               BadTankList="TankPulseMergerExcludedTanks",
               ExcludedTanks="ClusterCleaningExcludedTanks",
               InterStationTimeTolerance=200.0 * I3Units.ns,  # Default
               IntraStationTimeTolerance=200.0 * I3Units.ns,  # Default
               If=lambda frame: "IceTopHLCTankPulses" in frame and len(frame["IceTopHLCTankPulses"])>0,
               )

# Null split -> to split named 'NullSplit'
#     Used by min bias filters, FRT, etc...
tray.AddModule("I3NullSplitter", "NullSplit",
               SubEventStreamName="NullSplit")

# Distribute pole frame objects back into P frames, anything tagged "_InIceSplit<N>"
tray.AddModule("DistributePnFObjects", "_distribute",
               SubstreamNames=[
                   "InIceSplit",
                   "NullSplit",
                   "IceTopSplit",
               ])

# Decode any online saved frame objects into the right InIce_split
tray.Add(expand_saved_pframe, "_decode_saved_objects",
         object_key="PFrameEncoded",
         prepend_name="pole",
         If=lambda f: "PFrameEncoded" in f)

class PhysicsCopyTriggers(icetray.I3ConditionalModule):
    def __init__(self, context):
        icetray.I3ConditionalModule.__init__(self, context)
        self.AddOutBox("OutBox")

    def Configure(self):
        pass

    def Physics(self, frame):
        if frame.Has("DSTTriggers"):
            myth = dataclasses.I3TriggerHierarchy.from_frame(frame,
                                                             "DSTTriggers")
            if frame.Has("TriggerHierarchy"):
                icetray.logging.log_error("ERROR: PhysicsCopyTriggers: triggers in frame")
            else:
                frame.Put("TriggerHierarchy", myth)
        else:
            icetray.logging.log_error("Error: PhysicsCopyTriggers: Missing QTriggerHierarchy")
        self.PushFrame(frame)

# Null split and IceTop split need the triggerHierarchy put back in P frame

tray.AddModule(PhysicsCopyTriggers, "_IceTopTrigCopy",
               If=which_split(split_name="IceTopSplit"))

tray.AddModule(PhysicsCopyTriggers, "_nullTrigCopy",
               If=which_split(split_name="NullSplit"))


#tray.Add(online_basic_recos, 'polereco_dst')
#tray.Add(example_filter, 'example_filt')

# Select onlineL2 events only..
def select_filters_23(frame):
    if "OnlineFilterMask" in frame:
        fm = frame['OnlineFilterMask']
        keep = fm['OnlineL2Filter_23'].prescale_passed
        #keep = fm['GFUFilter_23'].prescale_passed
        return keep
    else:
        return False

def select_filters_22(frame):
    if "QFilterMask" in frame:
        fm = frame['QFilterMask']
        keep = fm['OnlineL2Filter_17'].prescale_passed
        #keep = fm['GFUFilter_17'].prescale_passed
        return keep
    else:
        return False

# include the filer selector

#tray.Add(select_filters_23,'filter_selector')

#tray.Add(select_filters_22,'filter_selector')

def drop_nulls(frame):
    header = frame['I3EventHeader']
    if header.sub_event_stream == 'NullSplit':
        return False
    else:
        return True

#tray.Add(drop_nulls,'drop_nulls')

my_garbage = ['QTriggerHierarchy'
              ]
#tray.Add("Delete", "final_cleanup",
#         keys=my_garbage)

# tray.Add(ChargeMonitorI3Module, "charge_histogram",
#          input_key = "InIceDSTPulses",
#          output_file_path = args.OUTPUT + ".npz")

# tray.Add(FilterRateMonitorI3Module, "filter_rates",
#          output_file = args.OUTPUT + ".txt")

# Write the physics and DAQ frames
tray.AddModule("I3Writer", "EventWriter", filename=args.OUTPUT,
               Streams=[icetray.I3Frame.Physics, icetray.I3Frame.DAQ],)
            #    DropOrphanStreams=[icetray.I3Frame.DAQ])

if args.PRETTY:
    print(tray)
    exit(0)

# tray.Execute(400)
tray.Execute()

stop_time = time.asctime()
print('Started:', start_time)
print('Ended:', stop_time)
