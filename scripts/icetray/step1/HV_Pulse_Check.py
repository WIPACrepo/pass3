#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py3-v4.3.0/icetray-start
#METAPROJECT /data/ana/Calibration/SPE_Templates_v3/Software/icetray_v1.15.3_new/build


import h5py
import numpy as np 
import icecube
from icecube.dataio import I3File #have to be running latest icetray, so use from my home dir
from icecube import dataclasses
from icecube import simclasses
import matplotlib.pyplot as plt
from scipy.optimize import curve_fit
import numpy as np
import tables
import argparse
from icecube import LeptonInjector
from icecube.icetray import I3Frame
import math, glob, sys, os
from os.path import expandvars
# from icecube import MuonInjector
from optparse import OptionParser
import numpy as np
from icecube import tableio, hdfwriter
from icecube.icetray import I3Tray
from icecube.dataio import I3File #have to be running latest icetray, so use from my home dir
from icecube import dataclasses
from icecube import simclasses
from icecube import icetray, dataio, dataclasses
from icecube.common_variables import direct_hits
from icecube.common_variables import hit_multiplicity
from icecube.icetray import I3Units
from icecube.common_variables import track_characteristics
from icecube import phys_services
import json

from icecube import VHESelfVeto, DomTools

#from icecube import weighting
#from icecube.weighting.weighting import from_simprod
#from icecube.weighting.fluxes import GaisserH3a
from icecube import truncated_energy
import subprocess
from collections import Counter

parser = OptionParser(usage='%prog [OPTIONS] FILE1 [FILE2, FILE3, ...]',
    description='Concatenate tables, optionally converting between supported table formats.')


parser.add_option("-i","--inloc", type="string",help='location of files (subruns) for run to check',
		default = '/data/exp/IceCube/2018/filtered/level2/0129/Run00130607/')
parser.add_option("-o","--outloc",type="string",help='location for output file',
		default = '')

options,args = parser.parse_args()
runloc	= options.inloc
outloc = options.outloc

if not runloc.endswith('/'):
    runloc = runloc + '/'

print('Run location: {}'.format(runloc))

print('Outloc: {}'.format(outloc))

run = runloc.split('/')[-2]
date = runloc.split('/')[-3]
year = runloc.split('/')[-6]

with open(outloc+"HV_check_result_"+year+"_"+date+"_"+run+".txt", "a") as f:
    f.write('*** HV Check for Run: {}\n'.format(runloc))
    f.write('\n')


def check_HV(frame):
    detector_test = frame['I3DetectorStatus']
    DOMs = list(detector_test.dom_status.keys())
    
    for i in range(len(DOMs)):
        key = DOMs[i]
        string = key.string
        om = key.om
        dom_label = '{},{}'.format(string,om)
        if string<87 and string >0 and om >0 and om <61:
            if detector_test.dom_status[DOMs[i]].pmt_hv != 0:
                HV_on.append(dom_label)
                
    return True

def check_pulses(frame):
    
    if not "I3EventHeader" in frame:
        print("Frame Failed: No I3EventHeader")
        return False
    event_id = float(str(frame["I3EventHeader"].run_id)+'.'+str(frame["I3EventHeader"].event_id))

    try:
        pulse_series 	= dataclasses.I3RecoPulseSeriesMap.from_frame(frame,'InIceDSTPulses')
        #print(pulse_series)
        #print('xxxxxxx')
        
        for om, pulses in pulse_series.items():
            #print(om[0]) #string
            #print(om[1]) #dom
            #print(pulses)
            
            for pulse in pulses:
                
                string =om[0]
                dom = om[1]
                dom_name = '{},{}'.format(string,dom)
                Qom = pulse.charge
                
                if Qom != 0:
                    if dom_name not in has_pulse:
                        has_pulse.append(dom_name)
                    n_pulses.append(1)
                
                
    except KeyError:
        print('Frame missing InIceDST')
                
    return True

run_gcd = glob.glob(runloc+'*GCD*')
run_gcd = run_gcd[0]

filelist = []
for file in sorted(glob.glob(runloc+'*Subrun*')):
    if 'IT' not in file:
        filelist.append(file)

uselist = filelist
uselist.insert(0,run_gcd)

HV_on = [] #list of DOMs that have HV on
has_pulse = [] #list of DOMs that have a pulse in the run
n_pulses = []

tray = I3Tray()
#tray.Add("I3Reader",FilenameList = infiles)
tray.AddModule("I3Reader","reader",
    FilenameList=uselist)
tray.Add(check_HV,Streams = [icetray.I3Frame.DetectorStatus])
tray.Add(check_pulses,Streams = [icetray.I3Frame.DAQ])
tray.Execute()
tray.Finish()

dom_has_pulse = []
for dom in has_pulse:
    if dom not in dom_has_pulse:
        dom_has_pulse.append(dom)

truth_arr_HV = []
for dom in HV_on:
    
    if dom in dom_has_pulse:
        truth_arr_HV.append(True)
    else:
        truth_arr_HV.append(False)

dom_list = Counter(truth_arr_HV)

for i in range(len(truth_arr_HV)):
        if truth_arr_HV[i] == False:
            with open(outloc+"HV_check_result_"+year+"_"+date+"_"+run+".txt", "a") as f:
                f.write('---------------\n')
                f.write(HV_on[i]+'\n')
