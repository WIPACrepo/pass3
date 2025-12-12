import argparse
import math
import json

from icecube.icetray import I3Frame
from icecube.icetray import I3Tray
from icecube.icetray import I3Units
from icecube.dataio import I3File

from os.path import expandvars
from collections import Counter

import numpy as np 

parser = argparse.ArgumentParser(
    usage='%prog [OPTIONS] FILE1 [FILE2, FILE3, ...]',
    description='Concatenate tables, optionally converting between supported table formats.')

parser.add_argument("-g",
                    "--gcdfile", 
                    type=str,
                    help='gcd to check',
                    default = '/data/ana/Calibration/SPE_Templates_v3/SPE_GCD//GCD_v3_923_NWD_50ns_3G_0.116_0.1_Pass3_FADCGainCorrected/GeoCalibDetectorStatus_v3_923_NWD_50ns_3G_0.116_0.1_Pass3_3G_FADCGainCorrected.i3.gz')
parser.add_argument("-o",
                    "--outloc",
                    type=str,
                    help='location for plots & output',
                    default = '')
parser.add_argument("-l",
                    "--label",
                    type=str,
                    help='label to distinguish names for file outputs',
                    default = '')
args=parser.parse_args()

gcd_scaled	= args.gcdfile
outloc	    = args.outloc
plot        = args.plot
label       = args.label

if label != '':
    label = "_" + label


gcd_nom = '/data/ana/Calibration/SPE_Templates_v3/SPE_GCD/GCD_v3_923_NWD_50ns_3G_0.116_0.1_Pass3/GeoCalibDetectorStatus_v3_923_NWD_50ns_3G_0.116_0.1_Pass3_3G.i3.gz' #this is the old (non-scaled) GCD to  compare to-- shouldn't need updating, but you never know.

corr_factors = []
calc_corrs = []
corr_dict = {}
calibration_nom = {}

if not outloc.endswith('/'):
    outloc = outloc + '/'

print('Outloc: '+outloc)

with open(outloc+"GCD_check_result"+label+".txt", "a") as f:
    f.write('*** Testing GCD file: {}\n'.format(gcd_scaled))
    f.write('Using Nominal GCD: {}\n'.format(gcd_nom))

file_path = '/data/ana/Calibration/MeasuringFADCGain/v3_923_NWD_50ns/AVG_FADC_bias_corrections.json' #true corrections as saved currently-- this may or may not need updating


with open(file_path, 'r') as file:
    # Load the JSON data from the file into a dictionary
    FADC_corrections = json.load(file)

def populate_cal_nom(frame):
    cal_nom = frame['I3Calibration']
    DOMs = list(cal_nom.dom_cal.keys())
    for i in range(len(DOMs)):
        key = DOMs[i]
        string = key.string
        om = key.om
        dom_label = '{},{}'.format(string,om)
        if string<87 and string >0 and om >0 and om <61:
            nom_gain = cal_nom.dom_cal[key].fadc_gain
            calibration_nom[dom_label] = {}
            calibration_nom[dom_label]['fadc_gain'] = nom_gain

def get_scaled_corrections(frame,plot=should_plot):
    
    calibration_scaled = frame['I3Calibration']
    DOMs = list(calibration_scaled.dom_cal.keys())
    
    for i in range(len(DOMs)):
        key = DOMs[i]
        string = key.string
        om = key.om
        dom_label = '{},{}'.format(string,om)
        if string<87 and string >0 and om >0 and om <61:

            corr_dict[dom_label] = {}
            corr_factor = FADC_corrections['FADC_gain_correction']['{},{}'.format(string,om)]
            scaled_gain = calibration_scaled.dom_cal[key].fadc_gain
            nom_gain = calibration_nom[dom_label]['fadc_gain']
            calc_corr = nom_gain/scaled_gain
            with open(outloc+"GCD_check_result"+label+".txt", "a") as f:
    
                if np.isnan(calc_corr):
                    f.write('***************NAN************\n')
                    f.write('scaled_gain: {}, nom_gain: {}\n'.format(scaled_gain,nom_gain))
                    
                f.write('DOM: {},{}\n'.format(string,om))
                f.write('True Correction: {}, Observed Correction: {}\n'.format(corr_factor,calc_corr))
                f.write('diff: {}\n'.format(calc_corr-corr_factor))
                f.write('-------------------\n')
            
            corr_factors.append(corr_factor)
            calc_corrs.append(calc_corr)
            corr_dict[dom_label]['corr_factor'] = corr_factor
            corr_dict[dom_label]['calc_corr'] = calc_corr
            
    truth_arr = []
    for i in range(len(corr_factors)):
        calc = calc_corrs[i]
        true = corr_factors[i]
        if not np.isnan(calc):
            calc = np.round(calc,3)
            true = np.round(true,3)
            if calc == true:
                truth_arr.append(True)
            elif calc != true:
                truth_arr.append(False)
        elif np.isnan(calc) or np.isnan(true):
            truth_arr.append('NaN')
            
    element_counts = Counter(truth_arr)
    if True in list(element_counts.keys()):
        num_true = element_counts[True]
    else:
        num_true = 0

    if False in list(element_counts.keys()):
        num_false = element_counts[False]
    else: 
        num_false = 0

    if 'NaN' in list(element_counts.keys()):
        num_NaN = element_counts['NaN']
    else:
        num_NaN = 0
        
    calc_corrs_nonan = [x for x in calc_corrs if not math.isnan(x)]

    calc_corrs_arr = np.asarray(calc_corrs)
    corr_factors_arr = np.asarray(corr_factors)

    pdiff = ((calc_corrs_arr-corr_factors_arr)/corr_factors_arr)*100
    
    with open(outloc+"GCD_check_result"+label+".txt", "a") as f:
        f.write('\n')
        f.write('Out of {} DOMs, {} correction factors match to ~0.1%, {} do not match, and {} correction factors were NaN\n'.format(len(truth_arr),num_true,num_false,num_NaN))


infiles_scaled = [gcd_scaled]
infiles_nom = [gcd_nom]
tray = I3Tray()
#tray.Add("I3Reader",FilenameList = infiles)
tray.AddModule("I3Reader", "reader",
    FilenameList=infiles_nom)
tray.Add(populate_cal_nom,
         Streams = [icetray.I3Frame.Calibration])
tray.Execute()
tray.Finish()

tray = I3Tray()
tray.AddModule("I3Reader", "reader2",
    FilenameList=infiles_scaled)
tray.Add(get_scaled_corrections,
         Streams = [icetray.I3Frame.Calibration])
tray.Execute()
tray.Finish()
