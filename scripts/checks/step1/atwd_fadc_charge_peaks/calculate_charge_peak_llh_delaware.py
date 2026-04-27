#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py3-v4.3.0/icetray-start
#METAPROJECT /data/ana/Calibration/SPE_Templates_v3/Software/icetray_v1.15.3_new/build

"""
This is the original LLH calculating script provided by Delaware for checking the FADC gain correction. It calculates the LLH of a run data file given a corrected and uncorrected template, and also plots the data and templates for eye-checking. It can be used on a single file or on a folder containing multiple files (eg. all runs in a year). The templates are currently set up to be the same ones that Delaware used, but they can be changed to whatever you want by changing the file paths and variable names in the code. The LLH is calculated using a Poisson likelihood in each bin of the 2D histogram of ATWD mean vs FADC mean, summing over all bins where the expectation is nonzero. The script also calculates the Pearson correlation coefficient and standard deviation of the ATWD and FADC means for the data file, which can be useful for understanding how well the data matches the templates.
"""

import numpy as np
from matplotlib import pyplot as plt
from matplotlib import colors
from scipy import stats
from scipy.stats import poisson
import glob
from optparse import OptionParser

parser = OptionParser(usage='%prog [OPTIONS] FILE1 [FILE2, FILE3, ...]')

parser.add_option("-i","--inloc", type="string",help='if a file, .npz file containing the ATWD and FADC mean information for a DOM. If a folder, a folder containing .npz files at some level.',
		default = '/data/ana/Calibration/Pass3_Monitoring/online/icetray/scripts/histograms/2021/134874/Run134874.fadc_atwd_charge.npz')

parser.add_option("-o","--outloc", type="string",help='location to save files and plots',
		default = '/data/user/mgarcia/Results/Calibration/FADC_Gain_Correction_LLH/')

options,args = parser.parse_args()
inloc   = options.inloc
outloc  = options.outloc

if not inloc.endswith('.npz') and not inloc.endswith('/'):
    inloc = inloc+'/'

if not outloc.endswith('/'):
    outloc = outloc + '/'

def calc_llh(data_file,corr_template,uncorr_template,nbins,plot=False):
    
    '''
    Calculate the log likelihood for a run data file, testing against both the corrected and uncorrected hypotheses.
    
    - data_file: location of the run data file to use
    - corr_template: the corrected template histogram to use. Should be kept in counts, not using density = True. Needs to be binned the same as data_file (I just use nbins here)
    - uncorr_template: same as above but for the uncorrected template histogram
    - nbins: bins for the histogramming of the data file
    - plot: Will plot the templates and the data file in case it is useful to look at (eg. for outlier LLHs)
    
    if plot = True, a plot with the templates and data file will be saved, with a stats box showing the LLH info and correlation coeff and STD of ATWD and FADC means of the data.
    Returns: (LLH with corrected template, LLH with uncorrected template)
    
    '''
    
    print(f'Data File: {data_file}')
    run_num = data_file.split('.')[0].split('/')[-1].split('Run')[1] #get year and run number for label stuff
    year = data_file.split('/')[-3]
    
    print(f'Data File: {year}, Run {run_num}')
    
    data_test = np.load(data_file)
    
    flat_atwd_mean_data_test = data_test['atwd_mean'].flatten()
    flat_fadc_mean_data_test = data_test['fadc_mean'].flatten()
    
    counts_test_data,xedges,yedges = np.histogram2d(flat_atwd_mean_data_test,flat_fadc_mean_data_test,bins=nbins,range=[[0.8,1.2],[0.8,1.2]],density=False) #histogram the data file with same binning as templates
    counts_test_data_masked = np.ma.masked_where(counts_test_data == 0, counts_test_data)
    
    flat_counts_test_data = counts_test_data.flatten()
    flat_counts_corr = corr_template.flatten()
    flat_counts_uncorr = uncorr_template.flatten()
    
    mask_corr = (flat_counts_corr > 0) #we only want to check bins where the expectation is nonzero
    mask_uncorr = (flat_counts_uncorr > 0)
    
    masked_corr = flat_counts_corr[mask_corr]
    masked_counts_test_data_corr = flat_counts_test_data[mask_corr]

    masked_uncorr = flat_counts_uncorr[mask_uncorr]
    masked_counts_test_data_uncorr = flat_counts_test_data[mask_uncorr]
    
    logL_corr = 0
    logL_uncorr = 0


    for i in range(len(masked_corr)): #calculate the LLH in each bin and sum
        if masked_counts_test_data_corr[i] >=0:
            pois_corr = poisson.pmf(masked_counts_test_data_corr[i],masked_corr[i])
            logL_corr += np.log(pois_corr)

    for j in range(len(masked_uncorr)):
        if masked_counts_test_data_uncorr[j] >=0:
            pois_uncorr = poisson.pmf(masked_counts_test_data_uncorr[j],masked_uncorr[j])
            logL_uncorr += np.log(pois_uncorr)
            
    print(f'Data Counts: {str(int(np.sum(counts_test_data)))}')
            
    print(f'logL_corrected: {logL_corr:.3f}')
    print(f'logL_unorrected: {logL_uncorr:.3f}')
    print(f'Delta_LLH: {(logL_corr-logL_uncorr):.3f}')
    
    lower_bound = 0.8
    upper_bound = 1.2
    
    condition1 = (flat_atwd_mean_data_test > lower_bound) #take the STD and correlation coefficient only for stuf in the same range as the LLH was calculated in ([0.8,1.2])
    condition2 = (flat_atwd_mean_data_test < upper_bound)
    condition3 = (flat_fadc_mean_data_test > lower_bound)
    condition4 = (flat_fadc_mean_data_test < upper_bound)
    
    mask = condition1 & condition2 & condition3 & condition4
    
    flat_atwd_mean_data_test_masked = flat_atwd_mean_data_test[mask]
    flat_fadc_mean_data_test_masked = flat_fadc_mean_data_test[mask]
    
    pearson_r = stats.pearsonr(flat_atwd_mean_data_test_masked,flat_fadc_mean_data_test_masked)[0]
    
    std_atwd = np.std(flat_atwd_mean_data_test_masked)
    std_fadc = np.std(flat_fadc_mean_data_test_masked)
    
    print(f'Pearson correlation coefficient: {pearson_r:.3f}')
    print(f'ATWD Standard Deviation: {std_atwd:.3f}')
    print(f'FADC Standard Deviation: {std_fadc:.3f}')
    
    
    if plot==True: #plot the (ATWD,FADC) points for templates and the data file (useful to eye-check if something is weird-looking), and report the LLHs, STD, and correlation coefficient
        ### Plot the FADC and ATwd
        omkey = (21, 42)

        fig, ax = plt.subplots(figsize=(8,6))

        ic2022 = np.load("/data/ana/Calibration/Pass3_Monitoring/online/icetray/scripts/histograms/Run136692.fadc_atwd_charge.npz")
        ax.scatter(ic2022['atwd_mean'], ic2022['fadc_mean'],
                  s = 2,
                  color='b',
                   alpha=0.25)
        ax.scatter([], [],
                  s = 16,
                  color='b',
                   alpha=0.75, 
                  label = "IC2022 (Run 136692)-- Corrected Template")

        ic2025 = np.load("/data/ana/Calibration/Pass3_Monitoring/online/icetray/scripts/histograms/Run140950.fadc_atwd_charge.npz")
        ax.scatter(ic2025['atwd_mean'], ic2025['fadc_mean'],
                  s = 2,
                  color='k',
                   alpha=0.25)
        ax.scatter([], [],
                  s = 16,
                  color='k',
                   alpha=0.75, 
                  label = "IC2025 (Run 140950)-- Uncorrected Template")

        ax.scatter(data_test['atwd_mean'], data_test['fadc_mean'],
                  s = 2,
                  color='r',
                   alpha=0.25)
        ax.scatter([], [],
                  s = 16,
                  color='r',
                   alpha=0.75, 
                  label = f"Test Data: {year} Run {run_num}")

        # plot a 1:1 line
        ax.plot([0, 2], [0, 2],
                color='r',
                linewidth=2,
                alpha=0.1)

        ax.set_xlim(0.8, 1.2) #remove these to see full range, there's sometimes a few doms hidden at lower x,y and bad doms at 0,0
        ax.set_ylim(0.8, 1.2)
        ax.legend(loc='upper left', framealpha=1, fontsize=12)
        ax.set_xlabel("ATWD mean (PE)")
        ax.set_ylabel("FADC mean (PE)")
        ax.grid(alpha=0.25)

        ax.text(
        0.99, 0.01,                      # position
        f'Data Counts: {np.sum(counts_test_data)}\nlogL_corrected: {logL_corr:.3f}\nlogL_uncorrected: {logL_uncorr:.3f}\nDelta_LLH: {(logL_corr-logL_uncorr):.3f}\nData Pearson correlation coefficient: {pearson_r:.3f}\nData ATWD Standard Deviation: {std_atwd:.3f}\nData FADC Standard Deviation: {std_fadc:.3f}',                   # text
        transform=ax.transAxes,          # use axes coordinates
        fontsize=10,
        ha = 'right',
        va = 'bottom',
        bbox=dict(boxstyle="round", facecolor="white", alpha=0.8)
        )

        plt.title('FADC vs ATWD means, all DOMs')
        plt.savefig(outloc+year+'_Run_'+run_num+'_FADC_ATWD_means.pdf') #save plot
    
    return logL_corr,logL_uncorr

#setup the templates for corrected and uncorrected
ic2022 = np.load("/data/ana/Calibration/Pass3_Monitoring/online/icetray/scripts/histograms/Run136692.fadc_atwd_charge.npz")
ic2025 = np.load("/data/ana/Calibration/Pass3_Monitoring/online/icetray/scripts/histograms/Run140950.fadc_atwd_charge.npz")


flat_atwd_mean_corr = ic2022['atwd_mean'].flatten()
flat_fadc_mean_corr = ic2022['fadc_mean'].flatten()

flat_atwd_mean_uncorr = ic2025['atwd_mean'].flatten()
flat_fadc_mean_uncorr = ic2025['fadc_mean'].flatten()

nbins = 40 #this seems to be a good binning

counts_corr_scaled,xedges,yedges = np.histogram2d(flat_atwd_mean_corr,flat_fadc_mean_corr,bins=nbins,range=[[0.8,1.2],[0.8,1.2]],density=True)
counts_uncorr_scaled,xedges,yedges = np.histogram2d(flat_atwd_mean_uncorr,flat_fadc_mean_uncorr,bins=nbins,range=[[0.8,1.2],[0.8,1.2]],density=True)

counts_corr,xedges,yedges = np.histogram2d(flat_atwd_mean_corr,flat_fadc_mean_corr,bins=nbins,range=[[0.8,1.2],[0.8,1.2]],density=False)
counts_uncorr,xedges,yedges = np.histogram2d(flat_atwd_mean_uncorr,flat_fadc_mean_uncorr,bins=nbins,range=[[0.8,1.2],[0.8,1.2]],density=False)

use_files = [] #which files to do the test for

if inloc.endswith('.npz'): #if you input a single file, it should just do that file
    print('Single File')
    use_files.append(inloc)


elif inloc.endswith('/'): #if you input a folder, it should be a year, and do the LLH calculation for each .npz file inside that folder (looking recursively at deeper folders...).
    print('Folder')
    for file in glob.glob(inloc+'**/*.npz',recursive=True):
        use_files.append(file)

else:
    print('Something went wrong with the file type to use!! Check that you are using a single .npz file or a folder containing runs with .npz files.')

for data_file in use_files:
    try:
        calc_llh(data_file,counts_corr,counts_uncorr,40,plot=True)
    except:
        print(f'File error with file: {data_file}')