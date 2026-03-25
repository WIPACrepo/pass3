#!/usr/bin/env python3

"""
This is the modified LLH calculating script.

It calculates the LLH of a run data file given a corrected and
uncorrected template, and also plots the data and templates for
eye-checking. It can be used on a single file or on a folder containing
multiple files (eg. all runs in a year). The templates are currently
set up to be the same ones that Delaware used, but they can be changed
to whatever you want by changing the file paths and variable names in
the code. The LLH is calculated using a Poisson likelihood in each bin
of the 2D histogram of ATWD mean vs FADC mean, summing over all bins
where the expectation is nonzero. The script also calculates the
Pearson correlation coefficient and standard deviation of the ATWD and
FADC means for the data file, which can be useful for understanding how
well the data matches the templates.
"""

# Switched to argparse because it is the modern standard.
# Optparse is deprecated
import argparse
import json

import numpy as np
from scipy import stats
from scipy.stats import poisson
from pathlib import Path

def check_input_file(file: Path):
    """
    Check that the input file is a .npz file containing the required
    arrays 'atwd_mean' and 'fadc_mean'. This is just a helper function
    to keep the code cleaner.

    - file: location of the run data file to use, should be a .npz file
    containing 'atwd_mean' and 'fadc_mean' arrays for the data, which
    will be histogrammed in the same way as the templates. The output
    will be two flattened arrays of the ATWD and FADC means for each DOM
    in the data file.
    """
    try:
        if not file.is_file():
            raise ValueError(
                f"The file {file} does not exist. Please check the file path and try again."
            )
        if file.suffix != ".npz":
            raise ValueError(
                f"The file {file} is not a .npz file. Please provide a .npz file and try again."
            )

        try:
            with np.load(file) as data:
                if "atwd_mean" not in data or "fadc_mean" not in data:
                    raise ValueError(
                        f'The file {file} does not contain the required arrays "atwd_mean" and "fadc_mean". Please check the file and try again.'
                    )
        except Exception as e:
            raise ValueError(f"Error occurred while loading the file {file}: {e}")

        return True
    except Exception as e:
        print(f"Input file error: {e}")
        return False

def load_mean_data(data_file: Path):
    """
    Load the mean data from a data file, and return the flattened ATWD and FADC mean arrays. This is just a helper function to keep the code cleaner.

    - data_file: location of the run data file to use, should be a .npz file containing 'atwd_mean' and 'fadc_mean' arrays for the data, which will be histogrammed in the same way as the templates. The output will be two flattened arrays of the ATWD and FADC means for each DOM in the data file.
    """
    try:
        data = np.load(data_file)
    except Exception as e:
        raise ValueError(f"Error occurred while loading the file {data_file}: {e}")

    flat_atwd_mean_data = data["atwd_mean"].flatten()
    flat_fadc_mean_data = data["fadc_mean"].flatten()

    return flat_atwd_mean_data, flat_fadc_mean_data

def create_mean_charge_hist(
    data_file: Path,
    nbins: int = 40,
    lower_bound: float = 0.8,
    upper_bound: float = 1.2,
    density=False,
):
    """
    Create the histogram from a file, and load it in the same format as the data file histograms (so that they can be compared in the LLH calculation). The template file should be a .npz file containing 'atwd_mean' and 'fadc_mean' arrays for the template, which will be histogrammed in the same way as the data files. The output will be a 2D histogram of counts (with option to make it the density) with the same binning as the data files, which can then be used in the LLH calculation.

    - data_file: location of the run data file to use, should be a .npz file containing 'atwd_mean' and 'fadc_mean' arrays for the data, which will be histogrammed in the same way as the templates. The output will be two flattened arrays of the ATWD and FADC means for each DOM in the data file.
    - nbins: number of bins to use for the histogramming (should be the same as the templates)
    - lower_bound: lower bound for the histogramming (should be the same as the templates)
    - upper_bound: upper bound for the histogramming (should be the same as the templates)
    - density: whether to return the density (normalized) histogram or the counts
    """

    atwd, fadc = load_mean_data(data_file)

    counts_template, xedges, yedges = np.histogram2d(
        atwd,
        fadc,
        bins=nbins,
        range=[[lower_bound, upper_bound], [lower_bound, upper_bound]],
        density=density,
    )

    return counts_template

def calc_llh(
    data_file: Path,
    corr_template: np.ndarray,
    uncorr_template: np.ndarray,
    nbins: int = 40,
    lower_bound: float = 0.8,
    upper_bound: float = 1.2,
):
    """
    Calculate the log likelihood for a run data file, testing against both the corrected and uncorrected hypotheses.

    - data_file: location of the run data file to use
    - corr_template: the corrected template histogram to use. Should be kept in counts, not using density = True. Needs to be binned the same as data_file (I just use nbins here)
    - uncorr_template: same as above but for the uncorrected template histogram
    - nbins: bins for the histogramming of the data file
    - plot: Will plot the templates and the data file in case it is useful to look at (eg. for outlier LLHs)

    if plot = True, a plot with the templates and data file will be saved, with a stats box showing the LLH info and correlation coeff and STD of ATWD and FADC means of the data.
    Returns: (LLH with corrected template, LLH with uncorrected template)

    """

    print(f"Data File: {data_file}")
    # # get year and run number for label stuff
    run_num = data_file.parts[-2]
    year = data_file.parts[-3]

    print(f"Data File: {year}, Run {run_num}")

    flat_atwd_mean_data_test, flat_fadc_mean_data_test = load_mean_data(data_file)

    # histogram the data file with same binning as templates
    counts_test_data = create_mean_charge_hist(
        data_file,
        nbins=nbins,
        lower_bound=lower_bound,
        upper_bound=upper_bound,
        density=False,
    )
    counts_test_data_masked = np.ma.masked_where(
        counts_test_data == 0, counts_test_data
    )

    flat_counts_test_data = counts_test_data.flatten()
    flat_counts_corr = corr_template.flatten()
    flat_counts_uncorr = uncorr_template.flatten()

    # we only want to check bins where the expectation is nonzero
    mask_corr = flat_counts_corr > 0
    mask_uncorr = flat_counts_uncorr > 0

    masked_corr = flat_counts_corr[mask_corr]
    masked_counts_test_data_corr = flat_counts_test_data[mask_corr]

    masked_uncorr = flat_counts_uncorr[mask_uncorr]
    masked_counts_test_data_uncorr = flat_counts_test_data[mask_uncorr]

    logL_corr = 0
    logL_uncorr = 0

    for i in range(len(masked_corr)):  # calculate the LLH in each bin and sum
        if masked_counts_test_data_corr[i] >= 0:
            pois_corr = poisson.pmf(masked_counts_test_data_corr[i], masked_corr[i])
            logL_corr += np.log(pois_corr)

    for j in range(len(masked_uncorr)):
        if masked_counts_test_data_uncorr[j] >= 0:
            pois_uncorr = poisson.pmf(
                masked_counts_test_data_uncorr[j], masked_uncorr[j]
            )
            logL_uncorr += np.log(pois_uncorr)

    print(f"Data Counts: {int(np.sum(counts_test_data))}")

    print(f"logL_corrected: {logL_corr:.3f}")
    print(f"logL_unorrected: {logL_uncorr:.3f}")
    print(f"Delta_LLH: {(logL_corr - logL_uncorr):.3f}")

    # take the STD and correlation coefficient only for stuf in the same range as the LLH was calculated in ([lower_bound,upper_bound])
    condition1 = (
        flat_atwd_mean_data_test > lower_bound
    )  
    condition2 = flat_atwd_mean_data_test < upper_bound
    condition3 = flat_fadc_mean_data_test > lower_bound
    condition4 = flat_fadc_mean_data_test < upper_bound

    mask = condition1 & condition2 & condition3 & condition4

    flat_atwd_mean_data_test_masked = flat_atwd_mean_data_test[mask]
    flat_fadc_mean_data_test_masked = flat_fadc_mean_data_test[mask]

    pearson_r = stats.pearsonr(
        flat_atwd_mean_data_test_masked, flat_fadc_mean_data_test_masked
    )[0]

    std_atwd = np.std(flat_atwd_mean_data_test_masked)
    std_fadc = np.std(flat_fadc_mean_data_test_masked)

    print(f"Pearson correlation coefficient: {pearson_r:.3f}")
    print(f"ATWD Standard Deviation: {std_atwd:.3f}")
    print(f"FADC Standard Deviation: {std_fadc:.3f}")

    return {
        "logL_corr": logL_corr,
        "logL_uncorr": logL_uncorr,
        "delta_logL": logL_corr - logL_uncorr,
        "pearson_r": pearson_r,
        "std_atwd": std_atwd,
        "std_fadc": std_fadc
    }


parser = argparse.ArgumentParser(description="Calculate charge peak LLH")

parser.add_argument(
    "-i",
    "--inloc",
    type=Path,
    help="if a file, .npz file containing the ATWD and FADC mean information for a DOM. If a folder, a folder containing .npz files at some level.",
    required=True,
)
parser.add_argument(
    "-n",
    "--nbins",
    type=int,
    help="number of bins to use for the histogramming of the data file (should be the same as the templates). Default is 40.",
    default=40,
)
parser.add_argument(
    "--lower_bound",
    type=float,
    help="lower bound for the histogramming of the data file (should be the same as the templates). Default is 0.8.",
    default=0.8,
)
parser.add_argument(
    "--upper_bound",
    type=float,
    help="upper bound for the histogramming of the data file (should be the same as the templates). Default is 1.2.",
    default=1.2,
)
parser.add_argument(
    "--template_corrected",
    type=Path,
    help="location of the corrected template .npz file.",
    required=True,
)
parser.add_argument(
    "--template_uncorrected",
    type=Path,
    help="location of the uncorrected template .npz file.",
    required=True,
)

args = parser.parse_args()

# TODO: needs to be corrected for using pathlib
inloc = args.inloc

# setup the templates for corrected and uncorrected
counts_corr = create_mean_charge_hist(
    args.template_corrected,
    nbins=args.nbins,
    lower_bound=args.lower_bound,
    upper_bound=args.upper_bound,
    density=False,
)
counts_uncorr = create_mean_charge_hist(
    args.template_uncorrected,
    nbins=args.nbins,
    lower_bound=args.lower_bound,
    upper_bound=args.upper_bound,
    density=False,
)

if args.inloc.is_file():
    # if you input a single file, it should just do that file
    if check_input_file(args.inloc):
        print("Single File")
        use_files = [args.inloc]
elif args.inloc.is_dir():
    # if you input a folder, it should be a year, and do the LLH calculation for each .npz file inside that folder (looking recursively at deeper folders...).
    print("Folder")
    # get all .npz files in the folder and subfolders, and check that they are valid before trying to use them
    use_files = [file for file in args.inloc.rglob("*.npz") if check_input_file(file)]
else:
    print(
        "Something went wrong with the file type to use!! Check that you are using a single .npz file or a folder containing runs with .npz files."
    )
    use_files = []

for data_file in use_files:
    try:
        llhs = calc_llh(
            data_file,
            counts_corr,
            counts_uncorr,
            args.nbins,
            lower_bound=args.lower_bound,
            upper_bound=args.upper_bound,
        )
    except Exception as e:
        print(f"File error with file: {data_file}")
        print(f"Error: {e}")

    llhs["data_file"] = str(data_file)
    llhs["corrected_template_file"] = str(args.template_corrected)
    llhs["uncorrected_template_file"] = str(args.template_uncorrected)

    with open(data_file.parent / (f"{data_file.stem}_comparison_results.json"), "w") as f:
        json.dump(llhs, f, indent=4)