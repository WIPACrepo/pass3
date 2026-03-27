import sys
import os
from os.path import exists, expandvars, join
from glob import glob
import time

from argparse import ArgumentParser

# Locate the test script that we need to produce the charge histograms

parser = ArgumentParser()
parser.add_argument("-i", "--indirs",
                    type=str, nargs="+",
                    help = ("Pass3 step1 folder containing all days. This will be used to build up the"
                            " list of runs to process."))
parser.add_argument("-g", "--gcddirs",
                    type=str, nargs="+",
                    help = "Location of pass3 GCDs for each directory specified by --indir.")
parser.add_argument("--dagman", 
                    type=str, default="pass3_step1_fadc_checks.dag",
                    help = "Name of the dagman to write.")
parser.add_argument("-l", "--logdir",
                    type=str, default=".",
                    help = ("The top-level directory to place output log files. All log files will end"
                            " up in this directory."))
parser.add_argument("-o", "--outdir",
                    type=str, default=".",
                    help = ("The top-level directory to place output npz files. The output directory"
                            " structure will look like {outdir}/{calendar year}/{run}/Run{run}.fadc_atwd_charge.npz"))
parser.add_argument("--histogramming-script",
                    type=str, default=expandvars("$PWD/../../icetray/step1/pass3_check_charge_filter_numba.py"),
                    help = ("The script to run for each run. This should be a script that takes the same arguments as"
                            " pass3_check_charge_filter_numba.py and produces the expected output npz files."))
parser.add_argument("--test-script",
                    type=str, default=expandvars("$PWD/../../checks/step1/mlarson_numba/scripts/checks/step1/atwd_fadc_charge_peaks calculate_charge_peak_llh.py"),
                    help = ("The script to run for each run. This should be a script that takes the same arguments as pass3_check_charge_filter_numba.py and produces the expected output npz files."))
parser.add_argument("--filter-rates", action="store_true", default=False,
                    help = ("Whether to include the filter rate calculation in the histogramming script. This will run the FilterRateMonitorI3Module in the histogramming script and output a text file with filter rates for each run."))
parser.add_argument("--llh-test", action="store_true", default=False,
                    help = ("Whether to include the charge peak llh test in the dag. This will run the script specified by --test-script on the output of the histogramming script and compare to templates."))
parser.add_argument("--llh-template-uncorrected",
                    type=str, default="/data/ana/Calibration/Pass3_Monitoring/online/icetray/scripts/histograms/Run140950.fadc_atwd_charge.npz",
                    help = ("The uncorrected ahistogram templates to use for the llh test. These will be passed to the test script specified by --test-script."))
parser.add_argument("--llh-template-corrected",
                    type=str, default="/data/ana/Calibration/Pass3_Monitoring/online/icetray/scripts/histograms/Run136692.fadc_atwd_charge.npz",
                    help = ("The corrected ahistogram templates to use for the llh test. These will be passed to the test script specified by --test-script."))
args = parser.parse_args()

#--------------------------------------------------
# Make some assumptions about the subrun 0 filename
# in order to identify the appropriate GCD from the
# given GCD file directories.
#--------------------------------------------------
def get_gcd(subrun_filename):
    if "Run00" not in subrun_filename:
        return None
    run = subrun_filename[subrun_filename.rindex("Run00")+len("Run00"):][:6]
    gcd_options = [glob(join(gcddir, f"*Run00{run}*GCD.i3.zst")) for gcddir in args.gcddirs]
    for paths in gcd_options:
        if len(paths) > 0:
            return paths[0]
    raise FileNotFoundError(f"No GCD file found for subrun file {subrun_filename}"
                            f" in GCD file directories {args.gcddirs}.")

#--------------------------------------------------
# Find the first subrun from each run in the given
# paths. Note that if we don't have a subrun 0 file,
# that run will be skipped here.
#--------------------------------------------------
start = time.time()
indirs = []
for path in args.indirs:
    first_subruns = glob(join(path, "*/*00000000.i3.zst"))
    if len(first_subruns) > 0:
        indirs += first_subruns
if len(indirs) == 0:
    print("No runs with subrun 0 found in provided indirs.")
    sys.exit()
print("Identifying runs took", time.time() - start, "seconds")

if not exists(args.outdir):
    mkdir(args.outdir)

#--------------------------------------------------
# Begin building the dag for NPX
#--------------------------------------------------
dag = ""

# Shared submit description
dag += "SUBMIT-DESCRIPTION online_checks_data {\n"
dag += f"executable   = {args.histogramming_script}\n"
dag += "arguments    = \"$(args)\"\n"
dag += f"output       = {args.logdir}/$(run).out\n"
dag += f"error        = {args.logdir}/$(run).err\n"
dag += "log          = /dev/null\n"
dag += "request_cpus   = 1\n"
dag += "request_memory = 2G\n"
dag += "request_disk   = 8G\n"
dag += "}\n"

if args.llh_test:
    # Shared submit description
    dag += "SUBMIT-DESCRIPTION charge_llh_checks {\n"
    dag += f"executable   = {args.test_script}\n"
    dag += "arguments    = \"$(args)\"\n"
    dag += f"output       = {args.logdir}/$(run)_llh.out\n"
    dag += f"error        = {args.logdir}/$(run)_llh.err\n"
    dag += "log          = /dev/null\n"
    dag += "request_cpus   = 1\n"
    dag += "request_memory = 2G\n"
    dag += "request_disk   = 8G\n"
    dag += "}\n"

# Loop over the identified subrun 0 files to build the dag
# Here, I assume a directory and file name structure to get
# the year and run numbers.
for path in indirs:
    run = int(path.split("Run")[1][:8])
    year = path.split("/")[4]
    gcd = get_gcd(path)

    # Find all files in this run
    filenames = sorted(glob(path.replace("00000000.i3.zst", "*.i3.zst")))

    # Build the output directory structure as needed
    year_outdir = join(args.outdir, year)
    current_outdir = join(year_outdir, str(run))
    if not exists(current_outdir):
        os.makedirs(current_outdir, exist_ok=True)

    # Map out the dag executable call and args
    cmd = f"JOB online_checks_data_{run} online_checks_data DIR {current_outdir}\n"
    cmd += f"VARS online_checks_data_{run}"
    cmd += f" run=\"{run}\""
    cmd += f" args=\""
    cmd += f" -o {current_outdir}/Run{run}"
    cmd += f" -g {gcd}"
    if args.filter_rates:
        cmd += " --filter-rates"
    cmd += f" --infiles"
    for f in sorted(filenames):
        cmd += f" {f}"
    cmd += "\""
    cmd += "\n"

    if args.llh_test:
        cmd += f"JOB online_checks_llh_{run} charge_llh_checks DIR {current_outdir}\n"
        cmd += f"VARS online_checks_llh_{run}"
        cmd += f" run=\"{run}\""
        cmd += f" args=\""
        cmd += f" --template_uncorrected {args.llh_template_uncorrected}"
        cmd += f" --template_corrected {args.llh_template_corrected}"
        cmd += f" -i {current_outdir}/Run{run}.fadc_atwd_charge.npz"  
        cmd += "\""
        cmd += "\n"

        cmd += f"PARENT online_checks_data_{run} CHILD online_checks_llh_{run}\n"


    # Write the commands for this run to the dag file
    dag += cmd

open(args.dagman, "w").writelines(dag)
