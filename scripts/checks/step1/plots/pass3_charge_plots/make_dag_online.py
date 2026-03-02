import sys
from os import mkdir, stat, system
from os.path import basename, dirname, exists, expandvars, join
from glob import glob
import time

from argparse import ArgumentParser

# Locate the test script that we need to produce the charge histograms
test_script = expandvars("$PWD/../../icetray/step1/pass3_check_charge_filter_numba.py")

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
dag += "SUBMIT-DESCRIPTION online_checks {\n"
dag += f"executable   = {test_script}\n"
dag += "arguments    = \"$(args)\"\n"
dag += f"output       = {args.logdir}/$(run).out\n"
dag += f"error        = {args.logdir}/$(run).err\n"
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
        if not exists(year_outdir):
            mkdir(year_outdir)
        mkdir(current_outdir)

    # Map out the dag executable call and args
    cmd = f"JOB online_checks_{run} online_checks DIR {current_outdir}\n"
    cmd += f"VARS online_checks_{run}"
    cmd += f" args=\""
    cmd += f" -o {current_outdir}/Run{run}"
    cmd += f" -g {gcd}"
    cmd += f" --infiles"
    for f in sorted(filenames):
        cmd += f" {f}"
    cmd += "\""

    # Write the commands for this run to the dag file
    dag += cmd

open(args.dagman, "w").writelines(dag)
