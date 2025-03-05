import argparse
import subprocess
import hashlib
import time
import datetime
import sys
import os
import multiprocessing
import zipfile
import shutil
import tempfile

from pathlib import Path, PosixPath
import concurrent.futures
from typing import Union

def remove_extension(path: Path) -> Path:
    """Remove multiple suffixes from filename"""
    suffixes = ''.join(path.suffixes)
    return Path(str(path).replace(suffixes, ''))

def get_gcd(infile: Path, gcddir: Path) -> Path:
    if not gcddir.exists():
        raise Exception("No GCD dir")
    # Assuming Format of infile name is:
    # PFRaw_PhysicsFiltering_Run<RunNumber>_Subrun<SubRunNumber>_<FileNumber>.tar.gz
    runnum = int(infile.split('_')[2][2:])
    gcdfiles = gcddir.glob(f"*{runnum}*")
    if len(gcdfiles) > 1:
        raise Exception(f"Multiple GCD Files {gcdfiles}")
    else:
        return gcdfiles[0]

def get_outfilename(infile: Path) -> Path:
    # Assuming Format of infile name is:
    # PFRaw_PhysicsFiltering_Run<RunNumber>_Subrun<SubRunNumber>_<FileNumber>.tar.gz
    infilename = str(remove_extension(infile))
    infilenwords = infilename.split('_')
    outfilewords = ["Pass3", "Step1"] + infilenwords[1:]
    return "_".join(outfilewords) + ".i3.zst"

def get_logfilenames(infile: Path, outdir: Path) -> tuple[Path, Path]:
    # Assuming Format of infile name is:
    # PFRaw_PhysicsFiltering_Run<RunNumber>_Subrun<SubRunNumber>_<FileNumber>.tar.gz
    infilename = str(remove_extension(infile))
    infilenwords = infilename.split('_')
    stdoutfilename = "_".join([
        "LOG", "OUT","Pass3", "Step1"] + infilenwords[1:]) + ".out"
    stderrfilename = "_".join(
        ["LOG", "ERR","Pass3", "Step1"] + infilenwords[1:]) + ".err"
    return outdir / stdoutfilename, outdir / stderrfilename

def generate_command(infile: Path, gcd: Path, outfile: Path) -> str:
    # TODO: Figure out if we can load the eval and env-shell in the container
    envshell_loc = "/cvmfs/icecube.opensciencegrid.org/py3-v4.4.0/RHEL_9_x86_64_v2/metaprojects/icetray/v1.13.0/bin/icetray-shell"
    scriptloc = os.environ['I3_BUILD'] + "offline_filterscripts/resources/scripts/pass3_reprocess_PFRaw.py"
    command = envshell_loc + " python3 " + scriptloc + f" -i {infile} -g {gcd} -o {outfile} --qify" 
    return command

def generate_moni_command(infile: Path, gcd: Path, outfile: Path) -> str:
    envshell_loc = "/cvmfs/icecube.opensciencegrid.org/py3-v4.4.0/RHEL_9_x86_64_v2/metaprojects/icetray/v1.13.0/bin/icetray-shell"
    scriptloc = os.environ['I3_BUILD'] + "offline_filterscripts/resources/scripts/pass3_check_charge_filter.py"
    command = envshell_loc + " python3 " + scriptloc + f" -i {infile} -g {gcd} -o {outfile}"
    return command

# Taken from LTA
# Adapted from: https://stackoverflow.com/a/44873382
def get_sha512sum(filename: Union[str, Path]) -> str:
    """Compute the SHA512 hash of the data in the specified file."""
    h = hashlib.sha512()
    b = bytearray(128 * 1024)
    mv = memoryview(b)
    with open(filename, 'rb', buffering=0) as f:
        # Known issue with MyPy: https://github.com/python/typeshed/issues/2166
        for n in iter(lambda: f.readinto(mv), 0):
            h.update(mv[:n])
    return h.hexdigest()

def runner(infiles: tuple[Path, Path, Path, Path]):
    # Creating a dir on the local disk
    tmpdir = Path(tempfile.mkdtemp(dir="/tmp"))

    # Getting file and file paths
    gcddir = infiles[0]
    bundle = infiles[1]
    infile = infiles[2]
    outdir = infiles[3]

    gcd = get_gcd(infile, gcddir)
    if not gcd.exists():
        raise FileNotFoundError("No GCD")

    # Prepping files and file paths
    ## Extracting in file from bundle
    zipfile.ZipFile(bundle).extract(infile, path=tmpdir)
    local_infile = tmpdir / infile
    if not local_infile.exists():
        raise FileNotFoundError("No Input File")

    outfilename = get_outfilename(infile, outdir)
    local_temp_outfile = tmpdir / "tmp_" + outfilename
    local_outfile = tmpdir / outfilename
    outfile = outdir / outfilename

    if outfile.exists():
        raise Exception("Output file already exists")

    local_stdout_file, local_stderr_file = get_logfilenames(infile,
                                                            tmpdir)
    stdout_file, stderr_file = get_logfilenames(infile,
                                                outdir)

    command = generate_command(local_infile, gcd, local_temp_outfile)
    moni_command = generate_moni_command(local_temp_outfile, gcd, local_outfile)

    # run step 1
    # We are first running the online processing that is the same as done 
    # at the south pole. we then read the file back in, rehydrate it, and 
    # run some moni code on it to make sure we are doing the right thing.
    with open(local_stdout_file, "w") as stdout, open(local_stderr_file, "w") as stderr:
        # TODO: Do we need to check the GCD file?
        stdout.write(
                f"Start Time: {datetime.datetime.now(datetime.timezone.utc)}\n")
        stdout.write(f"Hostname: {os.environ['HOSTNAME']}")
        try:
            subprocess.run(command, shell=True, stdout=stdout, stderr=stderr)
        except:
            raise Exception(
                f"ALERT: {infile} in {bundle} has failed to process")
        stdout.write(
                f"End Time PFRAW: {datetime.datetime.now(datetime.timezone.utc)}\n")
        try:
            subprocess.run(
                moni_command, shell=True, stdout=stdout, stderr=stderr)
        except:
            raise Exception(
                f"ALERT: {infile} in {bundle} has failed during moni")
        stdout.write(
                f"End Time: {datetime.datetime.now(datetime.timezone.utc)}\n")

    # create checksum of output file
    sha512sum = get_sha512sum(local_outfile)
    print(f"file: {local_outfile} sha512sum: {sha512sum}")
    with open(outfile + ".sha512sum", "w"):
        file.write(f"{sha512sum}")

    # Copying from local dir to absolute dir
    shutil.copyfile(local_stdout_file, stdout_file)
    shutil.copyfile(local_stderr_file, stderr_file)
    shutil.copyfile(local_outfile, outfile)
    # TODO: move charge and filter rate file
    shutil.copyfile(local_outfile + ".npz", outfile + ".npz",)
    shutil.copyfile(local_outfile + ".txt", outfile + ".txt")

    shutil.rmtree(tmpdir)

def run_parallel(infiles, max_num=1):
    with concurrent.futures.ProcessPoolExecutor(
        max_workers = max_num) as executor:
        futures = executor.map(runner, infiles)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--gcddir', 
                        help="GCD File", 
                        type=Path, 
                        required=True)
    parser.add_argument("--bundle", 
                        help="path to bundle on ranch", 
                        type=Path,
                        required=True)
    parser.add_argument("--outdir", 
                        help="",
                        type=Path,
                        required=True)
    parser.add_argument("--checksum",
                        help="bundle sha512sum",
                        type=str,
                        required=True)
    parser.add_argument("--maxnumcpus", 
                        help="",
                        type=int,
                        default=0)
    args=parser.parse_args()

    if args.maxnumcpus == 0:
        numcpus = multiprocessing.cpu_count() 
    else:
        numcpus = args.maxnumcpus   
    
    if not args.outdir.exists():
        Path(args.outdir).mkdir(parents=True, exist_ok=True)

    if not (args.outdir / args.bundle.name).exists():
        subprocess.run(f"scp {os.environ['$ARCHIVER']}:{args.bundle} {args.outdir}")
        bundle_sha512sum = get_sha512sum(args.outdir / args.bundle.name)
        if bundle_sha512sum != args.checksum:
            raise Exception(f"Bundle {args.bundle} checksum is not the same")

    scratch_bundle_loc = args.outdir / args.bundle.name

    infiles = [f for f in zipfile.ZipFile(scratch_bundle_loc).namelist() 
               if ".tar.gz" in f]
    
    if len(infiles) == 0:
        raise FileNotFoundError(
            f"No input files found in bundle {args.bundle}")
    
    inputs = [
        (Path(args.gcddir), 
         Path(scratch_bundle_loc), 
         Path(f), 
         Path(args.outdir)) 
         for f in infiles]

    print(inputs)
         
    # run_parallel(inputs, numcpus)

    # TODO: Delete bundle
    # shutil.rmtree(args.bundle)

