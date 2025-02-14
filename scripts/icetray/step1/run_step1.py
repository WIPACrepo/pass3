import argparse
import subprocess
import concurrent.futures
import hashlib
import time
import datetime
import sys
import os

from pathlib import Path, PosixPath

def runner(infiles: tuple[Path, Path]):
    # run step 1
    gcd = infiles[0]
    infile = infiles[1]
    outdir = infiles[2]
    if not gcd.exists():
        print("No GCD")
        sys.exit(1)
    if not infile.exists():
        print("No Input File")
        sys.exit(1)
    outfile = get_outfilename(infile, outdir)
    if outfile.exists():
        print("Output file already exists")
        sys.exit(1)
    command = generate_command(infile, gcd, outfile)
    stdout_file, stderr_file = get_logfilenames(infile, outdir)
    if stdout_file.exists() or stderr_file.exists():
        print("Log files already exists")
        sys.exit(1)
    with open(stdout_file, "w") as stdout, open(stderr_file, "w") as stderr:
        stdout.write(f"Start Time: {datetime.datetime.now()}\n")
        # subprocess.run(command, shell=True, stdout=stdout, stderr=stderr)
        stdout.write(f"End Time: {datetime.datetime.now()}\n")
    # TODO: create checks file contents
    # Need a script that counts number of frames and checks total charge per event
    # create checksum of output file
    sha512sum = get_sha512sum(outfile)
    print(f"file: {outfile} sha512sum: {sha512sum}")
    with open(outfile + ".sha512sum", "w"):
        file.write(f"{sha512sum}")

def run_parallel(infiles, max_num=20):
    with concurrent.futures.ProcessPoolExecutor(
        max_workers = max_num) as executor:
        futures = executor.map(runner, infiles)

# Taken from LTA
# Adapted from: https://stackoverflow.com/a/44873382
def get_sha512sum(filename: str) -> str:
    """Compute the SHA512 hash of the data in the specified file."""
    h = hashlib.sha512()
    b = bytearray(128 * 1024)
    mv = memoryview(b)
    with open(filename, 'rb', buffering=0) as f:
        # Known issue with MyPy: https://github.com/python/typeshed/issues/2166
        for n in iter(lambda: f.readinto(mv), 0):
            h.update(mv[:n])
    return h.hexdigest()

def remove_extention(path: PosixPath) -> PosixPath:
    suffixes = ''.join(path.suffixes)
    return Path(str(path).replace(suffixes, ''))

def get_outfilename(infile: Path, outdir: Path) -> Path:
    # Assuming Format of infile name is:
    # PFRaw_PhysicsFiltering_Run<RunNumber>_Subrun<SubRunNumber>_<FileNumber>.tar.gz
    infilename = str(remove_extention(infile))
    infilenwords = infilename.split('_')
    outfilewords = ["Pass3", "Step1"] + infilenwords[1:]
    outfilename = outdir / ("_".join(outfilewords) + ".i3.zst")
    return outfilename

def get_logfilenames(infile: Path, outdir: Path) -> tuple[Path, Path]:
    # Assuming Format of infile name is:
    # PFRaw_PhysicsFiltering_Run<RunNumber>_Subrun<SubRunNumber>_<FileNumber>.tar.gz
    infilename = str(remove_extention(infile))
    infilenwords = infilename.split('_')
    stdoutfilename = "_".join([
        "LOG", "OUT","Pass3", "Step1"] + infilenwords[1:]) + ".out"
    stderrfilename = "_".join(
        ["LOG", "ERR","Pass3", "Step1"] + infilenwords[1:]) + ".err"
    return outdir / stdoutfilename, outdir / stderrfilename

def generate_command(infile, gcd, outfile):
    envshell_loc = "/cvmfs/icecube.opensciencegrid.org/py3-v4.4.0/RHEL_9_x86_64_v2/metaprojects/icetray/v1.13.0/bin/icetray-shell"
    scriptloc = os.environ['I3_BUILD'] + "offline_filterscripts/resources/scripts/pass3_reprocess_PFRaw.py"
    return envshell_loc + " python3 " + scriptloc + f" -i {infile} -g {gcd} -o {outfile} --qify" 


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--gcd', 
                        help="GCD File", 
                        type=str, 
                        required=True)
    parser.add_argument("--files", 
                        help="", 
                        nargs='+',
                        required=True)
    parser.add_argument("--outdir", 
                        help="",
                        type=str,
                        required=True)
    parser.add_argument("--numcpus", 
                        help="",
                        type=int,
                        default=5)
    args=parser.parse_args()

    Path(args.outdir).mkdir(parents=True, exist_ok=True)

    commands = [(Path(args.gcd), Path(f), Path(args.outdir)) for f in args.files]
    run_parallel(commands, args.numcpus)