import argparse
import subprocess
import hashlib
import datetime
import os
import zipfile
import shutil
import tempfile
import time
import random

from pathlib import Path, PosixPath
import concurrent.futures
from typing import Union, NoReturn

def remove_extension(path: Path) -> Path:
    """Remove multiple suffixes from filename"""
    suffixes = ''.join(path.suffixes)
    return Path(str(path).replace(suffixes, ''))

def get_gcd(infile: Path, gcddir: Path) -> Path:
    if not gcddir.exists():
        raise FileNotFoundError("No GCD dir")
    # Assuming Format of infile name is:
    # PFRaw_PhysicsFiltering_Run<RunNumber>_Subrun<SubRunNumber>_<FileNumber>.tar.gz
    runnum = get_run_number(str(infile))
    gcdfiles = list(gcddir.glob(f"*{runnum}*"))
    if len(gcdfiles) > 1:
        raise Exception(f"Multiple GCD Files {gcdfiles}")
    elif len(gcdfiles) == 1:
        return gcdfiles[0]
    else:
        FileNotFoundError("No GCD found")

def get_outfilename(infile: Path) -> Path:
    # Assuming Format of infile name is:
    # PFRaw_PhysicsFiltering_Run<RunNumber>_Subrun<SubRunNumber>_<FileNumber>.tar.gz
    infilename = str(remove_extension(infile))
    # ukey_<uuid>_PFRaw_PhysicsFiltering_Run<RunNumber>_Subrun<SubRunNumber>_<FileNumber>.tar.gz
    infilenwords = infilename.split('_')
    if infilename.startswith("ukey"):
        outfilewords = ["Pass3", "Step1"] + infilenwords[3:]
    else:
        outfilewords = ["Pass3", "Step1"] + infilenwords[1:]
    return "_".join(outfilewords) + ".i3.zst"

def get_logfilenames(infile: Path, outdir: Path) -> tuple[Path, Path]:
    # Assuming Format of infile name is:
    # PFRaw_PhysicsFiltering_Run<RunNumber>_Subrun<SubRunNumber>_<FileNumber>.tar.gz
    infilename = str(remove_extension(infile))
    infilenwords = infilename.split('_')
    stdoutfilename = "_".join(
        ["LOG", "Pass3", "Step1"] + infilenwords[1:]) + ".out"
    stderrfilename = "_".join(
        ["LOG", "Pass3", "Step1"] + infilenwords[1:]) + ".err"
    return outdir / stdoutfilename, outdir / stderrfilename

def generate_command(scriptloc: Path,
                     infile: Path,
                     gcd: Path,
                     outfile: Path,
                     qify: bool = False) -> str:
    command = f"python3 {scriptloc} -i {infile} -g {gcd} -o {outfile}"
    if qify:
        command += " --qify"
    return command

# Taken from LTA
# Adapted from: https://stackoverflow.com/a/44873382
def get_sha512sum(filename: Union[str, Path]) -> str:
    """Compute the SHA512 hash of the data in the specified file."""
    print(f"Gettng sha512sum for {filename}")
    h = hashlib.sha512()
    b = bytearray(128 * 1024)
    mv = memoryview(b)
    with open(filename, 'rb', buffering=0) as f:
        # Known issue with MyPy: https://github.com/python/typeshed/issues/2166
        for n in iter(lambda: f.readinto(mv), 0):
            h.update(mv[:n])
    return h.hexdigest()

def get_bundle(bundle: Path, outdir: Path, retry_attempts: int = 5):
    print(f"Getting bundle {bundle.name}")
    wait = random.randint(0, 7) * 600
    for a in range(retry_attempts):
        try:
            # time.sleep(wait)
            print(f"scp'ing bundle {bundle}: scp ranch.tacc.utexas.edu:{bundle} {str(outdir) + '/'}")
            subprocess.run(f"scp ranch.tacc.utexas.edu:{bundle} {str(outdir) + '/'}", shell=True, check=True)
            print(f"Successfully retrieved bundle: {bundle}")
            break
        except subprocess.CalledProcessError as e:
            print(f"Retrieving bundled {bundle}  failed (attempt {a + 1}/{retry_attempts})")
            print(f"Error output: {e.stderr}")
            if a < (retry_attempts - 1):
                print(f"Waiting 10 seconds")
                time.sleep(10)
            else:
                raise

def get_run_number(file: str) -> int:
    # Assuming Format of infile name is:
    try:
        if file.startswith("ukey"):
            # ukey_<uuid>_PFRaw_PhysicsFiltering_Run<RunNumber>_Subrun<SubRunNumber>_<FileNumber>.tar.gz
            return int(file.split('_')[4][3:])
        else:
            #PFRaw_PhysicsFiltering_Run<RunNumber>_Subrun<SubRunNumber>_<FileNumber>.tar.gz
            return int(file.split('_')[2][3:])
    except:
        raise ValueError(f"File {file} is causing issues when extracting run number")

def prepare_inputs(outdir: Path,
                   scratchdir: Path,
                   bundle: Path,
                   checksum: str,
                   gcddir: Path,
                   grl: list[int],
                   bad_files: list[str]) -> list:
    if not outdir.exists():
        outdir.mkdir(parents=True, exist_ok=True)

    if (scratchdir / bundle.name).exists():
        # Checking if available bundle is good
        print(f"{scratchdir / bundle.name} exists")
        bundle_sha512sum = get_sha512sum(scratchdir / bundle.name)
        if bundle_sha512sum != checksum:
            shutil.rmtree(scratchdir / bundle.name)
            get_bundle(bundle, scratchdir)
            bundle_sha512sum = get_sha512sum(scratchdir / bundle.name)
            if bundle_sha512sum != checksum:
                # Bundle from tape is borked...
                raise Exception(f"Bundle {bundle} checksum is not what we expect. Something wrong with tape bundle???")
    else:
        # Getting bundle if it doesn't exist
        get_bundle(bundle, scratchdir)

    scratch_bundle_loc = scratchdir / bundle.name

    infiles = [f for f in zipfile.ZipFile(scratch_bundle_loc).namelist()
               if ".tar.gz" in f]

    if len(infiles) == 0:
        raise FileNotFoundError(
            f"No input files found in bundle {args.bundle}")

    inputs = [
        (Path(gcddir),
         Path(scratch_bundle_loc),
         Path(f),
         Path(outdir))
         for f in infiles
         if (get_run_number(f) in grl) and (f not in bad_files)]

    return inputs

def get_grl(grl_path: Path) -> list[int]:
    grl = []
    with Path.open(grl_path, "r") as f:
        while line := f.readline():
            grl.append(int(line.rstrip()))
    return grl

def get_bad_files(bad_files_path: Path) -> list[str]:
    bad_files = []
    with Path.open(bad_files_path, "r") as f:
        while line := f.readline():
            if line.startswith("#"): continue
            bad_files.append(line.rstrip("\n"))
    return bad_files

def check_i3_file(infile: Path) -> bool:
    print(f"Checking whether {infile} is a valid i3 file.")
    try:
        cmd = f"python3 {os.environ['I3_BUILD']}/dataio/resources/examples/scan.py -c {infile}"
        subprocess.run(cmd, shell=True)
    except:
        print("Renaming broken i3 file")
        os.rename(infile, str(infile) + ".bad")
        return False
    else:
        return True

def runner(infiles: tuple[Path, Path, Path, Path]) -> str:

    print(shutil.disk_usage("/tmp"))

    # Getting file and file paths
    gcddir = infiles[0]
    bundle = infiles[1]
    infile = infiles[2]
    outdir = infiles[3]

    # Creating a temporary working dir for this instance
    tmpdir = Path(tempfile.mkdtemp(dir=str(bundle.parent)))

    gcd = get_gcd(infile, gcddir)
    if not gcd.exists():
        raise FileNotFoundError("No GCD")

    print(f"Copying GCD file: {gcd}")
    shutil.copyfile(gcd, tmpdir / gcd.name)
    local_gcd = tmpdir / gcd.name

    # Prepping files and file paths
    ## Extracting in file from bundle
    print(f"Extracting {infile}")
    zipfile.ZipFile(bundle).extract(str(infile), path=tmpdir)
    local_infile = tmpdir / infile
    if not local_infile.exists():
        raise FileNotFoundError("No Input File")

    outfilename = get_outfilename(infile)
    local_temp_outfile = tmpdir / ("tmp_" + str(outfilename))
    local_outfile = tmpdir / outfilename
    outfile = outdir / outfilename

    if outfile.exists():
        if not check_i3_file(outfile):
            print(f"Output file {outfile} is not a valid i3 file.")
        return f"Output file {outfile} already exists"

    local_stdout_file, local_stderr_file = get_logfilenames(infile,
                                                            tmpdir)
    stdout_file, stderr_file = get_logfilenames(infile,
                                                outdir)

    command = generate_command(
        Path("/opt/pass3/scripts/icetray/step1/pass3_reprocess_PFRaw.py"),
        local_infile,
        local_gcd,
        local_outfile,
        qify = True)
    moni_command = generate_command(
        Path("/opt/pass3/scripts/icetray/step1/pass3_check_charge_filter.py"),
        local_outfile,
        local_gcd,
        local_outfile)

    # run step 1
    # We are first running the online processing that is the same as done
    # at the south pole. we then read the file back in, rehydrate it, and
    # run some moni code on it to make sure we are doing the right thing.

    print(f"Running command: {command}")

    try:
        with open(local_stdout_file, "w") as stdout, open(local_stderr_file, "w") as stderr:
            # TODO: Do we need to check the GCD file?
            stdout.write(
                    f"Start Time: {datetime.datetime.now(datetime.timezone.utc)}\n")
            stdout.write(f"Hostname: {os.environ['HOSTNAME']}\n")
            try:
                subprocess.run(command, shell=True, stdout=stdout, stderr=stderr)
            except:
                return f"ALERT: {infile} in {bundle} has failed to process\n"
            stdout.write(
                    f"End Time PFRAW: {datetime.datetime.now(datetime.timezone.utc)}\n")
            try:
                subprocess.run(
                    moni_command, shell=True, stdout=stdout, stderr=stderr)
            except:
                return f"ALERT: {infile} in {bundle} has failed during moni\n"
            stdout.write(
                    f"End Time: {datetime.datetime.now(datetime.timezone.utc)}\n")
    finally:
        print("Copying logs")
        shutil.copyfile(local_stdout_file, stdout_file)
        shutil.copyfile(local_stderr_file, stderr_file)

    # create checksum of output file
    print(f"Getting sha512sum for {local_outfile}")
    sha512sum = get_sha512sum(local_outfile)
    print(f"file: {local_outfile} sha512sum: {sha512sum}")
    with Path.open(str(outfile) + ".sha512sum", "w") as file:
        file.write(f"{outfile} {sha512sum}")

    # Copying from local dir to absolute dir
    print("Copying output file")
    shutil.copyfile(local_outfile, outfile)

    sha512sum_final = get_sha512sum(outfile)
    if sha512sum_final != sha512sum:
        return f"ALERT: Copying file {local_outfile} from tmp storage to final storage {outfile} failed. sha512sum mismatch."

    if not check_i3_file(outfile):
        return f"ALERT: Output file {outfile} is not a valid i3 file."

    print("Copying moni files")
    shutil.copyfile(str(local_outfile) + ".npz", str(outfile) + ".npz")
    shutil.copyfile(str(local_outfile) + "fadc_atwd_charge.npz",
                    str(outfile) + "fadc_atwd_charge.npz",)
    shutil.copyfile(str(local_outfile) + ".txt", str(outfile) + ".txt")

    shutil.rmtree(tmpdir)
    return f"Processing file {infile} from bundle {bundle} was SUCCESSFUL. Output file {outfile} with checksum {sha512sum}."

def run_parallel(infiles, max_num=1):
    print(f"max workers: {max_num}")
    print(f"length infiles: {len(infiles)}")
    with concurrent.futures.ProcessPoolExecutor(
        max_workers = max_num) as executor:
        futures = executor.map(runner, infiles)
        for f in futures:
            print(f)
    return futures

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--gcddir',
                        help="Directory with GCD files",
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
    parser.add_argument("--scratchdir",
                        help="Path where work should be done",
                        type=Path,
                        default="/tmp")
    parser.add_argument("--checksum",
                        help="bundle sha512sum",
                        type=str,
                        required=True)
    parser.add_argument("--maxnumcpus",
                        help="",
                        type=int,
                        default=0)
    parser.add_argument("--grl",
                        help="good run list",
                        type=Path,
                        required=True)
    parser.add_argument("--badfiles",
                        help="known bad files list",
                        type=Path,
                        required=True)
    args=parser.parse_args()

    if args.maxnumcpus == 0:
        numcpus = os.cpu_count()
    else:
        numcpus = args.maxnumcpus

    # print(os.environ)
    # os.environ['OPENBLAS_MAIN_FREE'] = str(1)
    # os.system(f'taskset -cp 0-{numcpus} {os.getpid()}')

    print(f"CPU count: {numcpus}")

    grl = get_grl(args.grl)
    badfiles = get_bad_files(args.badfiles)

    inputs = prepare_inputs(args.outdir,
                            args.scratchdir,
                            args.bundle,
                            args.checksum,
                            args.gcddir,
                            grl,
                            badfiles)

    run_parallel(inputs, numcpus)

    # TODO: Delete bundle
    # shutil.rmtree(args.bundle)

