import argparse
import hashlib
import json
import os
import random
import shutil
import subprocess
import tempfile
import time
import zipfile
import asyncio
import concurrent.futures
from datetime import datetime, timezone
from pathlib import Path
from typing import Union
from rest_tools.client import ClientCredentials


def remove_extension(path: Path) -> Path:
    """Remove multiple suffixes from filename."""
    suffixes = ''.join(path.suffixes)
    return Path(str(path).replace(suffixes, ''))

def get_gcd(infile: Path, gcddir: Path) -> Path:
    if not gcddir.exists():
        raise FileNotFoundError("No GCD dir")
    # Assuming format in infile name is PFRaw_PhysicsFiltering_Run<RunNumber>_...
    runnum = get_run_number(infile)
    gcdfiles = list(gcddir.glob(f"*{runnum}*"))
    if len(gcdfiles) > 1:
        raise Exception(f"Multiple GCD files {gcdfiles} for run {runnum}")
    if len(gcdfiles) == 1:
        return gcdfiles[0]
    raise FileNotFoundError(f"No GCD found for {runnum}")

def get_outfilename(infile: Path) -> Path:
    infilename = str(remove_extension(infile))
    infilenwords = infilename.split('_')
    if infilename.startswith("ukey"):
        # some files start with ukey_<uuid>_PFRaw_PhysicsFiltering_Run<runnumber>_...
        outfilewords = ["Pass3", "Step1"] + infilenwords[3:]
    else:
        outfilewords = ["Pass3", "Step1"] + infilenwords[1:]
    return Path("_".join(outfilewords) + ".i3.zst")

def get_logfilenames(infile: Path, outdir: Path) -> tuple[Path, Path]:
    infilename = str(remove_extension(infile))
    infilenwords = infilename.split('_')
    stdoutfilename = "_".join(["LOG", "Pass3", "Step1"] + infilenwords[1:]) + ".out"
    stderrfilename = "_".join(["LOG", "Pass3", "Step1"] + infilenwords[1:]) + ".err"
    return outdir / stdoutfilename, outdir / stderrfilename

def generate_command(scriptloc: Path, infile: Path, gcd: Path, outfile: Path, qify: bool = False) -> str:
    command = f"python3 {scriptloc} -i {infile} -g {gcd} -o {outfile}"
    if qify:
        command += " --qify"
    return command

# Taken from LTA
# Adapted from: https://stackoverflow.com/a/44873382
def get_sha512sum(filename: Union[str, Path]) -> str:
    """Compute the SHA512 hash of the data in the specified file."""
    print(f"Getting sha512sum for {filename}")
    h = hashlib.sha512()
    b = bytearray(128 * 1024)
    mv = memoryview(b)
    with open(str(filename), 'rb', buffering=0) as f:
        for n in iter(lambda: f.readinto(mv), 0):
            h.update(mv[:n])
    return h.hexdigest()

def get_bundle(bundle: Path, outdir: Path, retry_attempts: int = 5):
    print(f"Getting bundle {bundle.name}")
    wait = random.randint(0, 7) * 600
    for a in range(retry_attempts):
        try:
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

def get_run_number(file: Union[str, Path]) -> int:
    s = str(file)
    try:
        if s.startswith("ukey"):
            return int(s.split('_')[4][3:])
        return int(s.split('_')[2][3:])
    except Exception:
        raise ValueError(f"File {s} is causing issues when extracting run number")

def get_MMDD(bundle: Path) -> str:
    # /stornext/ranch_01/ranch/projects/TG-PHY150040/data/exp/IceCube/2022/unbiased/PFRaw/0131/e88990d2110611eea23ac29b9287f457.zip
    return str(bundle).split("/")[-2]

def prepare_inputs(
    outdir: Path,
    scratchdir: Path,
    bundle: Path,
    checksum: str,
    gcddir: Path,
    grl: list[int],
    bad_files: list[str],
    transfer_bundle: bool = False,
) -> list[tuple[Path, Path, Path, Path]]:
    outdir.mkdir(parents=True, exist_ok=True)

    MMDD = get_MMDD(bundle)

    if (scratchdir / bundle.name).exists():
        bundle_loc = scratchdir / bundle.name
        bundle_sha512sum = get_sha512sum(bundle_loc)
        if bundle_sha512sum != checksum:
            raise Exception(f"Bundle {bundle_loc} checksum is not what we expect.")
        scratch_bundle_loc = bundle_loc
    elif (scratchdir / MMDD / bundle.name).exists():
        bundle_loc = scratchdir / MMDD / bundle.name
        bundle_sha512sum = get_sha512sum(bundle_loc)
        if bundle_sha512sum != checksum:
            raise Exception(f"Bundle {bundle_loc} checksum is not what we expect")
        scratch_bundle_loc = bundle_loc
    elif bundle.exists():
        bundle_sha512sum = get_sha512sum(bundle)
        if bundle_sha512sum != checksum:
            raise Exception(f"Bundle {bundle} checksum is not what we expect.")
        scratch_bundle_loc = bundle
    elif transfer_bundle:
        get_bundle(bundle, scratchdir)
        bundle_loc = scratchdir / bundle.name
        bundle_sha512sum = get_sha512sum(bundle_loc)
        if bundle_sha512sum != checksum:
            raise Exception(f"Bundle {bundle_loc} checksum is not what we expect.")
        scratch_bundle_loc = bundle_loc
    else:
        raise FileExistsError(f"Bundle {bundle} does not exist in scratch dir {scratchdir} or provided path")

    infiles = [f for f in zipfile.ZipFile(scratch_bundle_loc).namelist() if ".tar.gz" in f]
    if len(infiles) == 0:
        raise FileNotFoundError(f"No input files found in bundle {bundle}")

    inputs: list[tuple[Path, Path, Path, Path]] = []
    for f in infiles:
        try:
            runnum = get_run_number(f)
        except ValueError:
            continue
        if (runnum in grl) and (f not in bad_files):
            inputs.append((Path(gcddir), Path(scratch_bundle_loc), Path(f), Path(outdir)))

    return inputs

def get_grl(grl_path: Path) -> list[int]:
    grl: list[int] = []
    with grl_path.open("r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            grl.append(int(line))
    return grl

def get_bad_files(bad_files_path: Path) -> list[str]:
    bad_files: list[str] = []
    with bad_files_path.open("r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            bad_files.append(line)
    return bad_files

def check_i3_file(infile: Path) -> bool:
    print(f"Checking whether {infile} is a valid i3 file.")
    try:
        cmd = f"python3 {os.environ.get('I3_BUILD')}/dataio/resources/examples/scan.py -c {infile}"
        subprocess.run(cmd, shell=True, check=True)
    except subprocess.CalledProcessError:
        print(f"Renaming broken i3 file {infile}")
        try:
            infile.rename(Path(str(infile) + ".bad"))
        except Exception:
            pass
        return False
    return True

def check_gcd_file(gcdfile: Path) -> bool:
    print(f"Checking whether {gcdfile} is a good GCD file.")
    cmd = f"python3 /opt/pass3/scripts/icetray/step1/pass3_check_gcd.py -g {gcdfile} --corrections /opt/pass3/data/average_FADC_gain_bias_corrections.json"
    try:
        subprocess.run(cmd, shell=True, check=True)
    except subprocess.CalledProcessError:
        raise Exception(f"GCD file {gcdfile} does not have correct values")
    return True

def runner(infiles: tuple[Path, Path, Path, Path]) -> dict:

    # Getting file and file paths
    # Tuple of 4 paths
    # Each bundble can be a mix of runs so we need to grab the right GCD
    # for each infile
    gcddir = infiles[0]
    # Location of the bundle
    bundle = infiles[1]
    # File to be processed
    infile = infiles[2]
    # Where to put the output
    outdir = infiles[3]

    # Creating a temporary working dir for this instance
    tmpdir = Path(tempfile.mkdtemp(dir=str(bundle.parent)))

    # Getting the appropriate GCD file for a the run
    gcd = get_gcd(infile, gcddir)

    if not check_gcd_file(gcd):
        return {"status": "ERROR", "msg": f"GCD file {gcd} is not correct."}

    print(f"Copying GCD file: {gcd}")
    shutil.copy(gcd, tmpdir / gcd.name)
    local_gcd = tmpdir / gcd.name

    # Prepping files and file paths
    ## Extracting in file from bundle
    print(f"Extracting {infile}")
    zipfile.ZipFile(bundle).extract(str(infile), path=tmpdir)
    local_infile = tmpdir / infile
    if not local_infile.exists():
        raise FileNotFoundError("No Input File")

    outfilename = get_outfilename(infile)
    local_temp_outfile = tmpdir / ("tmp_" + str(outfilename.name))
    local_outfile = tmpdir / outfilename.name
    outfile = outdir / outfilename.name

    if outfile.exists():
        if not check_i3_file(outfile):
            raise Exception(f"Output file {outfile} is not a valid i3 file.")
        return {"status": "WARNING", "msg": f"Output file {outfile} from bundle {bundle} already exists."}

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

    try:
        with open(local_stdout_file, "w") as stdout, open(local_stderr_file, "w") as stderr:
            stdout.write(f"Start Time: {datetime.now(timezone.utc)}\n")
            stdout.write(f"Hostname: {os.environ.get('HOSTNAME')}\n")
            try:
                subprocess.run(command, shell=True, stdout=stdout, stderr=stderr, check=True)
            except subprocess.CalledProcessError:
                return {"status": "ERROR", "msg": f"{infile} in {bundle} has failed to process."}
            stdout.write(f"End Time PFRAW: {datetime.now(timezone.utc)}\n")
            try:
                subprocess.run(moni_command, shell=True, stdout=stdout, stderr=stderr, check=True)
            except subprocess.CalledProcessError:
                return {"status": "ERROR", "msg": f"{infile} in {bundle} has failed during moni."}
            stdout.write(f"End Time: {datetime.now(timezone.utc)}\n")
    finally:
        print("Copying logs")
        try:
            shutil.copy(local_stdout_file, stdout_file)
            shutil.copy(local_stderr_file, stderr_file)
        except Exception:
            print(f"Warning: could not copy log files to {outdir}")

    # create checksum of output file
    print(f"Getting sha512sum for {local_outfile}")
    sha512sum = get_sha512sum(local_outfile)
    print(f"file: {local_outfile} sha512sum: {sha512sum}")
    with open(f"{outfile}.sha512sum", "w") as fh:
        fh.write(f"{outfile} {sha512sum}")

    # Copying from local dir to absolute dir
    print("Copying output file")
    shutil.copy(local_outfile, outfile)

    sha512sum_final = get_sha512sum(outfile)
    if sha512sum_final != sha512sum:
        return {"status": "ERROR", "msg": f"Copying file {local_outfile} to final storage {outfile} failed. sha512sum mismatch."}
    if not check_i3_file(outfile):
        return {"status": "ERROR", "msg": f"Output file {outfile} is not a valid i3 file."}

    print("Copying moni files")
    for suffix in [".npz", ".fadc_atwd_charge.npz", ".fadc_atwd_charge.npz.comparison", ".txt"]:
        src = Path(str(local_outfile) + suffix)
        dst = Path(str(outfile) + suffix)
        try:
            shutil.copyfile(src, dst)
        except Exception:
            print(f"Warning: could not copy {src} to {dst}")

    shutil.rmtree(tmpdir)
    return {
        "status": "SUCCESS",
        "infile": f"{infile}",
        "bundle": f"{bundle}",
        "outfile": {"path": f"{outfile}", "sha512sum": f"{sha512sum}"},
    }

def get_year_filepath(file_path: str) -> str:
    return str(file_path).split("/")[-3]

def get_date_filepath(file_path: str) -> str:
    return str(file_path).split("/")[-2]

async def post_filecatalog(file: Path, checksum: str, client_secret: str):
    client = ClientCredentials(
        address="https://file-catalog.icecube.aq",
        token_url="https://keycloak.icecube.aq",
        client_id="pass3-briedel",
        client_secret=client_secret,
    )

    year = get_year_filepath(file)
    MMDD = get_date_filepath(file)
    time_str = datetime.utcfromtimestamp(file.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
    data = {
        "logical_name": f"/data/exp/IceCube/{year}/unbiased/PFDST/{MMDD}/{file.name}",
        "checksum": f"{checksum}",
        "file_size": f"{file.stat().st_size}",
        "locations": [{"site": "TACC", "path": f"{file}"}],
        "create_date": f"{time_str}",
    }
    await client.request("POST", "/api/files", data)

def run_parallel(infiles, filecatalogsecret: str | None = None, max_num: int = 1):
    if not infiles:
        return {}

    bundle_key = str(infiles[0][1])
    success: dict = {bundle_key: []}
    files_to_be_processed = [str(i[2]) for i in infiles]

    with concurrent.futures.ProcessPoolExecutor(max_workers=max_num) as executor:
        results = executor.map(runner, infiles)
        for res in results:
            print(res)
            if res.get("status") == "SUCCESS":
                outfile_path = res["outfile"]["path"]
                checksum = res["outfile"]["sha512sum"]
                success[bundle_key].append({"file": outfile_path, "checksum": checksum})
                if filecatalogsecret:
                    try:
                        asyncio.run(post_filecatalog(Path(outfile_path), checksum, filecatalogsecret))
                    except Exception as e:
                        print(f"Warning: posting to filecatalog failed: {e}")
                if res.get("infile") in files_to_be_processed:
                    files_to_be_processed.remove(res.get("infile"))

    json_path = infiles[0][3] / (infiles[0][1].name + ".json")
    with json_path.open("w") as fh:
        json.dump(success, fh, indent=4, sort_keys=True)

    if files_to_be_processed:
        raise Exception(f"Did not finish {files_to_be_processed}")
    return success

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--gcddir", help="Directory with GCD files", type=Path, required=True)
    parser.add_argument("--bundle", help="path to bundle on ranch", type=Path, required=True)
    parser.add_argument("--outdir", help="", type=Path, required=True)
    parser.add_argument("--scratchdir", help="Path where work should be done", type=Path, default=Path("/tmp"))
    parser.add_argument("--checksum", help="bundle sha512sum", type=str, required=True)
    parser.add_argument("--maxnumcpus", help="", type=int, default=0)
    parser.add_argument("--grl", help="good run list", type=Path, required=True)
    parser.add_argument("--badfiles", help="known bad files list", type=Path, required=True)
    parser.add_argument("--transferbundle", help="transfer bundle from tape", action='store_true')
    parser.add_argument("--filecatalogsecret", type=str, required=False)
    args = parser.parse_args()

    if args.maxnumcpus == 0:
        numcpus = os.cpu_count()
    else:
        numcpus = args.maxnumcpus

    # print(os.environ)
    # os.environ['OPENBLAS_MAIN_FREE'] = str(1)
    # os.system(f'taskset -cp 0-{numcpus} {os.getpid()}')

    print(f"Processing {args.bundle}")

    if not Path("/cvmfs/icecube.opensciencegrid.org/data/photon-tables/splines/InfBareMu_mie_prob_z20a10_V2.fits").exists():
        raise FileNotFoundError("Cant find splines")

    grl = get_grl(args.grl)
    badfiles = get_bad_files(args.badfiles)

    inputs = prepare_inputs(args.outdir,
                            args.scratchdir,
                            args.bundle,
                            args.checksum,
                            args.gcddir,
                            grl,
                            badfiles,
                            args.transferbundle)

    run_parallel(inputs, args.filecatalogsecret, numcpus)

    # TODO: Delete bundle
    # shutil.rmtree(args.bundle)

