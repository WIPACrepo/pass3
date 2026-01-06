import argparse
import json
import math
import os
import random

from typing import NoReturn
from pathlib import Path, PosixPath
from collections import defaultdict, OrderedDict
from itertools import islice

def chunks(data, SIZE=10000):
    # taken from stackoverflow to chunk dict for < python 3.12
    it = iter(data.items())
    for i in range(0, len(data), SIZE):
        yield dict(islice(it, SIZE))

def write_slurm_file(file: Path,
                     queue: str,
                     jobname: str,
                     numnodes: int,
                     allocation: str,
                     multiprogfile: Path,
                     multiprogfileincrements: int,
                     premovedbundles: bool
                     ) -> NoReturn:
    if queue not in ["skx", "spr", "icx", "gg"]:
        raise Exception("Didn't select supported queue.")
    with Path.open(file, "w") as f:
        f.write(f"#!/bin/bash\n")
        f.write(f"#SBATCH -t 24:00:00\n")
        f.write(f"#SBATCH -A {allocation}\n")
        f.write(f"#SBATCH -p {queue}\n")
        f.write(f"#SBATCH -J {jobname}\n")
        f.write(f"#SBATCH -N {numnodes}\n")
        f.write(f"#SBATCH -n {numnodes}\n")
        f.write(f"#SBATCH -o {jobname}.o.%j\n")
        f.write(f"#SBATCH -e {jobname}.e.%j\n")
        # f.write(f"#SBATCH -A {numnodes}\n")
        f.write(f"\n")
        f.write(f"echo `date`\n\n")
        f.write(f"LD_PRELOAD=\n")
        f.write(f"\n")
        for i in range(multiprogfileincrements):
            multiprogfile_inc = multiprogfile.parent / (multiprogfile.name + str(i)) 
            f.write(f"if [ ! -e {multiprogfile_inc}.done ]; then\n")
            f.write(f"echo Starting {multiprogfile_inc}\n")
            f.write(f"echo `date`\n")
            if not premovedbundles:
                f.write(f"srun --nodes={numnodes} --ntasks-per-node=1 --exclusive --multi-prog {multiprogfile_inc}.rsync && {multiprogfile_inc}.rsync.done || touch {multiprogfile_inc}.rsync.failed\n")
            f.write(f"srun --nodes={numnodes} --ntasks-per-node=1 --exclusive --cpus-per-task=$SLURM_CPUS_ON_NODE --multi-prog {multiprogfile_inc} && touch {multiprogfile_inc}.done || touch {multiprogfile_inc}.failed\n")
            f.write(f"fi\n")
            f.write(f"\n")

def get_year_filepath(file_path: str) -> str:
    return str(file_path).split("/")[-5]

def get_date_filepath(file_path: str) -> str:
    return str(file_path).split("/")[-2]

def write_srun_multiprog(file: Path,
                         bundles: defaultdict[Path],
                         increment: int,
                         outdir: Path,
                         gcddir: Path,
                         apptainer_container: Path,
                         scratchdir: Path,
                         numcores: int,
                         grl: Path,
                         env_shell: Path,
                         badfiles: Path,
                         numnodes: int,
                         filecatalogsecret: str,
                         script: Path = Path("/opt/pass3/scripts/icetray/step1/run_step1.py"),
                         ) -> NoReturn:
    file = file.parent / (file.name + str(increment))
    with Path.open(file.parent / (file.name + str(".rsync")), "w") as f:
        for i, (bundle, checksum) in enumerate(bundles.items()):
            nodenum = random.randint(1,2)
            f.write(f"{i}  scp  {os.environ['ARCHIVER']}:{bundle} login{nodenum}.vista.tacc.utexas.edu:{scratchdir}/\n")
    with Path.open(file, "w") as f:
        for i, (bundle, checksum) in enumerate(bundles.items()):
            year = get_year_filepath(str(bundle))
            date = get_date_filepath(str(bundle))
            f.write(f"{i}  /opt/apps/tacc-apptainer/1.3.3/bin/apptainer ")
            f.write(f"exec -B /home1/04799/tg840985/pass3:/opt/pass3 ")
            f.write(f"-B /work/04799/tg840985/vista/splines/splines:/cvmfs/icecube.opensciencegrid.org/data/photon-tables/splines ")
            f.write(f"-B /work2 -B /scratch {apptainer_container} {env_shell} ")
            f.write(f"python3 {script} --bundle {bundle} --gcddir {gcddir} ")
            f.write(f"--outdir {outdir}/{year}/{date} --checksum {checksum} ")
            f.write(f"--scratchdir {scratchdir} --grl {grl} ")
            f.write(f"--badfiles {badfiles} ")
            f.write(f"--filecatalogsecret {filecatalogsecret}")
            if numcores != 0:
                f.write(f" --maxnumcpus {numcores}")
            f.write(f"\n")
        # Below is to make srun multi-prog file happy
        # you always need number of tasks = number of nodes
        # else when parsing the multiprog file it will fail
        if len(bundles) < numnodes:
            for i in range(len(bundles), numnodes):
                f.write(f"{i}  echo  \"extra tasks to make srun happy\"\n")


def month_in_path(file_path: str,
                  month: int) -> bool:
    """Example path on Ranch: /stornext/ranch_01/ranch/projects/TG-PHY150040/data/exp/IceCube/2020/unbiased/PFRaw/0420/7202275ab7a111eb8013bedaff42a7c6.zip"""
    if month ==  int(get_date_filepath(file_path)[0:2]):
        return True
    return False

def year_in_path(file_path: str,
                 year: int) -> bool:
    if str(year) == get_year_filepath(file_path):
        return True
    return False

def get_file_checksums(file_path: Path) -> defaultdict(Path):
    """"Reading in the checksum file"""
    tmp_checksums = defaultdict(Path)
    with Path.open(file_path, "r") as f:
        while line := f.readline():
            line = line.rstrip()
            # Assuming each line is formatted (<sha512sum> <absolute_file_path>) and space separated
            checksum, archive_path = line.split()
            tmp_checksums[Path(archive_path)] = checksum
    return tmp_checksums

def get_checksum_year_month(file_path: Path,
                            year: int,
                            month: int,
                            numnodes: int) -> list:
    """
    Get the list of file paths and their checksums for a given month and year. Chunking them up by number of nodes to make srun multiprog happy
    """
    if numnodes <= 0:
        raise Exception(f"Number of nodes {numnodes} has to be >= 1")
    tmp_checksums = get_file_checksums(file_path)
    filtered_checksum = { key: value for key, value in tmp_checksums.items() 
                         if year_in_path(key, year) and (month != 0 and month_in_path(key, month))}
    # tmp_checksums[Path(archive_path)] = checksum
    # tmp_checksums = OrderedDict(sorted(tmp_checksums.items()))
    if numnodes == 1:
        return [filtered_checksum]
    else:
        # chunking the map of files to checksums according to the
        # number of nodes
        return [i for i in chunks(filtered_checksum, numnodes)]

def get_checksums_bundles(file_path: Path,
                          bundles: list[Path],
                          numnodes: int) -> list:
    if numnodes <= 0:
        raise Exception(f"Number of nodes {numnodes} has to be >= 1")
    bundle_names = [b.name for b in bundles]
    tmp_checksums = get_file_checksums(file_path)
    filtered_checksum = sort({ key: value for key, value in tmp_checksums.items() 
                         if key.name in bundle_names})
    if numnodes == 1:
        return [filtered_checksum]
    else:
        # chunking the map of files to checksums according to the
        # number of nodes
        return [i for i in chunks(filtered_checksum, numnodes)]

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--checksum-file",
                        help="checksum file",
                        type=Path,
                        required=True)
    parser.add_argument("--year",
                        help="year to process",
                        type=int,
                        default=-1,
                        required=False)
    parser.add_argument("--gcddir",
                        help="Directory with GCD files",
                        type=Path,
                        required=True)
    parser.add_argument("--outdir",
                        help="output directory",
                        type=Path,
                        required=True)
    parser.add_argument("--container",
                        help="container to run in",
                        type=Path,
                        required=True)
    parser.add_argument("--submitfile",
                        help="Path of submit file to write",
                        type=Path,
                        required=True)
    parser.add_argument("--multiprogfile",
                        help="path of multiprogfilr to write",
                        type=Path,
                        required=True,
                        default=Path("/home1/04799/tg840985/test.multiprog"))
    parser.add_argument("--month",
                        help="month to process",
                        type=int,
                        default=-1,
                        required=False)
    parser.add_argument("--numnodes",
                        help="number of nodes",
                        type=int,
                        default=32,
                        required=False)
    parser.add_argument("--scratchdir",
                        help="scratch dir to use",
                        type=Path,
                        default="/tmp",
                        required=False)
    parser.add_argument("--slurmqueue",
                        help="slurm queue to use",
                        type=str,
                        required=True)
    parser.add_argument("--numcores",
                        help="how many cores per node to use",
                        type=int,
                        default=0,
                        required=False)
    parser.add_argument("--allocation",
                        help="allocation to use",
                        type=str,
                        choices=["TG-PHY150040","PHY20012","AST22007"],
                        default="TG-PHY150040",
                        required=False)
    parser.add_argument("--grl",
                        help="path to good run list",
                        type=Path,
                        required=True)
    parser.add_argument("--badfiles",
                        help="path to list of bad files",
                        type=Path,
                        required=True)
    parser.add_argument("--cpuarch",
                        help="cpu arch you are running on",
                        type=str,
                        default="x86_64_v4",
                        choices=["x86_64_v4", "aarch64"])
    parser.add_argument("--bundlesready",
                        help="whether bundles are already in tmp location",
                        action="store_true")
    parser.add_argument("--bundles",
                        help="a list of bundles to process",
                        nargs='+',
                        type=Path,
                        required=False
                        )
    parser.add_argument("--filecatalogsecret",
                        help="client secret for file catalog",
                        type=str,
                        required=True)
    args=parser.parse_args()

    env_shell = Path(f"/cvmfs/icecube.opensciencegrid.org/py3-v4.4.2/RHEL_9_{args.cpuarch}/metaprojects/icetray/v1.17.0/bin/icetray-shell")

    if args.year != -1 and args.month != -1:
        checksums = get_checksum_year_month(args.checksum_file,
                                            args.year, args.month, args.numnodes)
    elif len(args.bundles) > 0:
        checksums = get_checksums_bundles(args.checksum_file, args.year, args.month, args.numnodes)
    else:
        raise RuntimeError("Need to provide a year and month or list of bundles to process")

    for i, cs in enumerate(checksums):
        write_srun_multiprog(
            args.multiprogfile,
            cs,
            i,
            args.outdir,
            args.gcddir,
            args.container,
            args.scratchdir,
            args.numcores,
            args.grl,
            env_shell,
            args.badfiles,
            args.numnodes,
            args.filecatalogsecret)

    write_slurm_file(args.submitfile,
                    args.slurmqueue,
                    str(args.submitfile),
                    args.numnodes,
                    args.allocation,
                    args.multiprogfile,
                    len(checksums),
                    args.bundlesready)
