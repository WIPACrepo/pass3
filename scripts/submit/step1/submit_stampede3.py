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
        f.write(f"#SBATCH -o myjob.o.%j\n")
        f.write(f"#SBATCH -e myjob.e.%j\n")
        # f.write(f"#SBATCH -A {numnodes}\n")
        f.write(f"\n")
        f.write(f"echo `date`\n")
        f.write(f"\n")
        for i in range(multiprogfileincrements):
            multiprogfile_inc = multiprogfile.parent / (multiprogfile.name + str(i)) 
            f.write(f"if [ ! -e {multiprogfile_inc}.done ]; then\n")
            f.write(f"echo Starting {multiprogfile_inc}\n")
            f.write(f"echo `date`\n")
            if not premovedbundles:
                f.write(f"srun --nodes={numnodes} --ntasks-per-node=1 --exclusive --multi-prog {multiprogfile_inc}.rsync && {multiprogfile_inc}.rsync.done || touch {multiprogfile_inc}.rsync.failed\n")
            f.write(f"srun --nodes={numnodes} --ntasks-per-node=1 --exclusive --multi-prog {multiprogfile_inc} && touch {multiprogfile_inc}.done || touch {multiprogfile_inc}.failed\n")
            f.write(f"fi\n")
            f.write(f"\n")

def get_year_filepath(file_path: str) -> str:
    return file_path.split("/")[9]

def get_date_filepath(file_path: str) -> str:
    return file_path.split("/")[12]

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
            f.write(f"{i}  /opt/apps/tacc-apptainer/1.3.3/bin/apptainer  ")
            f.write(f"exec -B /home1/04799/tg840985/pass3:/opt/pass3 ")
            f.write(f"-B /work2 -B /scratch {apptainer_container} {env_shell} ")
            f.write(f"python3 {script} --bundle {bundle} --gcddir {gcddir} ")
            f.write(f"--outdir {outdir}/{year}/{date} --checksum {checksum} ")
            f.write(f"--scratchdir {scratchdir} --grl {grl}")
            if numcores != 0:
                f.write(f" --maxnumcpus {numcores}")
            f.write(f"\n")

def month_in_path(file_path: str,
                  month: int) -> bool:
    "/stornext/ranch_01/ranch/projects/TG-PHY150040/data/exp/IceCube/2020/unbiased/PFRaw/0420/7202275ab7a111eb8013bedaff42a7c6.zip"
    if str(month) in get_date_filepath(file_path)[0:2]:
        return True
    return False

def year_in_path(file_path: str,
                 year: int) -> bool:
    if str(year) == get_year_filepath(file_path):
        return True
    return False

def read_checksum_file(file_path: Path,
                       year: int,
                       month: int,
                       numnodes: int):
    if numnodes <= 0:
        raise Exception(f"Number of nodes {numnodes} has to be >= 1")
    tmp_checksums = defaultdict(Path)
    with Path.open(file_path, "r") as f:
        while line := f.readline():
            line = line.rstrip()
            # Assuming each line is formatted (<sha512sum> <file_path>) and the values are separated by a space
            checksum, archive_path = line.split()
            if year_in_path(archive_path, year) and (month != 0 and month_in_path(archive_path, month)):
                tmp_checksums[Path(archive_path)] = checksum
    tmp_checksums = OrderedDict(sorted(tmp_checksums.items()))
    if numnodes == 1:
        return [tmp_checksums]
    else:
        return [i for i in chunks(tmp_checksums, numnodes)]

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--checksum-file",
                        help="checksum file",
                        type=Path,
                        required=True)
    parser.add_argument("--year",
                        help="year to process",
                        type=int,
                        required=True)
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
                        default=0,
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
                        help="good run list",
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
    args=parser.parse_args()

    env_shell = Path(f"/cvmfs/icecube.opensciencegrid.org/py3-v4.4.1/RHEL_9_{args.cpuarch}/metaprojects/icetray/v1.15.2/bin/icetray-shell")

    checksums = read_checksum_file(args.checksum_file, args.year, args.month, args.numnodes)

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
            env_shell)

    write_slurm_file(args.submitfile,
                    args.slurmqueue,
                    "Test",
                    args.numnodes,
                    args.allocation,
                    args.multiprogfile,
                    len(checksums),
                    args.bundlesready)
