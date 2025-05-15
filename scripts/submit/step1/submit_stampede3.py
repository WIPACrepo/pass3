import argparse
import json

from typing import NoReturn
from pathlib import Path, PosixPath
from collections import defaultdict

def write_slurm_file(file: Path, 
                     queue: str,
                     jobname: str,
                     numnodes: int,
                     multiprogfile: Path
                       ) -> NoReturn:
    if queue not in ["skx", "spr", "icx"]:
        raise Exception("Didn't select supported queue.")
    with Path.open(file, "w") as f:
        f.write(f"#SBATCH -p {queue}\n")
        f.write(f"#SBATCH -J {jobname}\n")
        f.write(f"#SBATCH -N {numnodes}\n")
        # f.write(f"#SBATCH -A {numnodes}\n")
        f.write(f"\n")
        f.write(f"srun --nodes=1 --ntasks=1 --ntasks-per-node=1 --mult-prog {multiprogfile}")

def write_srun_multiprog(file: str, 
                         bundles: defaultdict[Path],
                         outdir: Path,
                         gcddir: Path,
                         script: Path,
                         ) -> NoReturn:
    script="/opt/pass3/scripts/icetray/step1/run_step1.py"
    for i, (bundle, checksum) in enumerate(bundles.items()):
        file.write(
            f"{i} apptainer exec run pass3step1.sif {script} --bundle {bundle} --gcddir {gcddir} --outdir {outdir} --checksum {checksum}\n")

def read_checksum_file(file_path: Path, year: int) -> dict:
    checksums = defaultdict(Path)
    with Path.open(file_path, "r") as f:
        while line := file.readline():
            line = line.rstrip()
            # Assuming each line is formatted (<sha512sum> <file_path>) and the values are separated by a space
            checksum, archive_path = line.split(" ")
            if str(year) in archive_path:
                checksums[Path(archive_path)] = checksum
    return checksums

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--checksum-file",
                        help="checksum file",
                        type=Path,
                        required=True
                        )
    parser.add_argument("--year",
                        help="year to process",
                        type=int,
                        required=True)
    args=parser.parse_args()

    checksums = read_checksum_file(args.checksum_file, args.year)

