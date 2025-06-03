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
                         apptainer_container: Path,
                         script: Path = Path("/opt/pass3/scripts/icetray/step1/run_step1.py"),
                         ) -> NoReturn:
    with Path.open(file, "w") as f:
        for i, (bundle, checksum) in enumerate(bundles.items()):
            f.write(
                f"{i} apptainer run -B /home1/04799/tg840985/pass3:/opt/pass3{apptainer_container} {script} --bundle {bundle} --gcddir {gcddir} --outdir {outdir} --checksum {checksum}\n")

def read_checksum_file(file_path: Path,
                       year: int) -> dict:
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
                        required=True
                        default=Path("/home1/04799/tg840985/test.multiprog"))
    parser.add_argument("--month",
                        help="month to process",
                        type=int,
                        required=False)
    args=parser.parse_args()

    checksums = read_checksum_file(args.checksum_file, args.year, args.month)

    write_srun_multiprog(args.multiprogfile,
                         checksums,
                         args.outdir,
                         args.gcddir,
                         args.container)

    write_slurm_file(args.submitfile,
                    "skx",
                    "Test",
                    32,
                    multiprogfile)

