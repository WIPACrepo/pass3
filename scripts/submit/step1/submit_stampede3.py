import argparse
import json

from typing import NoReturn
from pathlib import Path, PosixPath

def write_slurm_file(file: Path, 
                     queue: str,
                     jobname: str,
                     numnodes: int,
                     multiprogfile: Path
                       ) -> NoReturn:
    if queue not in ["skx", "spr", "icx"]:
        raise Exception("Didn't select supported queue.")
    with open(file, "w") as f:
        f.write(f"#SBATCH -p {queue}\n")
        f.write(f"#SBATCH -J {jobname}\n")
        f.write(f"#SBATCH -N {numnodes}\n")
        f.write(f"\n")
        f.write(f"srun --nodes=1 --ntasks=1 --ntasks-per-node=1 --mult-prog {multiprogfile}")


def write_srun_multiprog(file: str, 
                         bundles: list[Path],
                         outdir: Path,
                         gcddir: Path
                         ) -> NoReturn:
    
    for i, b in enumerate(bundles):
        file.write(f"{i} apptainer exec run pass3step1.sif ./run_step1.sh {b} {gcddir} {outdir}\n")

def get_ranch_dir(year: int, 
                  MMYY: str, 
                  ranch_base_dir: Path = Path("/stornext/ranch_01/ranch/projects/TG-PHY150040/data/exp/IceCube/")
                  ) -> Path:
    year_dir_ranch = ranch_base_dir / year / "unbiased" / "PFRaw"

    return year_dir_ranch / MMYY

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", 
                        help="Year to be processed", 
                        type=int,
                        required=True)
    

