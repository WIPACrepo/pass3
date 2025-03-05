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
                         bundles: list[Path],
                         outdir: Path,
                         gcddir: Path
                         ) -> NoReturn:
    script="./run_step1.sh"
    for i, b in enumerate(bundles):
        file.write(
            f"{i} apptainer exec run pass3step1.sif {script} {b} {gcddir} {outdir}\n")

def get_ranch_dir(year: int, 
                  MMYY: str, 
                  ranch_base_dir: Path = Path("/stornext/ranch_01/ranch/projects/TG-PHY150040/data/exp/IceCube/")
                  ) -> Path:
    year_dir_ranch = ranch_base_dir / year / "unbiased" / "PFRaw"

    return year_dir_ranch / MMYY

def get_scratch_dir(scratch_base_dir: Path) -> Path:
    pass

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

def read_json_file(file_path: Path) -> dict:
    """
    Reads a JSON file and returns the data as a Python dictionary.

    Args:
        file_path (str): The path to the JSON file.

    Returns:
        dict: The JSON data as a Python dictionary, or None if an error occurs.
    """
    try:
        with Path.open(file_path, 'r') as file:
            data = json.load(file)
            return data
    except FileNotFoundError:
        raise FileNotFoundError(f"Error: File not found at {file_path}")
    except json.JSONDecodeError:
        raise json.JSONDecodeError(
            f"Error: Invalid JSON format in {file_path}")
    
def read_checksum_file(file_path: Path) -> dict:
    checksums = defaultdict(Path)
    with Path.open(file_path, "r") as f:
        while line := file.readline():
            line = line.rstrip()
            # Assuming each line is formatted (<sha512sum> <file_path>) and the values are separated by a space
            checksum, archive_path = line.split(" ")
            checksums[Path(archive_path).name] = checksum
    return checksums

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", 
                        help="JSON of Year to be processed", 
                        type=str,
                        required=True)
    parser.add_argument("--checksums",
                        help="checksum file",
                        )
    args=parser.parse_args()








