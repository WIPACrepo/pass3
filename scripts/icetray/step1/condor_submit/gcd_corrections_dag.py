from __future__ import annotations

import json
from argparse import ArgumentParser
from pathlib import Path
from typing import Any


def build_parser() -> ArgumentParser:
    
    PASS3_CODE_DIR = Path(__file__).resolve().parents[-5]

    parser.add_argument(
        "--original-gcd-dirs",
        type=Path,
        default=Path("/data/ana/IceCube"),
        help=(
            "Root of the original GCD tree. The script searches "
            "{root}/YYYY/filtered/debug.v2/GCD/"
        ),
    )
    parser.add_argument(
        "--new-gcd-dirs",
        type=Path,
        default=Path("/data/ana/IceCube"),
        help=(
            "Root of the Pass3 GCD tree. The script places files under "
            "{root}/YYYY/filtered/debug.v3/GCD/."
        ),
    )
    parser.add_argument(
        "--fadc-gain-corrections",
        type=Path,
        required=True,
        help="Path to JSON file containing FADC gain corrections."
        default=PASS3_CODE_DIR / "data/fadc_gain_corrections.json"
    )
    parser.add_argument(
        "--correction-script",
        type=Path,
        default=PASS3_CODE_DIR / "scripts/icetray/step1/ pass3_correct_nan_relative_dom_eff.py"
        help="Comparison script executed by each DAG node.",
    )
    parser.add_argument(
        "--log-dir",
        type=Path,
        default="/scratch/briedel/pass3_gcd_corrections/logs",
        help="Comparison script executed by each DAG node.",
    )
    parser.add_argument(
        "--request-cpus",
        type=int,
        default=1,
        help="CPU request for each HTCondor job.",
    )
    parser.add_argument(
        "--request-memory",
        type=str,
        default="2G",
        help="Memory request for each HTCondor job.",
    )
    parser.add_argument(
        "--request-disk",
        type=str,
        default="2G",
        help="Disk request for each HTCondor job.",
    )

    return parser

def build_job_block(
    args,
    original_gcd: Path,
) -> str:
    job_name = f"gcd_correction_{original_gcd.stem}"

    new_gcd = get_output_path(args, original_gcd)

    args = (
        
        f"--ingcd {original_gcd} "
        f"--outgcd {new_gcd} "
        f"--corrections {fadc_gain_corrections} "
    )

    lines = [
        f"JOB {job_name} gcd_correction",
        f'VARS {job_name} args="{args}"',
        "",
    ]
    return "\n".join(lines)


def build_submit_description(args) -> str:
    lines = [
        "SUBMIT-DESCRIPTION gcd_correction {",
        f'executable   = {args.correction_script}',
        'arguments    = " $(args)"',
        f"output       = {args.logdir}/$(run).out",
        f"error        = {args.logdir}/$(run).err",
        "log          = /dev/null",
        f"request_cpus   = {args.request_cpus}",
        f"request_memory = {args.request_memory}",
        f"request_disk   = {args.request_disk}",
        "}",
        "",
    ]
    return "\n".join(lines)


def is_valid_year(s):
    return s.isdigit() and len(s) == 4

def build_input_files(args) -> list(Path):
    # Example path for GCD file: 
    # /data/ana/IceCube/2016/filtered/debug.v2/GCD/OnlinePass3_IC86.2016_data_Run00129002_1231_89_290_GCD.i3.zst
    input_files = set()
    for year_dir in args.original_gcd_dirs.glob("*/"):
        if is_valid_year(year_dir.name):
            for f in year_dir.glob("filtered/debug.v2/GCD/"):
                input_files.add(f)
    return sorted(input_files)

def get_output_path(args, original_gcd: Path) -> Path:
    parts = original_gcd.parts
    year = parts[-5]
    new_gcd_dir = args.new_gcd_dirs / year / "filtered/debug.v3/GCD"
    new_gcd_dir.mkdir(parents=True, exist_ok=True)
    return new_gcd_dir / original_gcd.name

def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    dag_chunks = [build_submit_description(args)]
    infiles = build_input_files(args)

    for f in infiles:
        dag_chunks.append(build_job_block(args, f))

    gcd_correction_dag = "\n".join(dag_chunks)
    with open("gcd_correction.dag", "w") as f:
        f.write(gcd_correction_dag)

if __name__ == "__main__":
    main()