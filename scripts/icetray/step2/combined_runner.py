#!/usr/bin/env python3
"""
THIS IS A TEST SCRIPT, NOT MEANT FOR PRODUCTION USE
Launch one worker script over many input files to allow for shared photospline
tables using python subprocess, i.e. this invokes separate python instances
for each file. Photospline-service internally handles the shared memory
management for the tables. 

This is to decrease the memory footprint of running millipoede and monopod in the final muon and cascade filter reconstruction. Currently the it takes ~7-8
 GB 


"""

from __future__ import annotations

import argparse
import os
import socket
import subprocess
import sys
import time
import uuid

from collections import deque
from pathlib import Path

def default_shared_label() -> str:
    scheduler_id = (
        os.environ.get("SLURM_JOB_ID")
        or os.environ.get("CONDOR_JOB_ID")
        or os.environ.get("JOB_ID")
    )
    if scheduler_id:
        return f"job-{scheduler_id}"

    return f"job-{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:8]}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Launch one worker script over many input files with one shared photospline label."
    )
    parser.add_argument(
        "--worker-script",
        required=True,
        help="Path to the worker script that should run once per input file.",
    )
    parser.add_argument(
        "--jobs",
        type=int,
        default=max(1, os.cpu_count() or 1),
        help="Maximum number of worker processes to run concurrently.",
    )
    parser.add_argument(
        "--shared-label",
        default=None,
        help="Optional explicit shared label. If omitted, derive one once per wrapper invocation.",
    )
    parser.add_argument(
        "--input_files",
        nargs="+",
        help="Input files to distribute across worker invocations.",
    )
    parser.add_argument(
        "--gcd-file",
        required=True,
        help="GCD file to use for all worker invocations.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        type=Path,
        help="Optional output directory. If omitted, outputs will be placed next to their inputs.",
    )
    return parser.parse_args()


def build_env(shared_label: str) -> dict[str, str]:
    env = os.environ.copy()
    env["I3PHOTOSPLINESERVICE_SHARE_MEMORY"] = "1"
    env["I3PHOTOSPLINESERVICE_SHARED_LABEL"] = shared_label
    return env

def get_output_file_name(
        input_file: Path,
        output_dir: Path = None) -> Path:
    if output_dir is None:
        output_dir = Path(".")
    return output_dir / f"{input_file.stem}_output{input_file.suffix}"

def construct_prod_filter_command(worker_script: Path, 
                                 input_file: Path,
                                 output_file: Path,
                                 output_file_unfiltered: Path,
                                 gaps_file: Path,
                                 nano_dst_file: Path,
                                 gcd_file: Path) -> list[str]:
    return [
        sys.executable,
        str(worker_script),
        "--prettyprint",
        "-i", f"{input_file}",
        "-o", f"{output_file}",
        "-g", f"{gcd_file}",
        "--output-unfiltered", f"{output_file_unfiltered}",
        "--nano-dst", f"{nano_dst_file}",
        "--gapsfile", f"{gaps_file}"
    ]

def main() -> int:
    args = parse_args()

    worker_script = Path(args.worker_script)
    if not worker_script.exists():
        raise SystemExit(f"worker script not found: {worker_script}")

    shared_label = args.shared_label or default_shared_label()
    env = build_env(shared_label)

    print(f"[setup] shared label: {shared_label}", flush=True)
    print(f"[setup] max parallel jobs: {args.jobs}", flush=True)

    pending = deque(Path(path) for path in args.input_files)
    running: list[tuple[Path, subprocess.Popen[bytes]]] = []
    failures: list[tuple[Path, int]] = []

    while pending or running:
        while pending and len(running) < args.jobs:
            input_path = pending.popleft()
            outfile = get_output_file_name(input_path, args.output_dir)
            cmd = construct_prod_filter_command(
                worker_script=args.worker_script,
                input_file=input_path,
                output_file=outfile,
                output_file_unfiltered=outfile.with_suffix(".unfiltered" + input_path.suffix),
                gaps_file=outfile.with_suffix(".gaps.txt"),
                nano_dst_file=outfile.with_suffix(".nanodst.json.gz"),
                gcd_file=args.gcd_file
            )
            proc = subprocess.Popen(cmd, env=env)
            print(f"[launch] pid={proc.pid} input={input_path}", flush=True)
            running.append((input_path, proc))

        next_running: list[tuple[Path, subprocess.Popen[bytes]]] = []
        for input_path, proc in running:
            return_code = proc.poll()
            if return_code is None:
                next_running.append((input_path, proc))
                continue

            print(
                f"[exit] pid={proc.pid} code={return_code} input={input_path}",
                flush=True,
            )
            if return_code != 0:
                failures.append((input_path, return_code))

        running = next_running
        if running:
            time.sleep(0.1)

    if failures:
        print("[summary] one or more workers failed:", file=sys.stderr)
        for input_path, return_code in failures:
            print(f"  {input_path}: exit code {return_code}", file=sys.stderr)
        return 1

    print("[summary] all workers completed successfully", flush=True)
    return 0


if __name__ == "__main__":
    print(f"[start] combined runner with PID {os.getpid()}", flush=True)
    print("THIS IS A TEST SCRIPT, NOT MEANT FOR PRODUCTION USE", flush=True)
    raise SystemExit(main())