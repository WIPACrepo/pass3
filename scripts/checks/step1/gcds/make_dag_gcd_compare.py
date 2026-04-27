from __future__ import annotations

import json
from argparse import ArgumentParser
from pathlib import Path
from typing import Any

import numpy as np

PASS2_2011_2016_SUBDIR = Path("filtered/level2pass2a")
PASS2_2017_2023_SUBDIR = Path("filtered/level2")
PASS3_GCD_SUBDIR = Path("filtered/debug.v2/GCD")
PASS2_SUFFIXES = (".i3.zst", ".i3.gz")
PASS3_SUFFIXES = (".i3.zst", ".i3.gz")


def build_parser() -> ArgumentParser:
    parser = ArgumentParser(
        description=(
            "Generate an HTCondor DAG that compares Pass2 and Pass3 GCD files for "
            "runs listed in a GRL file."
        )
    )
    parser.add_argument(
        "--grl",
        type=Path,
        required=True,
        help=(
            "GRL JSON file used to identify runs to compare. Only runs with good_i3=true "
            "are included unless --include-bad-runs is set."
        ),
    )
    parser.add_argument(
        "--pass2-gcd-dirs",
        type=Path,
        default=Path("/data/exp/IceCube"),
        help=(
            "Root of the Pass2 GCD tree. The script searches "
            "{root}/YYYY/filtered/level2pass2a/MMDD/ for 2011-2016 runs and "
            "{root}/YYYY/filtered/level2/MMDD/ for 2017-2023 runs."
        ),
    )
    parser.add_argument(
        "--pass3-gcd-dirs",
        type=Path,
        default=Path("/data/ana/IceCube"),
        help=(
            "Root of the Pass3 GCD tree. The script expects files under "
            "{root}/YYYY/filtered/debug.v2/GCD/."
        ),
    )
    parser.add_argument(
        "--compare-script",
        type=Path,
        default=Path(__file__).with_name("compare_pass2_pass3_gcd.py").resolve(),
        help="Comparison script executed by each DAG node.",
    )
    parser.add_argument(
        "--dagman",
        type=Path,
        default=Path("gcd_compare.dag"),
        help="Path to the DAG file to write.",
    )
    parser.add_argument(
        "-o",
        "--outdir",
        type=Path,
        default=Path("."),
        help=(
            "Top-level directory for JSON outputs. Per-run outputs are written to "
            "{outdir}/YYYY/RUN/."
        ),
    )
    parser.add_argument(
        "-l",
        "--logdir",
        type=Path,
        default=Path("."),
        help="Directory for HTCondor stdout and stderr logs.",
    )
    parser.add_argument(
        "--include-bad-runs",
        action="store_true",
        default=False,
        help="Include all GRL entries, not just runs with good_i3=true.",
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


def parse_grl_timestamp(value: str | None) -> np.datetime64 | None:
    if not value:
        return None
    return np.datetime64(value.replace(" ", "T"), "ns")


def get_timestamp_year(value: np.datetime64) -> int:
    return value.astype("datetime64[Y]").astype(int) + 1970


def get_timestamp_month_day(value: np.datetime64) -> str:
    day_string = np.datetime_as_string(value.astype("datetime64[D]"), unit="D")
    return day_string[5:7] + day_string[8:10]


def load_json_grl(filepath: Path, include_bad_runs: bool) -> list[dict[str, Any]]:
    with filepath.open("r") as handle:
        data = json.load(handle)

    runs: list[dict[str, Any]] = []
    for run in data["runs"]:
        if not include_bad_runs and not run.get("good_i3", False):
            continue

        run_copy = dict(run)
        run_copy["good_tstart"] = parse_grl_timestamp(run.get("good_tstart"))
        run_copy["good_tstop"] = parse_grl_timestamp(run.get("good_tstop"))
        runs.append(run_copy)

    return runs


def parse_grl(filepath: Path, include_bad_runs: bool) -> list[dict[str, Any]]:
    return load_json_grl(filepath, include_bad_runs=include_bad_runs)


def get_run_year(run_record: dict[str, Any]) -> int:
    good_tstart = run_record.get("good_tstart")
    if good_tstart is not None:
        return get_timestamp_year(good_tstart)

    good_tstop = run_record.get("good_tstop")
    if good_tstop is not None:
        return get_timestamp_year(good_tstop)

    raise ValueError(f"Run {run_record['run']} is missing both good_tstart and good_tstop")


def get_run_month_day_dirs(run_record: dict[str, Any]) -> list[str]:
    month_day_dirs: list[str] = []

    for key in ("good_tstart", "good_tstop"):
        timestamp = run_record.get(key)
        if timestamp is None:
            continue

        month_day = get_timestamp_month_day(timestamp)
        if month_day not in month_day_dirs:
            month_day_dirs.append(month_day)

    return month_day_dirs


def find_gcd_candidates(base_dir: Path, run_number: int, suffixes: tuple[str, ...]) -> list[Path]:
    run_token = f"Run{run_number:08d}"
    candidates: list[Path] = []

    if not base_dir.exists():
        return candidates

    for suffix in suffixes:
        candidates.extend(sorted(base_dir.glob(f"*{run_token}*GCD{suffix}")))

    return sorted(set(candidates))


def build_pass2_search_dirs(root_dir: Path, run_record: dict[str, Any]) -> list[Path]:
    year = get_run_year(run_record)
    run_number = int(run_record["run"])
    run_dirname = f"Run{run_number:08d}"
    if 2011 <= year <= 2016:
        pass2_subdir = PASS2_2011_2016_SUBDIR
    elif 2017 <= year <= 2023:
        pass2_subdir = PASS2_2017_2023_SUBDIR
    else:
        raise ValueError(f"Run {run_record['run']} has unsupported Pass2 year {year}")

    return [
        root_dir / str(year) / pass2_subdir / month_day / run_dirname
        for month_day in get_run_month_day_dirs(run_record)
    ]


def resolve_unique_pass2_gcd(
    root_dir: Path,
    run_record: dict[str, Any],
    suffixes: tuple[str, ...],
) -> Path:
    run_number = int(run_record["run"])
    search_roots = build_pass2_search_dirs(root_dir, run_record)

    candidates: list[Path] = []
    searched_dirs: list[Path] = []
    for search_dir in search_roots:
        searched_dirs.append(search_dir)
        candidates.extend(find_gcd_candidates(search_dir, run_number, suffixes))

    unique_candidates = sorted(set(candidates))
    if not unique_candidates:
        searched_text = "\n  ".join(str(search_dir) for search_dir in searched_dirs)
        raise FileNotFoundError(
            f"No Pass2 GCD found for run {run_number}. Searched:\n  {searched_text}"
        )

    if len(unique_candidates) > 1:
        candidate_text = "\n  ".join(str(candidate) for candidate in unique_candidates)
        raise RuntimeError(
            f"Multiple Pass2 GCDs found for run {run_number}:\n  {candidate_text}"
        )

    return unique_candidates[0]


def resolve_unique_gcd(
    root_dir: Path,
    year: int,
    subdir: Path,
    run_number: int,
    suffixes: tuple[str, ...],
    label: str,
) -> Path:
    search_dir = root_dir / str(year) / subdir
    candidates = find_gcd_candidates(search_dir, run_number, suffixes)

    if not candidates:
        raise FileNotFoundError(
            f"No {label} GCD found for run {run_number} in {search_dir}"
        )

    if len(candidates) > 1:
        candidate_text = "\n  ".join(str(candidate) for candidate in candidates)
        raise RuntimeError(
            f"Multiple {label} GCDs found for run {run_number} in {search_dir}:\n  {candidate_text}"
        )

    return candidates[0]


def build_submit_description(args) -> str:
    lines = [
        "SUBMIT-DESCRIPTION gcd_compare {",
        f'executable   = {args.compare_script}',
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


def build_job_block(
    run_number: int,
    run_outdir: Path,
    pass2_gcd: Path,
    pass3_gcd: Path,
) -> str:
    diff_json = run_outdir / f"Run{run_number:08d}.gcd_differences.json"
    summary_json = run_outdir / f"Run{run_number:08d}.gcd_summary.json"
    job_name = f"gcd_compare_{run_number}"

    args = (
        
        f"--pass2-gcd {pass2_gcd} "
        f"--pass3-gcd {pass3_gcd} "
        f"--output-diffs-json {diff_json} "
        f"--output-summary-json {summary_json}"
    )

    lines = [
        f"JOB {job_name} gcd_compare DIR {run_outdir}",
        f'VARS {job_name} run="{run_number}" args="{args}"',
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    args.outdir.mkdir(parents=True, exist_ok=True)
    args.logdir.mkdir(parents=True, exist_ok=True)

    runs = parse_grl(args.grl, include_bad_runs=args.include_bad_runs)
    if not runs:
        raise SystemExit("No runs found in the GRL after filtering.")

    dag_chunks = [build_submit_description(args)]
    scheduled_runs = 0
    skipped_runs = 0

    for run_record in runs:
        run_number = int(run_record["run"])

        try:
            year = get_run_year(run_record)
            pass2_gcd = resolve_unique_pass2_gcd(
                args.pass2_gcd_dirs,
                run_record,
                PASS2_SUFFIXES,
            )
            pass3_gcd = resolve_unique_gcd(
                args.pass3_gcd_dirs,
                year,
                PASS3_GCD_SUBDIR,
                run_number,
                PASS3_SUFFIXES,
                "Pass3",
            )
        except (FileNotFoundError, RuntimeError, ValueError) as exc:
            skipped_runs += 1
            print(f"Skipping run {run_number}: {exc}")
            continue

        run_outdir = args.outdir / str(year) / str(run_number)
        run_outdir.mkdir(parents=True, exist_ok=True)

        dag_chunks.append(
            build_job_block(
                run_number=run_number,
                run_outdir=run_outdir,
                pass2_gcd=pass2_gcd,
                pass3_gcd=pass3_gcd,
            )
        )
        scheduled_runs += 1

    if scheduled_runs == 0:
        raise SystemExit("No DAG jobs were created. Check the GRL and GCD directory roots.")

    args.dagman.parent.mkdir(parents=True, exist_ok=True)
    args.dagman.write_text("\n".join(dag_chunks))

    print(f"Wrote DAG to {args.dagman}")
    print(f"Scheduled {scheduled_runs} runs")
    print(f"Skipped {skipped_runs} runs")


if __name__ == "__main__":
    main()