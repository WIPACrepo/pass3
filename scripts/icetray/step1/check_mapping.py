from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from manifest_utils import extract_manifest_members_from_file, extract_manifest_uuid_from_file, is_manifest_file_path


DEFAULT_GRL_PATH = Path(__file__).resolve().parents[3] / "data" / "grl.pass3"


def remove_extension(path: Path) -> Path:
    suffixes = "".join(path.suffixes)
    return Path(str(path).replace(suffixes, ""))


def get_run_number(file: Path) -> int:
    s = str(file)
    try:
        if s.startswith("ukey"):
            return int(s.split("_")[4][3:])
        return int(s.split("_")[2][3:])
    except Exception as exc:
        raise ValueError(f"File {s} is causing issues when extracting run number") from exc


def load_grl(grl_path: Path) -> dict[int, bool]:
    text = grl_path.read_text().strip()
    if not text:
        return {}

    if text[0] == "{":
        payload = json.loads(text)
        runs = payload.get("runs", []) if isinstance(payload, dict) else []
        grl: dict[int, bool] = {}
        for record in runs:
            if not isinstance(record, dict):
                continue
            run = record.get("run")
            if run is None:
                continue
            grl[int(run)] = bool(record.get("good_i3", False))
        return grl

    grl = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        grl[int(line)] = True
    return grl


def get_outfilename(infile: Path) -> Path:
    infilename = str(remove_extension(infile))
    infilenwords = infilename.split("_")
    if infilename.startswith("ukey") or infilename.startswith("key"):
        outfilewords = ["Pass3", "Step1"] + infilenwords[3:]
    elif infilename.startswith("PFRaw"):
        outfilewords = ["Pass3", "Step1"] + infilenwords[1:]
    else:
        raise ValueError(f"Unexpected infile name format: {infile}")
    return Path("_".join(outfilewords) + ".i3.zst")

def find_manifest_files(path: Path) -> list[Path]:
    if path.is_file():
        return [path]

    manifests = sorted(
        member for member in path.iterdir()
        if is_manifest_file_path(member)
    )
    return manifests


def check_manifest(manifest_path: Path, output_dir: Path, grl: dict[int, bool]) -> dict[str, object]:
    missing: list[tuple[str, str]] = []
    invalid_inputs: list[str] = []
    invalid_run_numbers: list[str] = []
    not_in_grl: list[dict[str, object]] = []
    not_good_i3: list[dict[str, object]] = []
    input_count_in_grl = 0
    output_count_in_grl = 0
    output_count = 0

    for infile_name in extract_manifest_members_from_file(manifest_path):
        try:
            run_number = get_run_number(Path(infile_name))
        except ValueError:
            invalid_run_numbers.append(infile_name)
            continue

        if run_number not in grl:
            not_in_grl.append({"input_file": infile_name, "run": run_number})
            continue

        input_count_in_grl += 1

        try:
            expected_output = get_outfilename(Path(infile_name)).name
        except ValueError:
            invalid_inputs.append(infile_name)
            continue

        output_exists = (output_dir / expected_output).exists()
        if output_exists:
            output_count_in_grl += 1

        if not grl[run_number]:
            not_good_i3.append({"input_file": infile_name, "run": run_number})
            continue

        if not output_exists:
            missing.append((infile_name, expected_output))
            continue

        output_count += 1

    return {
        "input_count_in_grl": input_count_in_grl,
        "output_count_in_grl": output_count_in_grl,
        "output_count": output_count,
        "missing_outputs": [
            {
                "input_file": infile_name,
                "expected_output": expected_output,
            }
            for infile_name, expected_output in missing
        ],
        "invalid_inputs": invalid_inputs,
        "invalid_run_numbers": invalid_run_numbers,
        "not_in_grl": not_in_grl,
        "not_good_i3": not_good_i3,
    }


def report_has_issues(report: dict[str, object]) -> bool:
    return any(
        report[key]
        for key in ("missing_outputs", "invalid_inputs", "invalid_run_numbers", "not_in_grl", "not_good_i3")
    )


def build_manifest_report(manifest_path: Path, grl: dict[int, bool], grl_path: Path) -> dict[str, object]:
    output_dir = manifest_path.parent
    manifest_members = extract_manifest_members_from_file(manifest_path)
    manifest_uuid = extract_manifest_uuid_from_file(manifest_path)
    results = check_manifest(manifest_path, output_dir, grl)
    return {
        "manifest": str(manifest_path),
        "manifest_uuid": manifest_uuid,
        "output_dir": str(output_dir),
        "grl": str(grl_path),
        "input_count": len(manifest_members),
        **results,
    }


def write_issue_report(manifest_path: Path, report: dict[str, object]) -> Path:
    manifest_uuid = report.get("manifest_uuid")
    filename_stem = str(manifest_uuid) if manifest_uuid else manifest_path.stem
    report_path = manifest_path.parent / f"{filename_stem}.check_mapping.summary.json"
    report_with_path = dict(report)
    report_with_path["report_file"] = str(report_path)
    report_path.write_text(json.dumps(report_with_path, indent=2, sort_keys=True) + "\n")
    return report_path


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Check that each input file listed in a Step1 manifest has a corresponding "
            "Pass3 Step1 output file in the same directory."
        )
    )
    parser.add_argument(
        "--path",
        type=Path,
        required=True,
        help="Path to a manifest file or a directory containing manifest file(s) and output files",
    )
    parser.add_argument(
        "--grl",
        type=Path,
        default=DEFAULT_GRL_PATH,
        help="Path to GRL file. Supports either one-run-per-line text or JSON with runs[].good_i3",
    )
    args = parser.parse_args()

    manifests = find_manifest_files(args.path)
    if not manifests:
        raise FileNotFoundError(f"No manifest files found in {args.path}")

    grl = load_grl(args.grl)
    reports = [build_manifest_report(manifest_path, grl, args.grl) for manifest_path in manifests]
    issue_report_files: list[str] = []
    for manifest_path, report in zip(manifests, reports):
        if not report_has_issues(report):
            continue

        report_path = write_issue_report(manifest_path, report)
        report["report_file"] = str(report_path)
        issue_report_files.append(str(report_path))

    had_missing = any(report_has_issues(report) for report in reports)
    print(json.dumps({
        "path": str(args.path),
        "grl": str(args.grl),
        "issue_report_files": issue_report_files,
        "manifest_count": len(reports),
        "manifests": reports,
    }, indent=2, sort_keys=True))

    return 1 if had_missing else 0


if __name__ == "__main__":
    sys.exit(main())
