import argparse
import json
import math
import os
import random
import zipfile
from datetime import datetime, timezone

from typing import NoReturn, Optional, Dict, Tuple, List
from pathlib import Path
from collections import defaultdict
from itertools import islice


def normalize_member_path(path: str) -> str:
    path = str(path).strip()
    if path.startswith("./"):
        path = path[2:]
    # In manifests, logical_name is often absolute (/data/...). In zip members
    # it is typically relative (data/...). Normalize to relative.
    if path.startswith("/"):
        path = path[1:]
    return path


def member_key(member_path: str) -> str:
    """Canonical key for duplicate detection.

    User-requested behavior: use only the filename (no directories).
    """
    p = normalize_member_path(member_path)
    return Path(p).name


def resolve_local_bundle_path(archive_bundle: Path, bundledir: Optional[Path]) -> Path:
    """Resolve an archive-path bundle to a local file path.

    If the archive_bundle already exists locally, it is returned.
    Otherwise, if bundledir is provided, we look for:
    - bundledir/YYYY/MMDD/<bundle.name>  (expected TACC layout)
    - bundledir/<bundle.name>           (fallback)
    """
    if archive_bundle.exists():
        return archive_bundle
    if bundledir is not None:
        parts = str(archive_bundle).split("/")
        year = parts[-5] if len(parts) >= 5 else None
        mmdd = parts[-2] if len(parts) >= 2 else None

        if year and mmdd:
            candidate = bundledir / year / mmdd / archive_bundle.name
            if candidate.exists():
                return candidate

        candidate_flat = bundledir / archive_bundle.name
        if candidate_flat.exists():
            return candidate_flat
    raise FileNotFoundError(
        f"Bundle not found locally: {archive_bundle} (try --bundledir /scratch/.../tmp.v2)"
    )


def _extract_members_from_manifest_text(text: str) -> list[str]:
    text = text.strip()
    if not text:
        return []

    # JSON array
    if text[0] == "[":
        try:
            data = json.loads(text)
            if isinstance(data, list):
                return [member_key(str(x)) for x in data if str(x).strip()]
        except Exception:
            pass

    files: list[str] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except Exception:
            files.append(member_key(line))
            continue

        if isinstance(obj, str):
            if obj.strip():
                files.append(normalize_member_path(obj))
            continue

        if isinstance(obj, dict):
            # Your manifest format uses logical_name for members.
            if obj.get("logical_name"):
                files.append(member_key(obj["logical_name"]))
                continue
            for key in ("file", "filename", "name", "path", "member", "archive_member"):
                if key in obj and obj[key]:
                    files.append(member_key(obj[key]))
                    break
            continue

        files.append(member_key(str(obj)))

    return [f for f in files if f]


def _read_ndjson_manifest(path: Path) -> list[str]:
    """Read a bundle manifest in JSON or NDJSON form.

    We intentionally accept a few common encodings:
    - A JSON array of strings
    - NDJSON with each line being a string
    - NDJSON with each line being an object with a plausible filename key
    """
    return _extract_members_from_manifest_text(path.read_text())


def _read_manifest_from_zip(bundle: Path) -> Optional[Tuple[str, str]]:
    """Return (manifest_text, manifest_member_name) if a *.ndjson exists in the zip."""
    try:
        with zipfile.ZipFile(bundle) as zf:
            ndjson_members = [n for n in zf.namelist() if n.lower().endswith(".ndjson")]
            if not ndjson_members:
                return None
            member = sorted(ndjson_members)[0]
            with zf.open(member) as fh:
                text = fh.read().decode("utf-8", errors="replace")
            return text, member
    except Exception:
        return None


def find_bundle_manifest(bundle: Path) -> Optional[Path]:
    """Find a sidecar manifest for a bundle.

    We try the most common convention: <bundle>.ndjson (i.e. .zip -> .ndjson)
    and then fall back to searching in the same directory.
    """
    candidates: list[Path] = []
    if bundle.suffix == ".zip":
        direct = bundle.with_suffix(".ndjson")
        if direct.exists():
            return direct
        candidates.extend(sorted(bundle.parent.glob(bundle.stem + "*.ndjson")))
    else:
        candidates.extend(sorted(bundle.parent.glob(bundle.name + "*.ndjson")))

    if not candidates:
        return None
    # Prefer exact stem match
    for c in candidates:
        if c.stem == bundle.stem:
            return c
    return candidates[0]


def get_bundle_manifest_members(bundle: Path) -> Tuple[List[str], Optional[str]]:
    """Return (members, source).

    source is one of:
    - "sidecar:<path>"
    - "zip:<member>"
    - None (no manifest found)
    """
    sidecar = find_bundle_manifest(bundle)
    if sidecar and sidecar.exists():
        try:
            return _read_ndjson_manifest(sidecar), f"sidecar:{sidecar}"
        except Exception:
            pass

    if bundle.exists():
        z = _read_manifest_from_zip(bundle)
        if z is not None:
            text, member = z
            return _extract_members_from_manifest_text(text), f"zip:{member}"

    return [], None


def compute_duplicate_skip_lists(
    bundles: list[Path],
    local_bundle_by_archive: Optional[Dict[Path, Path]] = None,
) -> dict[Path, dict[str, object]]:
    """Compute which files should be skipped for each bundle due to duplicates.

    Deterministic rule: the lexicographically-smallest bundle path is the winner
    for any given member filename; all other bundles listing that member skip it.
    """
    winner_for_member: dict[str, Path] = {}
    dupes_for_bundle: dict[Path, list[str]] = {b: [] for b in bundles}
    source_for_bundle: Dict[Path, Optional[str]] = {b: None for b in bundles}

    for bundle in sorted(bundles, key=lambda p: str(p)):
        local_bundle = local_bundle_by_archive.get(bundle, bundle) if local_bundle_by_archive else bundle
        members, source = get_bundle_manifest_members(local_bundle)
        source_for_bundle[bundle] = source
        if not members:
            continue

        for member in members:
            member = member.strip()
            if not member:
                continue
            if member not in winner_for_member:
                winner_for_member[member] = bundle
            elif winner_for_member[member] != bundle:
                dupes_for_bundle[bundle].append(member)

    result: dict[Path, dict[str, object]] = {}
    now = datetime.now(timezone.utc).isoformat()
    for bundle in bundles:
        dups = sorted(set(dupes_for_bundle.get(bundle, [])))
        winners = {m: str(winner_for_member[m]) for m in dups if m in winner_for_member}
        local_bundle = local_bundle_by_archive.get(bundle, bundle) if local_bundle_by_archive else bundle
        result[bundle] = {
            "bundle": str(bundle),
            "local_bundle": str(local_bundle),
            "created": now,
            "manifest": source_for_bundle.get(bundle),
            "skip_members": dups,
            "winners": winners,
        }
    return result

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
                         duplicate_skip_json_by_bundle: Optional[Dict[Path, Path]] = None,
                         transferbundles: bool = False,
                         local_bundle_by_archive: Optional[Dict[Path, Path]] = None,
                         script: Path = Path("/opt/pass3/scripts/icetray/step1/run_step1.py"),
                         ) -> NoReturn:
    file = file.parent / (file.name + str(increment))
    with Path.open(file, "w") as f:
        for i, (bundle, checksum) in enumerate(bundles.items()):
            year = get_year_filepath(str(bundle))
            date = get_date_filepath(str(bundle))
            local_bundle = local_bundle_by_archive.get(bundle, bundle) if local_bundle_by_archive else bundle
            f.write(f"{i}  /opt/apps/tacc-apptainer/1.3.3/bin/apptainer ")
            f.write(f"exec -B /home1/04799/tg840985/pass3:/opt/pass3 ")
            f.write(f"-B /work/04799/tg840985/vista/splines/splines:/cvmfs/icecube.opensciencegrid.org/data/photon-tables/splines ")
            f.write(f"-B /work2 -B /scratch {apptainer_container} {env_shell} ")
            f.write(f"python3 {script} --bundle {local_bundle} --gcddir {gcddir} ")
            f.write(f"--outdir {outdir}/{year}/{date} --checksum {checksum} ")
            f.write(f"--scratchdir {scratchdir} --grl {grl} ")
            f.write(f"--badfiles {badfiles} ")
            f.write(f"--filecatalogsecret {filecatalogsecret}")
            if duplicate_skip_json_by_bundle is not None and bundle in duplicate_skip_json_by_bundle:
                f.write(f" --duplicate-skip-json {duplicate_skip_json_by_bundle[bundle]}")
            if transferbundles:
                f.write(" --transferbundle")
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

def get_file_checksums(file_path: Path) -> dict[Path, str]:
    """"Reading in the checksum file"""
    tmp_checksums: dict[Path, str] = {}
    with Path.open(file_path, "r") as f:
        while line := f.readline():
            line = line.rstrip()
            # Assuming each line is formatted (<sha512sum> <absolute_file_path>) and space separated
            checksum, archive_path = line.split()
            tmp_checksums[Path(archive_path)] = checksum
    return tmp_checksums


def _sorted_dict_by_key(d: dict[Path, str]) -> dict[Path, str]:
    return dict(sorted(d.items(), key=lambda kv: str(kv[0])))

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
    filtered_checksum = _sorted_dict_by_key(filtered_checksum)
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
    filtered_checksum = { key: value for key, value in tmp_checksums.items() 
                         if key.name in bundle_names}
    filtered_checksum = _sorted_dict_by_key(filtered_checksum)
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
    parser.add_argument(
        "--transferbundles",
        help="Opt-in: have run_step1.py transfer bundles from the archiver (requires it to be configured there)",
        action="store_true",
    )
    parser.add_argument(
        "--bundledir",
        help="Base directory containing local bundle .zip files (expects YYYY/MMDD/<zip> under it, e.g. /scratch/.../tmp.v2/YYYY/MMDD)",
        type=Path,
        required=False,
    )
    parser.add_argument(
        "--duplicate-skip-dir",
        help=(
            "Directory to write per-bundle duplicate-skip JSON files. "
            "If omitted, writes next to --submitfile."
        ),
        type=Path,
        required=False,
    )
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
    elif args.bundles and len(args.bundles) > 0:
        checksums = get_checksums_bundles(args.checksum_file, args.bundles, args.numnodes)
    else:
        raise RuntimeError("Need to provide a year and month or list of bundles to process")

    # Build duplicate skip lists across *all* selected bundles (not per chunk)
    all_bundles: list[Path] = []
    for cs in checksums:
        all_bundles.extend(list(cs.keys()))

    local_bundle_by_archive: dict[Path, Path] = {}
    for b in all_bundles:
        local_bundle_by_archive[b] = resolve_local_bundle_path(b, args.bundledir)

    duplicate_skip_payload = compute_duplicate_skip_lists(all_bundles, local_bundle_by_archive=local_bundle_by_archive)

    duplicate_skip_json_by_bundle: dict[Path, Path] = {}
    for bundle, payload in duplicate_skip_payload.items():
        if args.duplicate_skip_dir is not None:
            outdir_bundle = args.duplicate_skip_dir
        else:
            outdir_bundle = args.submitfile.resolve().parent
        outdir_bundle.mkdir(parents=True, exist_ok=True)
        out_json = outdir_bundle / f"{bundle.name}.duplicate_skip.json"
        with out_json.open("w") as fh:
            json.dump(payload, fh, indent=2, sort_keys=True)
        duplicate_skip_json_by_bundle[bundle] = out_json

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
            args.filecatalogsecret,
            duplicate_skip_json_by_bundle=duplicate_skip_json_by_bundle,
            transferbundles=args.transferbundles,
            local_bundle_by_archive=local_bundle_by_archive)

    write_slurm_file(args.submitfile,
                    args.slurmqueue,
                    str(args.submitfile),
                    args.numnodes,
                    args.allocation,
                    args.multiprogfile,
                    len(checksums),
                    args.bundlesready)
