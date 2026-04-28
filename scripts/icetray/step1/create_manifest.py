from __future__ import annotations

import argparse
import hashlib
import json
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path


def iter_tar_members(bundle_zip_path: Path) -> list[str]:
    with zipfile.ZipFile(bundle_zip_path) as zf:
        return sorted(
            member for member in zf.namelist()
            if member.endswith(".tar.gz") and not member.endswith("/")
        )


def get_member_sha512sum(bundle_zip_path: Path, member_name: str) -> str:
    h = hashlib.sha512()
    buffer = bytearray(8192 * 1024)
    view = memoryview(buffer)

    with zipfile.ZipFile(bundle_zip_path) as zf:
        with zf.open(member_name) as member_fh:
            while True:
                chunk_size = member_fh.readinto(view)
                if chunk_size == 0:
                    break
                h.update(view[:chunk_size])

    return h.hexdigest()


def build_manifest_header(file_count: int) -> dict[str, object]:
    return {
        "uuid": uuid.uuid4().hex,
        "component": "bundler",
        "version": 3,
        "create_timestamp": datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%S"),
        "file_count": file_count,
    }


def build_manifest_records(bundle_zip_path: Path) -> list[dict[str, object]]:
    members = iter_tar_members(bundle_zip_path)
    if not members:
        raise FileNotFoundError(f"No .tar.gz files found in bundle {bundle_zip_path}")

    records: list[dict[str, object]] = [build_manifest_header(len(members))]
    for member_name in members:
        checksum = get_member_sha512sum(bundle_zip_path, member_name)
        records.append({
            "name": Path(member_name).name,
            "checksum": {"sha512": checksum},
        })

    return records


def write_manifest(bundle_zip_path: Path, output_path: Path) -> Path:
    records = build_manifest_records(bundle_zip_path)
    output_path.write_text(
        "\n".join(json.dumps(record, sort_keys=True) for record in records) + "\n"
    )
    return output_path


def default_output_path(bundle_zip_path: Path) -> Path:
    return bundle_zip_path.with_name(f"{bundle_zip_path.stem}.metadata.ndjson")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Create a metadata manifest for a zip bundle containing .tar.gz files. "
            "The manifest is written as NDJSON with a bundle header followed by file records."
        )
    )
    parser.add_argument("--archive", type=Path, required=True, help="Path to the source zip archive")
    parser.add_argument(
        "--output",
        type=Path,
        help="Path to the output NDJSON manifest. Defaults to <archive-stem>.metadata.ndjson beside the archive",
    )
    args = parser.parse_args()

    output_path = args.output or default_output_path(args.archive)
    manifest_path = write_manifest(args.archive, output_path)
    print(manifest_path)


if __name__ == "__main__":
    main()

