import json
import zipfile
from pathlib import Path
from typing import Optional


def is_manifest_member_name(member_name: str, archive_key: Optional[str] = None) -> bool:
    basename = Path(member_name).name
    if archive_key is not None:
        return basename in {
            f"{archive_key}.metadata.json",
            f"{archive_key}.metadata.ndjson",
        }

    lower = basename.lower()
    return lower.endswith("metadata.ndjson") or lower.endswith(".metadata.json")


def is_manifest_file_path(path: Path, archive_key: Optional[str] = None) -> bool:
    return path.is_file() and is_manifest_member_name(path.name, archive_key=archive_key)


def find_manifest_members_in_zip(bundle_zip_path: Path, archive_key: Optional[str] = None) -> list[str]:
    with zipfile.ZipFile(bundle_zip_path) as zf:
        return sorted(
            member for member in zf.namelist()
            if is_manifest_member_name(member, archive_key=archive_key)
        )


def read_manifest_from_zip(bundle_zip_path: Path, archive_key: Optional[str] = None) -> Optional[tuple[str, str]]:
    manifest_members = find_manifest_members_in_zip(bundle_zip_path, archive_key=archive_key)
    if not manifest_members:
        return None

    manifest_member = manifest_members[0]
    with zipfile.ZipFile(bundle_zip_path) as zf:
        with zf.open(manifest_member) as handle:
            text = handle.read().decode("utf-8", errors="replace")
    return text, manifest_member


def manifest_record_member(record: object) -> Optional[str]:
    if isinstance(record, str):
        return record.strip() or None

    if not isinstance(record, dict):
        return None

    for key in ("logical_name", "fileName", "file", "filename", "name", "path", "member", "archive_member"):
        value = record.get(key)
        if value:
            return str(value)

    return None


def manifest_record_sha512(record: object) -> Optional[str]:
    if not isinstance(record, dict):
        return None

    checksum_obj = record.get("checksum", {})
    if isinstance(checksum_obj, dict):
        return checksum_obj.get("sha512")

    checksum_type = str(record.get("checksumType", "")).lower()
    if checksum_type == "sha512" and isinstance(checksum_obj, str):
        return checksum_obj

    return None


def extract_manifest_members_from_text(text: str) -> list[str]:
    stripped = text.strip()
    if not stripped:
        return []

    if stripped[0] in "[{":
        try:
            payload = json.loads(stripped)
        except json.JSONDecodeError:
            payload = None

        if isinstance(payload, dict) and isinstance(payload.get("files"), list):
            members: list[str] = []
            for record in payload["files"]:
                member = manifest_record_member(record)
                if member:
                    members.append(Path(member).name)
            return members

        if isinstance(payload, list):
            members: list[str] = []
            for record in payload:
                member = manifest_record_member(record)
                if member:
                    members.append(Path(member).name)
            return members

    members: list[str] = []
    for line in stripped.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue

        member = manifest_record_member(record)
        if member:
            members.append(Path(member).name)

    return members


def extract_manifest_members_from_file(manifest_path: Path) -> list[str]:
    return extract_manifest_members_from_text(manifest_path.read_text())


def extract_manifest_checksums_from_text(text: str) -> dict[str, Optional[str]]:
    stripped = text.strip()
    if not stripped:
        return {}

    checksums: dict[str, Optional[str]] = {}

    if stripped[0] == "{":
        try:
            payload = json.loads(stripped)
        except json.JSONDecodeError:
            payload = None

        if isinstance(payload, dict) and isinstance(payload.get("files"), list):
            for record in payload["files"]:
                member = manifest_record_member(record)
                sha512 = manifest_record_sha512(record)
                if member and sha512:
                    checksums[Path(member).name] = sha512
            return checksums

    for line in stripped.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue

        member = manifest_record_member(record)
        sha512 = manifest_record_sha512(record)
        if member and sha512:
            checksums[Path(member).name] = sha512

    return checksums


def extract_manifest_checksums_from_zip(bundle_zip_path: Path, archive_key: Optional[str] = None) -> dict[str, Optional[str]]:
    checksums: dict[str, Optional[str]] = {}
    for manifest_member in find_manifest_members_in_zip(bundle_zip_path, archive_key=archive_key):
        with zipfile.ZipFile(bundle_zip_path) as zf:
            with zf.open(manifest_member) as handle:
                checksums.update(extract_manifest_checksums_from_text(handle.read().decode("utf-8", errors="replace")))
    return checksums