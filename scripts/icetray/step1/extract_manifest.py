import argparse
import shutil
import zipfile
from pathlib import Path


def get_archive_key(archive_path: Path) -> str:
	if archive_path.suffix.lower() != ".zip":
		raise ValueError(f"Archive must end with .zip: {archive_path}")
	return archive_path.stem


def get_archive_date_parts(archive_path: Path) -> tuple[str, str]:
	mmdd = archive_path.parent.name
	year = archive_path.parent.parent.name

	if len(year) != 4 or not year.isdigit():
		raise ValueError(f"Archive path must include YYYY before MMDD: {archive_path}")
	if len(mmdd) != 4 or not mmdd.isdigit():
		raise ValueError(f"Archive path must include MMDD as the direct parent directory: {archive_path}")

	return year, mmdd


def find_manifest_member(bundle_zip_path: Path) -> str:
	archive_key = get_archive_key(bundle_zip_path)
	expected_names = {
		f"{archive_key}.metadata.json",
		f"{archive_key}.metadata.ndjson",
	}

	with zipfile.ZipFile(bundle_zip_path) as zf:
		matches = [
			member for member in zf.namelist()
			if Path(member).name in expected_names
		]

	if not matches:
		raise FileNotFoundError(
			f"No manifest named {archive_key}.metadata.json or {archive_key}.metadata.ndjson found in {bundle_zip_path}"
		)
	if len(matches) > 1:
		raise RuntimeError(f"Multiple matching manifests found in {bundle_zip_path}: {sorted(matches)}")

	return matches[0]


def extract_manifest(bundle_zip_path: Path, output_root: Path) -> Path:
	year, mmdd = get_archive_date_parts(bundle_zip_path)
	manifest_member = find_manifest_member(bundle_zip_path)
	destination_dir = output_root / year / mmdd
	destination_dir.mkdir(parents=True, exist_ok=True)
	destination_path = destination_dir / Path(manifest_member).name

	with zipfile.ZipFile(bundle_zip_path) as zf:
		with zf.open(manifest_member) as src_fh, destination_path.open("wb") as dst_fh:
			shutil.copyfileobj(src_fh, dst_fh)

	return destination_path


def main() -> None:
	parser = argparse.ArgumentParser(
		description=(
			"Extract <archive-stem>.metadata.json or <archive-stem>.metadata.ndjson "
			"from a zip archive into <output-root>/YYYY/MMDD/."
		)
	)
	parser.add_argument("archive", type=Path, help="Path to the source zip archive")
	parser.add_argument("output_root", type=Path, help="Root directory where YYYY/MMDD/manifest will be written")
	args = parser.parse_args()

	destination_path = extract_manifest(args.archive, args.output_root)
	print(destination_path)


if __name__ == "__main__":
	main()
