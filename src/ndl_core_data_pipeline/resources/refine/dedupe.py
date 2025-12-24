"""File deduplication utility using BLAKE2b.

Scans a folder recursively, computes BLAKE2b hash of each file's content (streaming), ignores files larger than `max_size_bytes`, and writes a text file listing the paths of non-duplicate files (one per line).

This utility hashes the raw file bytes (opens files in binary mode) so it works for both text files (CSV, TXT) and binary files (Parquet, XLSX, PDF, images, etc.).

Public API:
- deduplicate_folder(folder: str | Path, max_size_bytes: int = 100*1024*1024, output_file: Optional[str|Path] = None) -> List[Path]

CLI usage: python -m ndl_core_data_pipeline.resources.refine.dedupe --folder data/raw --output non_duplicates.txt
"""
from __future__ import annotations

from pathlib import Path
from typing import List, Optional
import hashlib
from tqdm import tqdm


CURRENT_DIR = Path(__file__).parent
RAW_DATA_DIR = (CURRENT_DIR / "../../../../data/raw").resolve()
TARGET_DIR = (CURRENT_DIR / "../../../../target").resolve()
OUT_FILE = TARGET_DIR / "non_duplicates.txt"

TARGET_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_MAX_SIZE = 100 * 1024 * 1024  # 100 MB
CHUNK_SIZE = 8 * 1024


def _hash_file(path: Path) -> str:
    """Compute BLAKE2b hash of a file streaming in chunks (reads bytes).

    Reading in binary mode ensures both text and binary files are hashed correctly.
    """
    h = hashlib.blake2b()
    with path.open("rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def deduplicate_folder(folder: str | Path = RAW_DATA_DIR,
                       max_size_bytes: int = DEFAULT_MAX_SIZE,
                       output_file: Optional[str | Path] = OUT_FILE) -> List[Path]:
    """Scan `folder` recursively and write non-duplicate file paths to `output_file`.

    Returns the list of Path objects for the chosen (non-duplicate) files.

    - Files larger than `max_size_bytes` are ignored (not listed).
    - For files with identical content, the first encountered path is kept and written.
    """
    folder_path = Path(folder)
    if not folder_path.exists():
        raise FileNotFoundError(f"Folder does not exist: {folder}")

    # Normalize output_file to a Path
    if output_file is None:
        output_file_path = Path.cwd() / "non_duplicates.txt"
    else:
        output_file_path = Path(output_file)

    if output_file_path.exists():
        output_file_path.unlink()

    all_files = [p for p in folder_path.rglob("*") if p.is_file() and p.name != ".DS_Store"]

    seen_hashes = {}
    kept_paths: List[Path] = []
    duplicate_count = 0
    skipped_large_count = 0

    print(f"print Scanning {len(all_files)} files")

    desc = f"Scanning files in {folder_path}"
    for path in tqdm(all_files, desc=desc, unit="file"):
        # try:
        #     size = path.stat().st_size
        # except OSError as e:
        #     print("Could not stat file %s: %s", path, e)
        #     continue
        #
        # if size > max_size_bytes:
        #     print("Ignoring large file %s (%d bytes)", path, size)
        #     skipped_large_count += 1
        #     continue

        try:
            file_hash = _hash_file(path)
        except Exception as e:
            print("Could not read/hash file %s: %s", path, e)
            continue

        if file_hash in seen_hashes:
            print("Duplicate found: %s (matches %s)", path, seen_hashes[file_hash])
            duplicate_count += 1
            continue

        seen_hashes[file_hash] = path
        kept_paths.append(path)

    write_report(kept_paths, output_file_path)
    print("Found %d duplicate files.", duplicate_count,)
    return kept_paths


def write_report(kept_paths: list[Path], output_file: str | Path | None):
    """
    Write the list of kept paths to the output file, one per line.
    :param kept_paths: List of Path objects to write
    :param output_file: Output file path
    """
    if output_file is None:
        output_file = Path.cwd() / "non_duplicates.txt"
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as out_f:
        for p in kept_paths:
            out_f.write(str(p) + "\n")
    print("Wrote %d non-duplicate paths to %s", len(kept_paths), output_path)


if __name__ == "__main__":
    deduplicate_folder()
