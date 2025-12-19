"""Count files by extension under data/raw/data_gov_uk.

This script recursively walks the given directory (default: data/raw/data_gov_uk),
counts files grouped by extension, and prints the results. Files matching
the pattern "*_metadata.json" are ignored.

Usage:
    python -m src.ndl_core_data_pipeline.resources.count_extensions
or
    python src/ndl_core_data_pipeline/resources/count_extensions.py --root data/raw/data_gov_uk

Example output:
.csv	407
.html	306
.xlsx	294
.pdf	237
.json	57
.ods	55
.aspx	39
.zip	37
.xml	33
.exe	29
.xsl	19
.ashx	18
<no_ext>	18
.sqlite	11
.xlsm	8
.geojson	4
.application	2
"""
from __future__ import annotations

import os
import argparse
import fnmatch
from collections import Counter
from pathlib import Path
import sys
from typing import Counter as CounterType


IGNORE_PATTERN = "*_metadata.json"
CURRENT_DIR = Path(__file__).parent
DEFAULT_ROOT = os.path.join(CURRENT_DIR, "../../../data/raw/data_gov_uk")


def count_extensions(root: Path, ignore_pattern: str = IGNORE_PATTERN) -> CounterType[str]:
    """Recursively count files by extension under `root`.

    Args:
        root: Path to the directory to walk.
        ignore_pattern: fnmatch pattern for filenames to ignore.

    Returns:
        Counter mapping extension (including leading dot, e.g. '.json') or
        '<no_ext>' for files without an extension, to counts.
    """
    root = root.expanduser()
    counts: CounterType[str] = Counter()

    if not root.exists():
        raise FileNotFoundError(f"Root path does not exist: {root}")

    for p in root.rglob("*"):
        if not p.is_file():
            continue
        if fnmatch.fnmatch(p.name, ignore_pattern):
            # skip metadata files
            continue
        ext = p.suffix.lower() if p.suffix else "<no_ext>"
        counts[ext] += 1

    return counts


def format_counts(counts: CounterType[str]) -> str:
    """Format the counts into a human-readable multi-line string."""
    if not counts:
        return "No files found."

    lines = ["Extension\tCount"]
    # sort by descending count then extension name
    for ext, cnt in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
        lines.append(f"{ext}\t{cnt}")
    return "\n".join(lines)


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Count files by extension under a directory (ignores '*_metadata.json')."
    )
    p.add_argument(
        "--root",
        default=str(DEFAULT_ROOT),
        help=f"Root directory to scan (default: {DEFAULT_ROOT}).",
    )
    return p.parse_args(argv)


def main(argv=None) -> int:
    """Main entrypoint. Returns 0 on success, non-zero on error."""
    args = parse_args(argv)
    root = Path(args.root)

    try:
        counts = count_extensions(root)
    except FileNotFoundError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2

    print(format_counts(counts))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

