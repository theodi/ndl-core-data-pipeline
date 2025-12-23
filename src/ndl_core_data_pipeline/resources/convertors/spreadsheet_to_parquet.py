"""Spreadsheet (XLSX / ODS) -> Parquet converter.

This module re-uses helpers from the existing CSV converter to apply the same
inference rules (null token handling, numeric/date inference) to spreadsheets.

Behaviour notes / assumptions:
- Reads all sheets with pandas.read_excel(..., sheet_name=None, dtype=str,
  keep_default_na=False) so every cell is initially treated as a string.
- If the workbook contains multiple sheets, `output_parquet` is treated as a
  directory: a parquet file per sheet will be written into that directory with
  the sheet name (sanitised) as filename ("{sheet_name}.parquet").
- If the workbook has a single sheet, `output_parquet` may be a file path.
  If a directory path is passed it will write into that directory using the
  sheet name as filename.
- Reuses null/numeric/date detection from the CSV converter to keep behaviour
  consistent across formats.

API:
    convert_spreadsheet_to_parquet(input_path, output_parquet)

Returns the Path to the written parquet file or directory (Path).
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Import reusable helpers from csv_to_parquet
from ndl_core_data_pipeline.resources.convertors.csv_to_parquet import (
    NULL_TOKENS,
    handle_null_values,
    handle_numeric_column,
    handle_iso8601_dates,
)


def _safe_sheet_filename(name: str) -> str:
    """Sanitise sheet name so it is safe as a filename.

    Replace path separators and whitespace with underscore and strip other
    problematic characters.
    """
    # Replace common separators / whitespace with underscore
    safe = name.replace("/", "_").replace("\\", "_").replace(" ", "_")
    # Keep alnum, dash, underscore and dot
    import re

    safe = re.sub(r"[^A-Za-z0-9._-]", "_", safe)
    # Avoid empty names
    if not safe:
        safe = "sheet"
    return safe


def _process_dataframe_and_write(df_raw: pd.DataFrame, out_path: Path) -> Path:
    """Convert a single dataframe (all strings) to parquet at out_path.

    This mirrors the logic used in the CSV converter: infer dates, numbers and
    fallback to string dtype while preserving nulls.
    """
    # Ensure we operate on a copy
    df_raw = df_raw.copy()

    # Normalise explicit null tokens
    handle_null_values(df_raw)

    df_converted = pd.DataFrame()
    for col in df_raw.columns:
        s = df_raw[col]
        s_stripped = s.astype(str).str.strip()

        # If entire column is null, keep as string dtype with nulls
        if s.dropna().empty:
            df_converted[col] = pd.Series([pd.NA] * len(s), dtype="string")
            continue

        # Date handling
        if handle_iso8601_dates(col, s, df_converted):
            continue

        # Numeric handling
        numeric_series = handle_numeric_column(s)
        if numeric_series is not None:
            df_converted[col] = numeric_series
            continue

        # Fallback to string dtype, convert recognized null tokens to pd.NA
        s_final = s_stripped.replace(list(NULL_TOKENS), pd.NA)
        df_converted[col] = s_final.astype("string")

    df_for_table = df_converted.reset_index(drop=True)
    table = pa.Table.from_pandas(df_for_table)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, str(out_path))
    return out_path


def convert_spreadsheet_to_parquet(input_path: str | Path, output_parquet: str | Path) -> Path:
    """Convert spreadsheet file to parquet(s).

    Parameters
    - input_path: path to .xlsx or .ods file
    - output_parquet: path to output file, or directory when multiple sheets

    Returns Path to written parquet file or directory containing per-sheet
    parquet files when multiple sheets exist.
    """
    input_path = Path(input_path)
    output_parquet = Path(output_parquet)

    # Read all sheets as strings (None -> dict of sheet_name -> DataFrame)
    sheets: Dict[str, pd.DataFrame] = pd.read_excel(
        input_path, sheet_name=None, dtype=str, keep_default_na=False
    )

    if len(sheets) > 1:
        # Treat output_parquet as directory path
        out_dir = output_parquet
        out_dir.mkdir(parents=True, exist_ok=True)

        written_paths = []
        for name, df in sheets.items():
            safe_name = _safe_sheet_filename(name)
            out_file = out_dir / f"{safe_name}.parquet"
            _process_dataframe_and_write(df, out_file)
            written_paths.append(out_file)

        return out_dir

    # Single sheet
    sheet_name, df = next(iter(sheets.items()))
    # If output_parquet is a directory, create file inside with sheet name
    if output_parquet.exists() and output_parquet.is_dir():
        safe_name = _safe_sheet_filename(sheet_name)
        out_file = output_parquet / f"{safe_name}.parquet"
        _process_dataframe_and_write(df, out_file)
        return out_file

    # If output_parquet ends with a slash or has no suffix and looks like a dir,
    # create parent directory
    if str(output_parquet).endswith("/") or output_parquet.suffix == "":
        output_parquet.mkdir(parents=True, exist_ok=True)
        safe_name = _safe_sheet_filename(sheet_name)
        out_file = output_parquet / f"{safe_name}.parquet"
        _process_dataframe_and_write(df, out_file)
        return out_file

    # Otherwise write to the provided file path
    _process_dataframe_and_write(df, output_parquet)
    return output_parquet

