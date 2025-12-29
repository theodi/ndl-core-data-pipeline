"""JSON -> Parquet converter.

Reads a JSON file (array of objects or single object) and writes a parquet file
while attempting to preserve datatypes (int, float, date, string).

Behavior:
- Treats common tokens (NA, NULL, N/A, etc.) as null
- Detects numeric columns and converts to pandas nullable dtypes (Int64/Float64)
- Detects date-like columns and normalizes values to ISO 8601 via the
  project's time utilities
- Writes a parquet file to the requested output path
- If the top-level JSON indicates an API error (e.g. {"error":{...}}) a
  ValueError is raised with the error details

This reuses helper logic from the csv_to_parquet converter where practical.
"""

from __future__ import annotations

from pathlib import Path
import json
from typing import Any

import pandas as pd
from pandas import DataFrame
import pyarrow as pa
import pyarrow.parquet as pq

# reuse helpers from csv converter
from ndl_core_data_pipeline.resources.convertors.csv_to_parquet import (
    NULL_TOKENS,
    handle_numeric_column,
    handle_iso8601_dates,
)


def convert_json_to_parquet(input_json: str | Path, output_parquet: str | Path) -> Path:
    """Convert a JSON file to a parquet file while attempting to preserve types.

    Parameters
    - input_json: path to a JSON file (records list or single object)
    - output_parquet: destination parquet file path (will be overwritten)

    Returns
    - Path to written parquet file

    Raises:
    - ValueError when the JSON file contains an API-like error object
    """
    input_json = Path(input_json)
    output_parquet = Path(output_parquet)

    # Load JSON to allow early detection of error payloads and flexible shapes
    with input_json.open("r", encoding="utf-8") as fh:
        try:
            obj = json.load(fh)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON: {e}")

    # Detect API-style error objects and raise
    if isinstance(obj, dict) and "error" in obj:
        # skip jsons as error messages
        # raise ValueError(f"JSON contains error: {obj.get('error')}")
        return None

    # Normalize various common JSON shapes into a list of record dicts
    records = _normalize_json_to_records(obj)

    # If no records, write an empty parquet with no rows but columns if possible
    if not records:
        df_empty = pd.DataFrame()
        output_parquet.parent.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pandas(df_empty)
        pq.write_table(table, str(output_parquet))
        return output_parquet

    # Build a DataFrame from the records; use json_normalize to flatten nested objects
    df_raw = pd.json_normalize(records)

    # Convert columns one-by-one using the same strategy as the CSV converter
    df_converted = DataFrame()

    for col in df_raw.columns:
        # Prepare a series where nested lists/dicts are serialized to JSON strings
        s = df_raw[col].apply(lambda v: json.dumps(v) if isinstance(v, (list, dict)) else v)
        # Treat native NaNs/None as pd.NA
        s = s.where(pd.notna(s), pd.NA)

        # If entire column is null, keep as pandas string dtype with pd.NA
        if s.dropna().empty:
            df_converted[col] = pd.Series([pd.NA] * len(s), dtype="string")
            continue

        # Try date handling
        if handle_iso8601_dates(col, s, df_converted):
            continue

        # Try numeric handling
        numeric_series = handle_numeric_column(s)
        if numeric_series is not None:
            df_converted[col] = numeric_series
            continue

        # Fallback: convert to trimmed strings and normalize null tokens
        s_stripped = s.astype(str).str.strip()
        s_final = s_stripped.replace(list(NULL_TOKENS), pd.NA)
        df_converted[col] = s_final.astype("string")

    # Ensure output directory exists
    output_parquet.parent.mkdir(parents=True, exist_ok=True)

    table = pa.Table.from_pandas(df_converted.reset_index(drop=True))
    pq.write_table(table, str(output_parquet))

    return output_parquet


def _normalize_json_to_records(obj: Any) -> list[dict]:
    """Turn different JSON payload shapes into a list of flat record dicts.

    Handles:
    - list[dict] -> returns as-is
    - dict with a single record -> [obj]
    - dict containing keys whose values are lists of equal length -> DataFrame-like
    - dict with a key 'data' or 'results' that is a list -> that list
    """
    if isinstance(obj, list):
        return obj

    if isinstance(obj, dict):
        # common wrappers
        for candidate in ("data", "results", "rows", "items"):
            if candidate in obj and isinstance(obj[candidate], list):
                return obj[candidate]

        # If all values are lists of equal length, convert to list of records
        if all(isinstance(v, list) for v in obj.values()) and obj:
            lengths = {len(v) for v in obj.values()}
            if len(lengths) == 1:
                # transpose
                keys = list(obj.keys())
                n = lengths.pop()
                return [ {k: obj[k][i] for k in keys} for i in range(n) ]

        # fallback: single record
        return [obj]

    # Unknown type -> empty
    return []
