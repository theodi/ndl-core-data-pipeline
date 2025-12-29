"""CSV -> Parquet converter using pyarrow.

Features:
- Reads CSV as strings first and treats common placeholders (NA/NULL/N/A etc.) as null
- Attempts to infer and coerce column types: integer, float, date, string
- Converts detected dates to ISO 8601 strings (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)
- Preserves nulls using pandas nullable dtypes and writes a parquet file at the given output path

Usage:
    convert_csv_to_parquet(input_csv, output_parquet)

"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from pandas import DataFrame
import pyarrow as pa
import pyarrow.parquet as pq

from charset_normalizer import from_path

# use project time utilities for ISO parsing/normalization
from ndl_core_data_pipeline.resources import time_utils


# Tokens to treat as null (case-sensitive and lowercase variants covered where appropriate)
NULL_TOKENS = {"NA", "N/A", "NULL", "null", "na", "n/a", "None", "NONE", "-", ""}


def _clean_numeric_string(s: pd.Series) -> pd.Series:
    """Normalize numeric-looking strings so they can be converted to numeric types.

    - strips whitespace
    - replaces common null tokens with pd.NA
    - removes thousands separators (commas and spaces)
    - removes currency symbols £ $ €
    - leaves percent signs for caller to remove if desired
    """
    s2 = s.astype(str).str.strip()
    # Treat exact tokens as missing
    s2 = s2.replace(list(NULL_TOKENS), pd.NA)
    # remove thousands commas and internal spaces
    s2 = s2.str.replace(r"[\s,]", "", regex=True)
    # remove common currency symbols
    s2 = s2.str.replace(r"[£$€]", "", regex=True)
    return s2


def convert_csv_to_parquet(input_csv: str | Path, output_parquet: str | Path) -> Path:
    """Read CSV and write a parquet file with inferred/preserved datatypes.

    Parameters
    - input_csv: path to csv file
    - output_parquet: output parquet path (file will be overwritten)

    Returns
    - Path to written parquet file
    """
    input_csv = Path(input_csv)
    output_parquet = Path(output_parquet)

    encoding_result = from_path(input_csv).best()
    if encoding_result is None:
        raise ValueError("Could not detect file encoding")

    df_raw = pd.read_csv(input_csv, dtype=str, keep_default_na=False, encoding=encoding_result.encoding)

    handle_null_values(df_raw)

    df_converted = pd.DataFrame()
    for col in df_raw.columns:
        s = df_raw[col]
        s_stripped = s.astype(str).str.strip()

        # If entire column is null, keep as string dtype with nulls
        if s.dropna().empty:
            df_converted[col] = pd.Series([pd.NA] * len(s), dtype="string")
            continue

        # If the column was detected and handled as a date, skip further processing
        if handle_iso8601_dates(col, s, df_converted):
            continue

        # Try numeric parsing via helper
        numeric_series = handle_numeric_column(s)
        if numeric_series is not None:
            df_converted[col] = numeric_series
            continue

        # Fallback: keep as string (trim whitespace), convert recognized null tokens to pd.NA
        s_final = s_stripped.replace(list(NULL_TOKENS), pd.NA)
        df_converted[col] = s_final.astype('string')

    # Convert pandas DataFrame to pyarrow table.
    # Reset the index so we avoid relying on the preserve_index parameter
    # (some static analyzers warn about the 'df' parameter when keyword args are used).
    df_for_table = df_converted.reset_index(drop=True)
    table = pa.Table.from_pandas(df_for_table)

    # Ensure output directory exists
    output_parquet.parent.mkdir(parents=True, exist_ok=True)

    # Write parquet file
    pq.write_table(table, str(output_parquet))

    return output_parquet


def handle_null_values(df_raw: DataFrame):
    na_values = list(NULL_TOKENS)
    for col in df_raw.columns:
        # Using .where to preserve non-matching values and replace matching tokens with pd.NA
        df_raw[col] = df_raw[col].where(~df_raw[col].isin(na_values), pd.NA)


def handle_numeric_column(s: pd.Series) -> pd.Series | None:
    """Attempt to coerce a string Series to numeric values.

    Returns a pandas Series with dtype 'Int64' or 'Float64' when conversion is
    successful for the majority of non-null entries, otherwise returns None.
    """
    # Normalize numeric-like strings
    s_clean = _clean_numeric_string(s)
    # Remove percent sign if present for numeric conversion
    s_numeric_candidate = s_clean.str.replace('%', '', regex=False)
    # If empty string remains, replace with pd.NA so to_numeric will coerce
    s_numeric_candidate = s_numeric_candidate.replace('', pd.NA)

    numeric_vals = pd.to_numeric(s_numeric_candidate, errors='coerce')
    numeric_ok = numeric_vals.notna().sum()
    total_non_null_num = s_numeric_candidate.notna().sum()

    # If >= 90% of non-null values are numeric, coerce the column
    if total_non_null_num > 0 and numeric_ok / total_non_null_num >= 0.9:
        nonnull_numeric = numeric_vals.dropna()
        if not nonnull_numeric.empty and (nonnull_numeric % 1 == 0).all():
            # integer - use pandas nullable integer dtype
            return numeric_vals.astype('Int64')
        return numeric_vals.astype('Float64')

    return None


def handle_iso8601_dates(col, s, df_converted) -> bool:
    """Detect and normalize date-like columns to ISO 8601 strings (UTC).

    Returns True and writes the normalized column into df_converted when the
    column is considered date-like; otherwise returns False.
    """
    # Use pandas string dtype for safe string operations; this preserves pd.NA
    s_stripped = s.astype('string').str.strip()
    # Explicitly treat common null tokens as NA (some CSVs may contain literal tokens)
    s_for_date = s_stripped.replace(list(NULL_TOKENS), pd.NA)

    # Detect time-only strings (e.g. '15:00' or '15:00:00') and avoid
    # treating columns that are predominantly time-only as dates. Pandas
    # will happily parse time-only strings into today's date + time which
    # is undesirable for columns that represent times of day.
    time_re = r"^\d{1,2}:\d{2}(?::\d{2}(?:\.\d+)?)?$"
    non_null_series = s_for_date.dropna()
    total_non_null = non_null_series.shape[0]
    if total_non_null > 0:
        time_only_count = non_null_series.astype(str).str.match(time_re).sum()
        # If the majority of non-null values are time-only, don't treat as date
        if (time_only_count / total_non_null) >= 0.5:
            return False

    # Parse using pandas; values that cannot be parsed become NaT
    parsed_dates = pd.to_datetime(s_for_date, errors="coerce")
    parsed_ok = parsed_dates.notna().sum()
    if 'total_non_null' not in locals() or total_non_null is None:
        total_non_null = s_for_date.notna().sum()

    # If a reasonable fraction of non-null values parse as dates, treat column as date
    # Threshold is 50% (useful for sparse columns with many blanks)
    if total_non_null > 0 and (parsed_ok / total_non_null) >= 0.5:
        # Determine if any parsed datetimes contain a time component
        has_time = ((parsed_dates.dt.hour.fillna(0) != 0)
                    | (parsed_dates.dt.minute.fillna(0) != 0)
                    | (parsed_dates.dt.second.fillna(0) != 0)).any()

        def _to_iso(val):
            # val is a pandas.Timestamp or NaT
            if pd.isna(val):
                return pd.NA
            # Convert to ISO via time_utils (normalizes to UTC)
            try:
                return time_utils.parse_to_iso8601_utc(val.isoformat())
            except Exception:
                # fallback formatting
                if has_time:
                    return val.strftime('%Y-%m-%dT%H:%M:%S')
                return val.strftime('%Y-%m-%d')

        iso_series = parsed_dates.apply(_to_iso).astype('string')
        df_converted[col] = iso_series
        return True

    return False
