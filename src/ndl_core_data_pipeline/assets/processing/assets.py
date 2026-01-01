from dagster import asset, AssetExecutionContext, StaticPartitionsDefinition
from pathlib import Path
from uuid import uuid4
import json
from typing import List, Optional, Dict, Any, Literal
from langdetect import detect, DetectorFactory

from ndl_core_data_pipeline.resources.refine.dedupe import deduplicate_folder
from ndl_core_data_pipeline.resources.token_counter import count_tokens

from ndl_core_data_pipeline.resources.convertors.html_extractor import (
    extract_text_from_file as extract_text_from_html_file,
    extract_text_from_html,
)
from ndl_core_data_pipeline.resources.convertors.pdf_extractor import (
    extract_text_from_pdf,
)
from ndl_core_data_pipeline.resources.convertors.json_to_parquet import (
    convert_json_to_parquet,
)
from ndl_core_data_pipeline.resources.convertors.csv_to_parquet import (
    convert_csv_to_parquet,
)
from ndl_core_data_pipeline.resources.convertors.spreadsheet_to_parquet import (
    convert_spreadsheet_to_parquet,
)

# added import for EU theme classifier
from ndl_core_data_pipeline.resources.embedding.eu_theme_classifier import classify_eu_themes
from ndl_core_data_pipeline.resources.refine.anonymizer import anonymize_text, run_batch_process

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import math
from tqdm import tqdm

MIN_TEXT_LENGTH = 200
SUPPORTED_FORMATS = ["text", "html", "htm", "xhtml", "csv", "xlsx", "xls", "pdf", "json", "ods"]
BATCH_SIZE = 1000  # number of metadata files per partition; adjust as needed

CURRENT_DIR = Path(__file__).parent
TARGET_DIR = (CURRENT_DIR / "../../../../target").resolve()
DATA_DIR = (CURRENT_DIR / "../../../../data").resolve()
PROCESSED_DIR = TARGET_DIR / "processed"
STRUCTURED_DIR = TARGET_DIR / "structured"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
STRUCTURED_DIR.mkdir(parents=True, exist_ok=True)

NON_DUPLICATE_FILES = TARGET_DIR / "non_duplicates.txt"
AGGREGATED_ASSET = TARGET_DIR / "ndl_core_dataset_aggregated.parquet"
TAGGED_ASSET = TARGET_DIR / "ndl_core_dataset_tagged.parquet"
FINAL_ASSET = TARGET_DIR / "ndl_core_dataset.parquet"

# Build a static partitions definition based on the number of deduplicated files (if available).
# This allows running individual partitions (batches) and supports backfills by rerunning failed partition keys.

def _build_partitions_def(batch_size: int = BATCH_SIZE) -> StaticPartitionsDefinition:
    if NON_DUPLICATE_FILES.exists():
        try:
            with NON_DUPLICATE_FILES.open("r", encoding="utf-8") as fh:
                lines = [l for l in fh.readlines() if l.strip()]
            total = len(lines)
            if total == 0:
                # default single partition
                return StaticPartitionsDefinition(["0"])
            n_parts = math.ceil(total / batch_size)
            keys = [str(i) for i in range(n_parts)]
            return StaticPartitionsDefinition(keys)
        except Exception:
            return StaticPartitionsDefinition(["0"])
    # if file not present yet (e.g., first run), create a reasonable default set of partitions
    default_parts = 100
    return StaticPartitionsDefinition([str(i) for i in range(default_parts)])

PARTITIONS_DEF = _build_partitions_def()

stats = dict()
stats_unsupported = dict()
data_file_not_found = list()

@asset(
    group_name="processing"
)
def deduplicated_records(context: AssetExecutionContext):
    """
    Deduplicate files in the raw folder and write non-duplicate file paths to txt file.
    :param context:
    :return:
    """
    deduplicate_folder()

@asset(
    group_name="processing",
    partitions_def=PARTITIONS_DEF,
    non_argument_deps={"deduplicated_records"}
)
def format_records(context: AssetExecutionContext):
    """
    Formats the deduplicated files in a partition-aware manner and generates per-partition parquet files.

    Each partition run processes a slice of the metadata file list and writes a partitioned parquet named
    `ndl_core_dataset_part_{partition_key}.parquet` into the processed folder. A separate aggregation step
    will combine all partition files into the single `ndl_core_dataset_aggregated.parquet`.
    """
    input_file = NON_DUPLICATE_FILES
    if not input_file.exists():
        context.log.warning(f"No deduplicated records file found at {input_file}")
        return
    with input_file.open("r", encoding="utf-8") as f:
        file_paths = [line.strip() for line in f.readlines() if line.strip()]
    context.log.info(f"Found {len(file_paths)} deduplicated files to format.")

    # Determine partition slice
    partition_key = getattr(context, "partition_key", None) or getattr(context, "asset_partition_key", None)
    if partition_key is None:
        # If no partition_key (materializing without partitions), process everything in a single run.
        start_idx = 0
        end_idx = len(file_paths)
        partition_key = "0"
    else:
        try:
            idx = int(partition_key)
        except Exception:
            context.log.error(f"Invalid partition key: {partition_key}. Expected integer string.")
            return
        start_idx = idx * BATCH_SIZE
        end_idx = min(start_idx + BATCH_SIZE, len(file_paths))

    selected_paths = file_paths[start_idx:end_idx]
    context.log.info(f"Partition {partition_key}: processing items {start_idx}..{end_idx} (count={len(selected_paths)})")

    final_meta_files = filter_supported_files(selected_paths)
    context.log.info(f"Processing {len(final_meta_files)} metadata json files to build parquet (partition {partition_key}).")

    rows: List[Dict[str, Any]] = []

    DetectorFactory.seed = 0
    stats_empty_text = 0
    index = 0
    failed_meta: List[str] = []
    per_meta_exceptions = 0
    for meta_path in final_meta_files:
        try:
            index = index + 1
            print(f"Processing {index} / {len(final_meta_files)} - {meta_path}")
            json_data = _read_json(meta_path)
            if "metadata" in json_data:
                meta =  json_data["metadata"]
            elif "meta" in json_data:
                meta = json_data["meta"]
            else:
                meta = json_data
            text = json_data.get("text", "")
            data_file = _find_associated_data_file(meta_path)
            data_format = "text"

            fmt = (meta.get("format") or "").lower()
            # If not provided, try to infer format from associated data file
            if not fmt and data_file:
                fmt = data_file.suffix.lower().lstrip('.')

            if fmt not in SUPPORTED_FORMATS:
                stats_unsupported[fmt] = stats_unsupported.get(fmt, 0) + 1
                continue
            if "hansard_gov_uk/scrapedxml" in str(meta_path):
                # skip XML folder, we will process json version
                continue

            stats[fmt] = stats.get(fmt, 0) + 1

            data_file_rel = ""
            if fmt in {"json", "csv", "xlsx", "xls", "ods"}:
                if data_file:
                    conv = _convert_structured_to_parquet(data_file)
                    if conv:
                        data_file_rel = str(conv.relative_to(STRUCTURED_DIR))
                    else:
                        data_file_not_found.append(str(meta_path))
                        continue
                    text = ""
                    data_format = "parquet"
                else:
                    data_file_not_found.append(str(meta_path))
                    continue

            if fmt in {"html", "htm", "xhtml", "pdf"}:
                if data_file:
                    if fmt in {"html", "htm", "xhtml"}:
                        text = extract_text_from_html_file(data_file)
                    elif fmt =="pdf":
                        text = extract_text_from_pdf(str(data_file))

            text = text.strip()
            if "<" in text and ">" in text:
                text = extract_text_from_html(text)

            if data_format == "text" and "texts" not in json_data:
                if not text or len(text.strip()) < MIN_TEXT_LENGTH:
                    # skip empty or problematic text data
                    stats_empty_text = stats_empty_text + 1
                    continue

            common_metadata = ["identifier", "title", "description", "source", "date", "collection_time", "open_type", "license", "tags",
                               "language", "format", "text", "word_count", "token_count", "data_file"]
            extra_metadata = {}
            for keys in meta:
                if keys not in common_metadata and meta[keys]:
                    extra_metadata[keys] = meta[keys]

            if "texts" in json_data:
                conversation_id = 1
                for conversation in json_data["texts"]:
                    if not conversation.get("text") or len(conversation.get("text").strip()) < MIN_TEXT_LENGTH:
                        # skip empty or problematic text data
                        continue
                    extra_metadata["speakers"] = conversation.get("speakers", [])
                    add_dataset_record(data_file_rel, data_format, extra_metadata, meta, rows, conversation["text"], " conversation_" + str(conversation_id))
                    conversation_id = conversation_id + 1
            else:
                add_dataset_record(data_file_rel, data_format, extra_metadata, meta, rows, text)
        except Exception as exc:
            # Record failed metadata and continue processing other files. This ensures a single bad file won't crash the partition.
            per_meta_exceptions += 1
            failed_meta.append(str(meta_path))
            context.log.error(f"Failed processing {meta_path}: {exc}")
            continue

    # Write per-partition parquet
    if rows:
        df = pd.DataFrame(rows)
        out_parquet = PROCESSED_DIR / f"ndl_core_dataset_part_{partition_key}.parquet"
        table = pa.table(df)
        pq.write_table(table, str(out_parquet))
        context.log.info(f"Wrote {len(rows)} records to {out_parquet} (partition {partition_key})")
    else:
        context.log.info(f"No rows produced for partition {partition_key}")

    # Write partition status file so failed items can be re-run individually later.
    status = {
        "partition_key": partition_key,
        "start_index": start_idx,
        "end_index": end_idx,
        "input_count": len(selected_paths),
        "meta_files_count": len(final_meta_files),
        "rows_written": len(rows),
        "failed_meta_count": len(failed_meta),
        "failed_meta": failed_meta,
        "per_meta_exceptions": per_meta_exceptions,
        "empty_text_count": stats_empty_text,
        "unsupported_format_stats": stats_unsupported,
        "data_file_not_found_count": len(data_file_not_found),
        "data_file_not_found": data_file_not_found
    }
    status_path = None
    try:
        status_path = PROCESSED_DIR / f"ndl_core_dataset_part_{partition_key}.status.json"
        with status_path.open("w", encoding="utf-8") as fh:
            json.dump(status, fh)
        context.log.info(f"Wrote partition status to {status_path}")
    except Exception as e:
        context.log.error(f"Failed to write partition status for {partition_key}: {e}")

    context.log.info("Stats: " + str(stats))
    context.log.info("Unsupported format stats: " + str(stats_unsupported))
    context.log.info("Skipped empty texts: " + str(stats_empty_text))
    context.log.info("Data file not found: " + str(data_file_not_found))
    if failed_meta:
        if status_path:
            context.log.warning(f"Partition {partition_key} had {len(failed_meta)} failed metadata files; see {status_path}")
        else:
            context.log.warning(f"Partition {partition_key} had {len(failed_meta)} failed metadata files; and status file could not be written.")


def add_dataset_record(data_file_rel: str, data_format: Literal["text"] | str, extra_metadata: dict[Any, Any],
                       meta: dict[str, Any] | None | Any, rows: list[dict[str, Any]], text: str, alt_description:str=""):
    row = {
        "identifier": str(uuid4()),
        "title": (meta.get("title", "") + alt_description).strip(),
        "description": ((meta.get("description", "") or "") + alt_description).strip(),
        "source": meta.get("source", ""),
        "date": meta.get("date", meta.get("public_time", meta.get("first_publish_time", ""))),
        "collection_time": meta.get("collection_time"),
        "open_type": meta.get("open_type", "Open Government"),
        "license": get_standard_license(meta),
        "tags": meta.get("tags", []),
        "language": detect_language(meta, text),
        "format": data_format,
        "text": text,
        "word_count": len(text.split()),
        "token_count": count_tokens(text),
        "data_file": data_file_rel,
        "extra_metadata": json.dumps(extra_metadata)
    }
    rows.append(row)


def get_standard_license(meta: dict[str, Any] | None | Any) -> str | None | Any:
    license_map = { "open government licence v3.0": "OGL-UK-3.0",
                    "ogl": "OGL-UK-3.0",
                   "uk-ogl": "OGL-UK-3.0",
                   "ogl-uk-3.0": "OGL-UK-3.0",
                   "cc-by": "CC BY",
                   "other-pd": "Other PD",
                   "other-open": "Other Open",
                   "odc-pddl": "ODC-PDDL",
                   "odc-odbl": "ODC-ODbL",
                   "odc-by": "ODC-BY",
                   "cc-nc": "CC-NC",
                   "other-nc": "other-NC",
                   "cc-zero": "CC0"
                   }
    data_license = license_map.get(meta.get("license", "ogl-uk-3.0").lower())
    return data_license


def detect_language(meta: dict[str, Any] | None | Any, text: str) -> Any:
    language = None
    if text and len(text.strip()) > MIN_TEXT_LENGTH:
        try:
            language = detect(text)
        except Exception as e:
            print(f"Failed to detect language: {e}")
    if not language:
        language = meta.get("language", "en")
    return language


def _is_metadata_file(p: Path) -> bool:
    return p.suffix.lower() == ".json" and p.stem.endswith("_metadata")


def _read_json(p: Path) -> Optional[Dict[str, Any]]:
    with p.open("r", encoding="utf-8") as fh:
        return json.load(fh)

def _find_associated_data_file(meta_path: Path) -> Optional[Path]:
    """Given a metadata file like X_metadata.json, look for X.(json|csv|xlsx|ods|pdf|html|htm|txt) in the same directory."""
    base = meta_path.stem.removesuffix("_metadata")
    candidates = [f"{base}.{extension}" for extension in SUPPORTED_FORMATS]
    for name in candidates:
        p = meta_path.parent / name
        if p.exists():
            return p
    return None


def _convert_structured_to_parquet(data_path: Path) -> Optional[Path]:
    """Convert structured input (json/csv/xlsx/ods) into a parquet file placed in DATA_DIR/structured with a UUID name.
    Returns the relative path (to DATA_DIR) Path object of written parquet or None on failure.
    """
    out_name = f"{uuid4()}.parquet"
    out_path = STRUCTURED_DIR / out_name

    path = None
    if data_path.suffix.lower() == ".json":
        path = convert_json_to_parquet(data_path, out_path)
    if data_path.suffix.lower() == ".csv":
        path = convert_csv_to_parquet(data_path, out_path)
    if data_path.suffix.lower() in {".xlsx", ".xls", ".ods"}:
        path = convert_spreadsheet_to_parquet(data_path, STRUCTURED_DIR, uuid_names=True)
    if path:
        return path
    print(f"Cannot convert {data_path} to parquet")
    return None

def filter_supported_files(file_paths: list[str]) -> list[Path]:
    """
    Filters supported files also removes json data files (not metadata file).
    :param file_paths: all paths
    :return: filtered paths
    """
    # final_meta_files: List[Path] = [Path(p) for p in file_paths if Path(p).suffix.lower() != ".json" and Path(
    #     p).suffix.lower() in SUPPORTED_EXTENSIONS]
    final_meta_files: List[Path] = list()

    json_paths = [Path(p) for p in file_paths if Path(p).suffix.lower() == ".json"]
    metadata_paths = [p for p in json_paths if _is_metadata_file(p)]
    meta_basenames = {p.stem.removesuffix("_metadata") for p in metadata_paths}
    for p in json_paths:
        if p.stem not in meta_basenames:
            final_meta_files.append(p)
    return final_meta_files


# New aggregation asset: combines per-partition parquet files into the single ndl_core_dataset_aggregated.parquet
@asset(group_name="processing", non_argument_deps={"format_records"})
def aggregate_records(context: AssetExecutionContext):
    """
    Aggregate all per-partition parquet files into a single ndl_core_dataset_aggregated.parquet. Missing partition files are
    skipped but logged. This allows rerunning specific partitions and then re-running this aggregation to include
    the fixed partitions.
    """
    part_files = sorted(PROCESSED_DIR.glob("ndl_core_dataset_part_*.parquet"))
    if not part_files:
        context.log.warning("No partition parquet files found to aggregate.")
        return

    dfs = []
    for p in part_files:
        try:
            table = pq.read_table(str(p))
            dfs.append(table.to_pandas())
        except Exception as e:
            context.log.error(f"Failed to read partition file {p}: {e}")

    if not dfs:
        context.log.warning("No valid partition dataframes to aggregate.")
        return

    combined = pd.concat(dfs, ignore_index=True)
    table = pa.table(combined)
    pq.write_table(table, str(AGGREGATED_ASSET))
    context.log.info(f"Wrote aggregated dataset with {len(combined)} records to {AGGREGATED_ASSET}")

    # report missing partitions (use PARTITIONS_DEF if available)
    try:
        expected_keys = set(PARTITIONS_DEF.get_partition_keys())
        existing_keys = {p.stem.rsplit("_", 1)[-1] for p in part_files}
        missing = sorted(expected_keys - existing_keys)
        if missing:
            context.log.info(f"Missing partition keys (not yet produced): {missing}")
    except Exception:
        pass

    log_total_statistics(context)

def log_total_statistics(context: AssetExecutionContext):
    """
    Logs total statistics from all partition status files.
    :param context:
    :return:
    """
    status_files = sorted(PROCESSED_DIR.glob("ndl_core_dataset_part_*.status.json"))
    total_stats = {
        "input_count": 0,
        "meta_files_count": 0,
        "rows_written": 0,
        "failed_meta_count": 0,
        "per_meta_exceptions": 0,
        "empty_text_count": 0,
        "data_file_not_found_count": 0,
        "unsupported_format_stats": {}
    }
    for status_file in status_files:
        try:
            with status_file.open("r", encoding="utf-8") as fh:
                status = json.load(fh)
            for key in total_stats.keys():
                if key == "unsupported_format_stats":
                    for fmt, count in status.get(key, {}).items():
                        total_stats[key][fmt] = total_stats[key].get(fmt, 0) + count
                else:
                    total_stats[key] += status.get(key, 0)
        except Exception as e:
            context.log.error(f"Failed to read status file {status_file}: {e}")

    context.log.info(f"Total statistics across all partitions: {total_stats}")


# New asset to tag dataset records using the EU theme classifier
@asset(group_name="processing", non_argument_deps={"aggregate_records"})
def tagged_data(context: AssetExecutionContext):
    """
    Reads target/ndl_core_dataset_aggregated.parquet, builds a small DataFrame for classification where:
      - text-format records: use first 1000 characters of `text`
      - parquet-format records: read referenced parquet(s) under STRUCTURED_DIR and concatenate column names
    Passes the small DataFrame to `classify_eu_themes` and writes a new parquet target/ndl_core_dataset_tagged.parquet
    with the `tags` column updated to the classifier's `predicted_themes`.
    """
    if not AGGREGATED_ASSET.exists():
        context.log.warning(f"Source dataset not found at {AGGREGATED_ASSET}; nothing to tag.")
        return

    df = pd.read_parquet(str(AGGREGATED_ASSET))

    to_tag_rows = []
    for i, row in df.iterrows():
        try:
            fmt = (row.get("format") or "").lower()
            identifier = row.get("identifier") or str(uuid4())
            title = row.get("title", "")
            description = row.get("description", "")

            text_val = ""
            if fmt == "text":
                text_val = (row.get("text") or "")[:3000]
            elif fmt == "parquet":
                # data_file is expected to be a relative path under STRUCTURED_DIR
                data_file_rel = row.get("data_file") or ""
                if data_file_rel:
                    data_path = STRUCTURED_DIR / data_file_rel
                    if data_path.exists() and data_path.is_file():
                        try:
                            parquet_file = pq.ParquetFile(str(data_path))
                            cols = parquet_file.schema_arrow.names
                            text_val = ", ".join(cols)
                        except Exception as e:
                            context.log.warning(f"Failed to read parquet schema from {data_path}: {e}")
                    elif data_path.exists() and data_path.is_dir():
                        # find up to first three parquet files in directory
                        parquet_candidates = sorted([p for p in data_path.glob("*.parquet")])
                        selected = parquet_candidates[:3]
                        col_names = []
                        for pfile in selected:
                            try:
                                pf = pq.ParquetFile(str(pfile))
                                col_names.extend(pf.schema_arrow.names)
                            except Exception as e:
                                context.log.warning(f"Failed reading parquet {pfile}: {e}")
                        text_val = ", ".join(set(col_names))
                    else:
                        context.log.warning(f"Referenced structured path not found: {data_path}")
                else:
                    context.log.warning(f"No data_file reference for parquet-format record id={identifier}")

            to_tag_rows.append({
                "identifier": identifier,
                "title": title,
                "description": description,
                "text": text_val
            })
        except Exception as exc:
            context.log.error(f"Failed preparing record for tagging (index {i}): {exc}")
            continue

    to_tag_df = pd.DataFrame(to_tag_rows)

    # Call classifier
    try:
        classified = classify_eu_themes(to_tag_df)
    except Exception as e:
        context.log.error(f"EU theme classifier failed: {e}")
        return

    # Merge predicted themes back into original dataframe (overwrite tags column)
    try:
        preds = classified.set_index("identifier")["predicted_themes"].apply(lambda x: json.dumps(x) if x is not None else None).to_dict()
        df_tagged = df.copy()

        # Use explicit column checks
        if "identifier" in df_tagged.columns:
            id_series = df_tagged["identifier"]
        else:
            id_series = pd.Series([None] * len(df_tagged), index=df_tagged.index)
        mapped = id_series.map(preds)
        if "tags" in df_tagged.columns:
            existing_tags = df_tagged["tags"]
        else:
            existing_tags = pd.Series([None] * len(df_tagged), index=df_tagged.index)
        df_tagged["tags"] = mapped.fillna(existing_tags)
    except Exception as e:
        context.log.error(f"Failed merging classifier output into dataset: {e}")
        return

    # Write out tagged parquet
    try:
        table = pa.table(df_tagged)
        pq.write_table(table, str(TAGGED_ASSET))
        context.log.info(f"Wrote tagged dataset with {len(df_tagged)} records to {TAGGED_ASSET}")
    except Exception as e:
        context.log.error(f"Failed to write tagged parquet to {TAGGED_ASSET}: {e}")


@asset(
    group_name="processing", non_argument_deps={"tagged_data"}
)
def anonymize_text_asset(context: AssetExecutionContext):
    """
    Reads the tagged dataset, anonymizes text records, and writes the result to a new parquet file.
    """
    context.log.info(f"Anonymizing text records in {TAGGED_ASSET}")
    if not TAGGED_ASSET.exists():
        context.log.error(f"Input file not found: {TAGGED_ASSET}")
        return

    df = pd.read_parquet(TAGGED_ASSET)
    # mask = df['format'] == 'text'
    # # text_rows = df[df['format'] == 'text']
    #
    # # Anonymize the text column
    # def anonymize_row(row):
    #     try:
    #         return anonymize_text_presidio(row)
    #     except Exception as e:
    #         context.log.error(f"Failed to anonymize row: {e}")
    #         return row
    #
    # tqdm.pandas(desc="Anonymizing text")
    # df.loc[mask, 'text'] = df.loc[mask, 'text'].progress_apply(anonymize_row)

    df_out = run_batch_process(df)

    # Write the anonymized dataset to the output path
    df_out.to_parquet(FINAL_ASSET, index=False)
    context.log.info(f"Anonymized dataset written to {FINAL_ASSET}")

