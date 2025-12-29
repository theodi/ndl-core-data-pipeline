from dagster import asset, AssetExecutionContext
from pathlib import Path
from uuid import uuid4
import json
from typing import List, Optional, Dict, Any, Literal
from langdetect import detect, DetectorFactory

from ndl_core_data_pipeline.resources.refine.dedupe import deduplicate_folder
from ndl_core_data_pipeline.resources.token_counter import count_tokens

# reuse existing converters/extractors
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

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

MIN_TEXT_LENGTH = 150
SUPPORTED_FORMATS = ["text", "html", "htm", "xhtml", "csv", "xlsx", "xls", "pdf", "json", "ods"]

CURRENT_DIR = Path(__file__).parent
TARGET_DIR = (CURRENT_DIR / "../../../../target").resolve()
NON_DUPLICATE_FILES = TARGET_DIR / "non_duplicates.txt"
DATA_DIR = (CURRENT_DIR / "../../../../data").resolve()
PROCESSED_DIR = TARGET_DIR / "processed"
STRUCTURED_DIR = TARGET_DIR / "structured"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
STRUCTURED_DIR.mkdir(parents=True, exist_ok=True)

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
    non_argument_deps={"deduplicated_records"}
)
def format_records(context: AssetExecutionContext):
    """
    Formats the deduplicated files and generates a standardized and unified parquet file. Formats structured data files into parquet and moves
    them to a separate dataset folder.
    :param context:
    :return:
    """
    input_file = NON_DUPLICATE_FILES
    if not input_file.exists():
        context.log.warning(f"No deduplicated records file found at {input_file}")
        return
    with input_file.open("r") as f:
        file_paths = [line.strip() for line in f.readlines()]
    context.log.info(f"Found {len(file_paths)} deduplicated files to format.")

    final_meta_files = filter_supported_files(file_paths)
    context.log.info(f"Processing {len(final_meta_files)} metadata json files to build parquet.")

    rows: List[Dict[str, Any]] = []

    DetectorFactory.seed = 0
    stats_empty_text = 0
    index = 0
    for meta_path in final_meta_files:
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
                    data_file_rel = str(conv)
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

        if data_format == "text":
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
                extra_metadata["speakers"] = conversation.get("speakers", [])
                add_dataset_record(data_file_rel, data_format, extra_metadata, meta, rows, conversation["text"], " conversation_" + str(conversation_id))
                conversation_id = conversation_id + 1
        else:
            add_dataset_record(data_file_rel, data_format, extra_metadata, meta, rows, text)

    df = pd.DataFrame(rows)
    out_parquet = PROCESSED_DIR / "ndl_core_dataset.parquet"
    table = pa.Table.from_pandas(df=df)
    pq.write_table(table, str(out_parquet))

    context.log.info(f"Wrote {len(rows)} records to {out_parquet}")
    context.log.info("Stats: " + str(stats))
    context.log.info("Unsupported format stats: " + str(stats_unsupported))
    context.log.info("Skipped empty texts: " + str(stats_empty_text))
    context.log.info("Data file not found: " + str(data_file_not_found))


def add_dataset_record(data_file_rel: str, data_format: Literal["text"] | str, extra_metadata: dict[Any, Any],
                       meta: dict[str, Any] | None | Any, rows: list[dict[str, Any]], text: str, alt_description:str=""):
    row = {
        "identifier": str(uuid4()),
        "title": meta.get("title", ""),
        "description": (meta.get("description", "") or "") + alt_description,
        "source": meta.get("source", ""),
        "date": meta.get("date"),
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
    if text and len(text.strip()) > 20:
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
        convert_csv_to_parquet(data_path, out_path)
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
