from dagster import asset, AssetExecutionContext
from pathlib import Path

from ndl_core_data_pipeline.resources.refine.dedupe import deduplicate_folder

CURRENT_DIR = Path(__file__).parent
TARGET_DIR = (CURRENT_DIR / "../../../../target").resolve()
OUT_FILE = TARGET_DIR / "non_duplicates.txt"


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
    out_file = OUT_FILE
    if not out_file.exists():
        context.log.warning(f"No deduplicated records file found at {out_file}")
        return
    with out_file.open("r") as f:
        file_paths = [line.strip() for line in f.readlines()]
    context.log.info(f"Found {len(file_paths)} deduplicated files to format.")