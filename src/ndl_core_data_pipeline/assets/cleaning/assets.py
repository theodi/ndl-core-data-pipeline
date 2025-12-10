import os
import glob
import json
from dagster import asset, AssetExecutionContext

RAW_GOV_UK_PATH = "data/raw/gov_uk"


@asset(
    group_name="cleaning",
    deps=["gov_uk_process_batch", "data_gov_feed_b"]
)
def clean_csv_records(context: AssetExecutionContext):
    """
    Reads ALL json files from the raw folder and cleans the ones that are CSVs.
    """

    # 2. SCAN THE DISK
    # Since the upstream asset is partitioned, we don't rely on it passing 
    # a list of 1,500 file paths in memory. We just scan the folder.
    # This is the "Fan-In" pattern (Many partitions -> One cleaner).
    all_files = glob.glob(f"{RAW_GOV_UK_PATH}/*.json")

    if not all_files:
        context.log.warning("No files found to clean!")
        return []

    processed_results = []

    context.log.info(f"Scanning {len(all_files)} files for CSV content...")

    for f_path in all_files:
        try:
            with open(f_path, "r") as f:
                data = json.load(f)

            # 3. CHECK LOGIC
            # Depending on how you saved it, the structure might be flat or nested.
            # Our previous code saved it as: {"metadata": ..., "content": ...}

            # Example heuristic: Check if 'format' is in metadata OR infer from content
            # Adjust this logic based on your actual data shape
            metadata = data.get("metadata", {})
            content = data.get("content", "")

            # Example: Only clean if it looks like a CSV (has commas, newlines)
            if "," in content and "\n" in content:
                # ... Perform cleaning ...
                clean_data = content.replace("\r", "")  # Simple example

                processed_results.append({
                    "source": f_path,
                    "clean_data": clean_data
                })

        except Exception as e:
            context.log.warning(f"Skipping corrupt file {f_path}: {e}")

    context.add_output_metadata({
        "total_files_scanned": len(all_files),
        "csvs_processed": len(processed_results)
    })

    return processed_results