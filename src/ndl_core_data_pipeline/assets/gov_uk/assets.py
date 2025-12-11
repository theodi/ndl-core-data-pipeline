import os
import math
import json
import tempfile
from datetime import timezone
from typing import List, Optional, Dict, Any
from dagster import (
    asset,
    AssetExecutionContext,
    RetryPolicy,
    Backoff,
    Jitter,
    DynamicPartitionsDefinition
)
from ndl_core_data_pipeline.resources.api_client import RateLimitedApiClient
from ndl_core_data_pipeline.resources.time_utils import now_iso8601_utc, parse_to_iso8601_utc, parse_iso_to_ts

gov_uk_batches = DynamicPartitionsDefinition(name="gov_uk_search_batches")

CRAWL_FROM_DATE = "2025-01-01"

BATCH_SIZE = 100  # Process 100 documents per partition
RAW_DATA_PATH = "data/raw/gov_uk"
os.makedirs(RAW_DATA_PATH, exist_ok=True)

NETWORK_RETRY_POLICY = RetryPolicy(
    max_retries=3,              # Try 3 times before turning Red
    delay=2,                    # Wait 2 seconds initially
    backoff=Backoff.EXPONENTIAL,# Wait 2s, then 4s, then 8s
    jitter=Jitter.FULL          # Add randomness so we don't hammer the API at exact intervals
)

@asset(group_name="gov_uk_dynamic")
def gov_uk_search_index(context: AssetExecutionContext, api_gov_uk: RateLimitedApiClient):
    """
    1. Calls Search API to get TOTAL count only.
    2. Calculates how many batches we need.
    3. Instruct Dagster to create partitions.
    """
    # Just fetch one item to get the 'total' field
    response = api_gov_uk.get(f"/api/search.json?q=&filter_public_timestamp=from:{CRAWL_FROM_DATE}", params={"count": 1})
    total_docs = response.get("total", 0)

    # Calculate number of batches needed (e.g., 1500 / 100 = 15 batches)
    num_batches = math.ceil(total_docs / BATCH_SIZE)

    # Generate keys like "batch_0", "batch_1", etc.
    partition_keys = [f"batch_{i}" for i in range(num_batches)]

    context.log.info(f"Found {total_docs} docs. Creating {num_batches} partitions.")
    context.instance.add_dynamic_partitions(
        partitions_def_name="gov_uk_search_batches",
        partition_keys=partition_keys
    )

    return total_docs


@asset(
    group_name="gov_uk_dynamic",
    partitions_def=gov_uk_batches,  # Bind this asset to the dynamic definition
    deps=[gov_uk_search_index],  # Wait for search to finish first
    retry_policy=NETWORK_RETRY_POLICY
)
def gov_uk_process_batch(context: AssetExecutionContext, api_gov_uk: RateLimitedApiClient):
    """
    Batched search and content API calls:
    Search API: https://github.com/alphagov/search-api/blob/main/docs/using-the-search-api.md
    Content API: https://content-api.publishing.service.gov.uk/#gov-uk-content-api
    """
    # 1. Decode partition (e.g., "batch_5")
    partition_key = context.partition_key
    batch_index = int(partition_key.split("_")[1])
    start_offset = batch_index * BATCH_SIZE

    context.log.info(f"Processing {partition_key}: Items {start_offset} to {start_offset + BATCH_SIZE}")

    # 2. Search batch links
    results = search_batch(api_gov_uk, start_offset)

    if not results:
        context.log.warning(f"[{partition_key}] No results found. Batch might be empty.")
        return None

    # 3. Retrieve the Content
    files_processed = 0
    files_skipped = 0
    for item in results:
        link = item.get("link")

        safe_name = link.strip("/").replace("/", "_")
        target_file_path = f"{RAW_DATA_PATH}/{safe_name}.json"
        if os.path.exists(target_file_path):
            files_skipped += 1
            continue

        if item.get("primary_publishing_organisation", []):
            orgs = item.get("primary_publishing_organisation", [])
            org_titles = [org.get("title", "") for org in orgs if "title" in org]
        else:
            orgs = item.get("organisations", [])
            org_titles = [org.get("title", "") for org in orgs if "title" in org]

        metadata = {
            "link": "https://www.gov.uk" + item.get("link"),
            "title": item.get("title"),
            "description": item.get("description"),
            "source": "gov.uk",
            "creator": org_titles,
            "public_time": parse_to_iso8601_utc(item.get("public_timestamp")),
            "first_publish_time": get_oldest_change_history(item.get("details", {}).get("change_history", []), parse_to_iso8601_utc(item.get("first_published_at", item.get("public_timestamp", "")))),
            "collection_time": now_iso8601_utc(),
            "open_type": "Open Government",
            "license:": "Open Government Licence v3.0",
            "language": item.get("locale", "en"),
            "format": "text"
        }

        # get content
        content_url = f"/api/content{link}"
        content_resp = api_gov_uk.get(content_url)
        content = content_resp.get("details", {}).get("body", "")
        if content:
            full_record = {
                "metadata": metadata,
                "text": content,
                "full_api_response": content_resp
            }

            save_raw_file(full_record, target_file_path)

            files_processed += 1
            if files_processed % 10 == 0:
                context.log.debug(f"[{partition_key}] processed {files_processed} items...")

    context.add_output_metadata({
        "batch_index": batch_index,
        "newly_saved": files_processed,
        "skipped_existing": files_skipped,
        "total_in_batch": len(results)
    })
    return f"Batch {batch_index} Complete"


def save_raw_file(full_record: dict,target_file_path: str):
    """
    Write JSON safely to a temp file then atomically rename.
    """
    with tempfile.NamedTemporaryFile("w", delete=False, dir=RAW_DATA_PATH, suffix=".tmp") as tmp:
        json.dump(full_record, tmp, indent=2)
        tmp_path = tmp.name
    os.rename(tmp_path, target_file_path)


def search_batch(api_gov_uk: RateLimitedApiClient, start_offset: int) -> Any:
    search_params = {
        "q": "",
        "filter_public_timestamp": f"from:{CRAWL_FROM_DATE}",
        "count": BATCH_SIZE,
        "start": start_offset,
        "fields": "link,title,description,public_timestamp,organisations"
    }
    resp = api_gov_uk.get("/api/search.json", params=search_params)
    results = resp.get("results", [])
    return results

def get_oldest_change_history(change_history: List[Dict], default_value: str) -> Optional[str]:
    """
    Return the oldest `public_timestamp` from change_history as an ISO8601 string,
    or None if no valid timestamps found.
    """
    datetimes = [parse_iso_to_ts(default_value)]
    for item in change_history:
        ts = item.get("public_timestamp")
        if not ts:
            continue
        try:
            dt = parse_iso_to_ts(ts)
        except Exception:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        datetimes.append(dt)
    if not datetimes:
        return None
    oldest = min(datetimes)
    return parse_to_iso8601_utc(oldest.astimezone(timezone.utc).isoformat())
