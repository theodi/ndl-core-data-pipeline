"""
Dagster asset(s) to download timeseries data from ONS (api.beta.ons.gov.uk).

This asset fetches the list of topics from the ONS API, retrieves the latest timeseries datasets
for each topic, downloads the associated CSV files, and saves them along with metadata in a
designated raw data directory.
"""

import os
import json
import tempfile
import urllib.parse
import glob
from typing import Optional, List, Dict

from dagster import asset, AssetExecutionContext, RetryPolicy, Backoff, Jitter

from ndl_core_data_pipeline.resources.api_client import RateLimitedApiClient
from ndl_core_data_pipeline.resources.time_utils import now_iso8601_utc, parse_to_iso8601_utc

RESOURCES_PER_TOPIC = 15

RAW_DATA_PATH = "data/raw/ons_gov_uk"
os.makedirs(RAW_DATA_PATH, exist_ok=True)

TOPICS_URL = "https://api.beta.ons.gov.uk/v1/topics"
SEARCH_URL = "https://api.beta.ons.gov.uk/v1/search"
CSV_GENERATOR = "https://www.ons.gov.uk/generator?format=csv&uri={uri}"

NETWORK_RETRY_POLICY = RetryPolicy(
    max_retries=3,
    delay=1,
    backoff=Backoff.EXPONENTIAL,
    jitter=Jitter.FULL,
)


def _safe_name(uri: str) -> str:
    decoded = urllib.parse.unquote(uri)
    cleaned = decoded.replace("/", "_")
    safe = "".join(c if (c.isalnum() or c in "_-") else "_" for c in cleaned)
    while "__" in safe:
        safe = safe.replace("__", "_")
    safe = safe.strip("_")
    return safe[:200] if len(safe) > 200 else safe


def _atomic_write_text(path: str, text: str, encoding: str = "utf-8") -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=os.path.dirname(path), encoding=encoding) as tmp:
        tmp.write(text)
        tmp_path = tmp.name
    os.replace(tmp_path, path)


def _atomic_write_bytes(path: str, data: bytes) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with tempfile.NamedTemporaryFile("wb", delete=False, dir=os.path.dirname(path)) as tmp:
        tmp.write(data)
        tmp_path = tmp.name
    os.replace(tmp_path, path)


def fetch_topics(api: RateLimitedApiClient, context: AssetExecutionContext) -> List[Dict]:
    """Use RateLimitedApiClient.get to fetch topics JSON and return items list."""
    try:
        data = api.get(TOPICS_URL)
        return data.get("items")
    except Exception as exc:
        context.log.error(f"Failed to fetch topics from ONS: {exc}")
        raise exc


def fetch_resources_for_topic(api: RateLimitedApiClient, topic_id: str, context: AssetExecutionContext, limit: int = RESOURCES_PER_TOPIC) -> List[Dict]:
    params = {
        "topics": topic_id,
        "content_type": "timeseries",
        "sort": "last_updated",
        "limit": limit,
    }
    try:
        data = api.get(SEARCH_URL, params=params)
        return data.get("items", [])
    except Exception as exc:
        context.log.error(f"Failed to fetch resources for topic {topic_id}: {exc}")
        raise exc


def download_csv_for_uri(api: RateLimitedApiClient, uri: str, context: AssetExecutionContext, safe_name: str) -> Optional[str]:
    """Use RateLimitedApiClient.download_file to fetch CSV into RAW_DATA_PATH. Returns saved_path or None."""
    encoded = urllib.parse.quote(uri, safe="")
    url = CSV_GENERATOR.format(uri=encoded)
    try:
        saved_path, actual_filename, ext = api.download_file(url, RAW_DATA_PATH, preferred_name=safe_name)
        if not saved_path:
            context.log.error(f"api.download_file returned no path for {url}")
            return None
        return saved_path
    except Exception as exc:
        context.log.error(f"Failed to download CSV for {uri} via api.download_file: {exc}")
        raise exc


@asset(group_name="ons_gov_uk", retry_policy=NETWORK_RETRY_POLICY)
def ons_gov_uk_timeseries(context: AssetExecutionContext, api_ons: RateLimitedApiClient):
    """Fetch topics, latest timeseries per topic, download CSV and save metadata."""
    topics = fetch_topics(api_ons, context)
    context.log.info(f"Fetched {len(topics)} topics from ONS")

    total_saved = 0
    total_skipped = 0
    failures = 0

    for t in topics:
        topic_id = t.get("id")
        if not topic_id:
            context.log.warning(f"Skipping topic with no id: {t}")
            continue
        context.log.info(f"Processing topic {topic_id}")
        timeseries_items = fetch_resources_for_topic(api_ons, topic_id, context)
        context.log.info(f"Found {len(timeseries_items)} timeseries for topic {topic_id}")

        for item in timeseries_items:
            uri = item.get("uri")
            if not uri:
                context.log.warning(f"Skipping timeseries with no uri: {item}")
                failures += 1
                continue

            safe = _safe_name(uri)

            # Check for existing files: need both a CSV and JSON to consider it fully downloaded
            pattern = os.path.join(RAW_DATA_PATH, f"{safe}.*")
            matches = glob.glob(pattern)
            has_json = any(m.lower().endswith('.json') for m in matches)
            has_csv = any(m.lower().endswith('.csv') for m in matches)
            if has_json and has_csv:
                context.log.info(f"Skipping already-downloaded timeseries: {uri}")
                total_skipped += 1
                continue

            metadata = {
                "uri": uri,
                "link": "https://www.ons.gov.uk" + uri,
                "title": item.get("title") or "",
                "description": item.get("summary") or "",
                "public_time": parse_to_iso8601_utc(item.get("release_date", "")),
                "first_publish_time": parse_to_iso8601_utc(item.get("release_date", "")),
                "topics": item.get("keywords") or item.get("keyword") or [],
                "source": "ons.gov.uk",
                "collection_time": now_iso8601_utc(),
                "open_type": "Open Government",
                "license:": "Open Government Licence v3.0",
                "language": "en",
                "format": "csv",
                "file_name": safe + ".csv",
            }

            context.log.info(f"Downloading CSV for uri={uri}")
            saved_path = download_csv_for_uri(api_ons, uri, context, safe + ".csv")
            if not saved_path:
                context.log.error(f"Failed to download CSV for {uri}")
                failures += 1
                continue

            # Determine base name (without extension) to write metadata JSON next to CSV
            base = os.path.splitext(saved_path)[0]
            json_path = f"{base}.json"

            try:
                _atomic_write_text(json_path, json.dumps(metadata, ensure_ascii=False, indent=2))
                total_saved += 1
                context.log.info(f"Saved timeseries {uri} -> {saved_path}")
            except Exception as exc:
                context.log.error(f"Failed to save metadata for {uri}: {exc}")
                failures += 1

    context.add_output_metadata({
        "saved": total_saved,
        "skipped": total_skipped,
        "failures": failures,
    })

    return f"ONS: saved {total_saved}, skipped {total_skipped}, failures {failures}"
