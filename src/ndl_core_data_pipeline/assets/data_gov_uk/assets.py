import os
import json
import tempfile
import time
import urllib.parse
from typing import Any, Dict, List

from dagster import (
    asset,
    AssetExecutionContext,
    DynamicPartitionsDefinition,
    RetryPolicy,
    Backoff,
    Jitter,
)
from ndl_core_data_pipeline.resources.api_client import RateLimitedApiClient
from ndl_core_data_pipeline.resources.time_utils import now_iso8601_utc, parse_to_iso8601_utc

# Useful data.gov.uk endpoints
#
# - All categories: https://ckan.publishing.service.gov.uk/api/3/action/package_search?facet.field=[%22theme-primary%22]&rows=0
# - All License types: https://ckan.publishing.service.gov.uk/api/3/action/package_search?facet.field=[%22license_id%22]&rows=0
# - List last 10 public "government" category datasets: https://ckan.publishing.service.gov.uk/api/action/package_search?fq=theme-primary:government%20AND%20license_id:(uk-ogl%20OR%20cc-by)&sort=metadata_created%20desc&rows=10

PUBLIC_LICENCES = ["ogl", "uk-ogl", "OGL-UK-3.0", "cc-by", "other-pd", "other-open", "odc-pddl", "odc-odbl", "odc-by", "cc-nc", "other-nc", "cc-zero"]
public_license_filter = "license_id:(" + " OR ".join(PUBLIC_LICENCES) + ")"

# Dynamic partitions for categories
data_gov_categories = DynamicPartitionsDefinition(name="data_gov_categories")

RAW_DATA_PATH = "data/raw/data_gov_uk"
os.makedirs(RAW_DATA_PATH, exist_ok=True)

NETWORK_RETRY_POLICY = RetryPolicy(
    max_retries=3,
    delay=2,
    backoff=Backoff.EXPONENTIAL,
    jitter=Jitter.FULL,
)

CATEGORIES_ENDPOINT = (
    "https://ckan.publishing.service.gov.uk/api/3/action/package_search?facet.field=[%22theme-primary%22]&rows=0"
)
PACKAGE_SEARCH_BASE = "https://ckan.publishing.service.gov.uk/api/action/package_search"


@asset(group_name="data_gov_crawling")
def data_gov_discover_categories(context: AssetExecutionContext, api_data_gov: RateLimitedApiClient):
    """Discover all categories (theme-primary) and create dynamic partitions.

    Uses the CKAN facet query to retrieve theme-primary facets and create
    partitions for each category name.
    """
    resp = api_data_gov.get(CATEGORIES_ENDPOINT)

    facets = resp.get("result", {}).get("facets", {}).get("theme-primary", {})
    if not facets:
        context.log.error("No categories found in data_gov response")
        return []

    categories = list(facets.keys())

    context.log.info(f"Registering {len(categories)} categories as dynamic partitions")
    context.instance.add_dynamic_partitions(
        partitions_def_name=data_gov_categories.name,
        partition_keys=categories,
    )

    return categories


@asset(
    group_name="data_gov_crawling",
    partitions_def=data_gov_categories,
    deps=[data_gov_discover_categories],
    retry_policy=NETWORK_RETRY_POLICY,
)
def data_gov_process_category(context: AssetExecutionContext, api_data_gov: RateLimitedApiClient):
    """Process the given category partition: retrieve top-10 recent datasets and download their resources.

    The asset will create a directory per category under `data/raw/data_gov_uk/<category>/` and
    for each package will write a metadata JSON and download each resource file. The implementation is
    defensive and will continue on individual failures.
    """
    category = context.partition_key
    context.log.info(f"Processing category partition: {category}")

    # Prepare target directory
    safe_category = category.replace("/", "_").replace(" ", "_")
    target_dir = os.path.join(RAW_DATA_PATH, safe_category)
    os.makedirs(target_dir, exist_ok=True)

    # Build package_search call (use params to avoid manual encoding)
    params = {
        "fq": f"theme-primary:{category}",
        "sort": "metadata_created desc",
        "rows": 10,
    }

    try:
        resp = api_data_gov.get(PACKAGE_SEARCH_BASE, params=params)
    except Exception as exc:
        context.log.error(f"Failed to search packages for category '{category}': {exc}")
        return None

    results = resp.get("result", {}).get("results", []) if isinstance(resp, dict) else []

    if not results:
        context.log.info(f"No packages found for category '{category}'")
        context.add_output_metadata({"packages_found": 0})
        return 0

    session = api_data_gov.get_session()

    processed = 0
    saved_packages = []
    for pkg in results:
        try:
            pkg_id = pkg.get("id") or pkg.get("name") or pkg.get("title", "package")
            pkg_title = pkg.get("title") or pkg_id
            pkg_created = pkg.get("metadata_created")
            pkg_notes = pkg.get("notes")
            org = pkg.get("organization") or {}
            org_name = org.get("name") or org.get("title")
            license_title = pkg.get("license_title")

            # Normalize times
            try:
                metadata_created_iso = parse_to_iso8601_utc(pkg_created) if pkg_created else None
            except Exception:
                metadata_created_iso = None

            meta: Dict[str, Any] = {
                "id": pkg_id,
                "title": pkg_title,
                "metadata_created": metadata_created_iso,
                "notes": pkg_notes,
                "organization": org_name,
                "license_title": license_title,
                "category": category,
                "collection_time": now_iso8601_utc(),
            }

            # Prepare package directory
            safe_pkg_name = str(pkg_id).replace("/", "_").replace(" ", "_")
            pkg_dir = os.path.join(target_dir, safe_pkg_name)
            os.makedirs(pkg_dir, exist_ok=True)

            resources = pkg.get("resources", []) or []
            downloaded_resources: List[str] = []

            for i, res in enumerate(resources):
                res_url = res.get("url") or res.get("resource_url")
                if not res_url:
                    context.log.warning(f"Package {pkg_id} resource #{i} has no URL, skipping")
                    continue

                # Respect rate limit
                time.sleep(1.0 / float(getattr(api_data_gov, "rate_limit_per_second", 5.0)))

                try:
                    # Use session to download binary content
                    r = session.get(res_url, timeout=20)
                    r.raise_for_status()
                except Exception as exc:
                    context.log.warning(f"Failed to download resource {res_url} for package {pkg_id}: {exc}")
                    continue

                # Derive filename from url or resource name
                parsed = urllib.parse.urlparse(res_url)
                filename = os.path.basename(parsed.path) or f"resource_{i}"
                # Fallback to format field
                if not filename and res.get("format"):
                    filename = f"resource_{i}.{res.get('format').lower()}"

                target_file = os.path.join(pkg_dir, filename)
                # Write to temp file then move
                try:
                    with tempfile.NamedTemporaryFile("wb", delete=False, dir=pkg_dir) as tmp:
                        tmp.write(r.content)
                        tmp_path = tmp.name
                    os.replace(tmp_path, target_file)
                    downloaded_resources.append(filename)
                except Exception as exc:
                    context.log.warning(f"Failed to save resource {res_url} to {target_file}: {exc}")
                    # Try to clean temp file if present
                    try:
                        if tmp_path and os.path.exists(tmp_path):
                            os.remove(tmp_path)
                    except Exception:
                        pass
                    continue

            # Save metadata including downloaded resource filenames
            meta["downloaded_resources"] = downloaded_resources

            # Also save the original package JSON for reference
            meta["raw_package"] = pkg

            meta_file = os.path.join(pkg_dir, "metadata.json")
            try:
                with tempfile.NamedTemporaryFile("w", delete=False, dir=pkg_dir, suffix=".tmp") as tmp:
                    json.dump(meta, tmp, indent=2)
                    tmp_meta = tmp.name
                os.replace(tmp_meta, meta_file)
            except Exception as exc:
                context.log.warning(f"Failed to write metadata for package {pkg_id}: {exc}")

            processed += 1
            saved_packages.append(pkg_id)

        except Exception as exc:
            context.log.warning(f"Unexpected error processing package in category '{category}': {exc}")
            continue

    context.add_output_metadata({"packages_found": len(results), "packages_processed": processed})
    return saved_packages

