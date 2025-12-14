import os
import json
import tempfile

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
# - List the most recent N public "government" category datasets: https://ckan.publishing.service.gov.uk/api/action/package_search?fq=theme-primary:government%20AND%20license_id:(uk-ogl%20OR%20cc-by)&sort=metadata_created%20desc&rows=10

RESULTS_COUNT_PER_CATEGORY = 10
RESULTS_COUNT_FOR_ENVIRONMENT = 100

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

    results_count = RESULTS_COUNT_PER_CATEGORY
    if category == "environment":
        results_count = RESULTS_COUNT_FOR_ENVIRONMENT

    params = {
        "fq": f"theme-primary:{category} AND {public_license_filter}",
        "sort": "metadata_created desc",
        "rows": results_count,
    }


    resp = api_data_gov.get(PACKAGE_SEARCH_BASE, params=params)
    results = resp.get("result", {}).get("results", []) if isinstance(resp, dict) else []

    if not results:
        context.log.info(f"No packages found for category '{category}'")
        context.add_output_metadata({"packages_found": 0})
        return 0

    processed = 0
    saved_packages = []
    for pkg in results:
        pkg_id = pkg.get("id") or pkg.get("name") or pkg.get("title", "package")
        org = pkg.get("organization") or {}
        tags = set(pkg.get("tags", {}))
        tags.add(category)

        meta: Dict[str, Any] = {
            "title": pkg.get("title") or pkg.get("name") or pkg_id,
            "description": pkg.get("notes"),
            "source": "data.gov.uk",
            "creator": org.get("title") or org.get("name", ""),
            "collection_time": now_iso8601_utc(),
            "open_type": "Open Government",
            "license": pkg.get("license_id") or pkg.get("license_title") or pkg.get("licence-custom", ""),
            "language": pkg.get("locale", "en"),
            "category": category,
            "tags": list(tags),
            "dataset_url": f"https://data.gov.uk/dataset/{pkg.get('id')}",
        }

        # Prepare package directory
        safe_pkg_name = str(pkg_id).replace("/", "_").replace(" ", "_")
        pkg_dir = os.path.join(target_dir, safe_pkg_name)
        os.makedirs(pkg_dir, exist_ok=True)

        resources = pkg.get("resources", []) or []

        for i, res in enumerate(resources):
            # resource id (used for filenames)
            res_id = res.get("id") or f"resource_{i}"
            res_meta_fname = f"{res_id}_metadata.json"
            res_meta_path = os.path.join(pkg_dir, res_meta_fname)

            # If metadata file already exists for this resource, skip processing
            if os.path.exists(res_meta_path):
                context.log.info(f"Resource metadata already exists for {res_id} in {pkg_dir}, skipping")
                continue

            res_url = res.get("url") or res.get("resource_url")
            resource_metadata = meta.copy()
            resource_metadata["link"] = res_url
            resource_metadata["format"] = res.get("format", "")
            resource_metadata["public_time"] = parse_to_iso8601_utc(pkg.get("metadata_modified", ""))
            resource_metadata["first_publish_time"] = parse_to_iso8601_utc(pkg.get("datafile-date")) or parse_to_iso8601_utc(pkg.get("created", ""))

            if res.get("name"):
                resource_metadata["collection_title"] = resource_metadata["title"]
                resource_metadata["description"] = resource_metadata["title"] + ". " + resource_metadata["description"]
                resource_metadata["title"] = res.get("name")

            if not res_url:
                raise Exception(f"Package {pkg_id} resource #{i} has no URL")

            # Download using the API client; let exceptions propagate to fail the asset
            saved_path, actual_name, ext = api_data_gov.download_file(res_url, pkg_dir, preferred_name=res_id)
            if not saved_path:
                # Treat a failed download as an error
                # raise Exception(f"Download returned no file for resource {res_url} (package {pkg_id})")
                # Facing gdrive access permission issues etc. - log and skip
                context.log.warning(f"Download returned no file for resource {res_url} (package {pkg_id}), skipping")
                continue

            # Update metadata with saved file info
            try:
                abs_raw_base = os.path.abspath(RAW_DATA_PATH)
                rel_path = os.path.relpath(os.path.abspath(saved_path), start=abs_raw_base)
            except Exception:
                rel_path = os.path.basename(saved_path)

            resource_metadata["data_file_path"] = rel_path
            resource_metadata["format"] = (ext or resource_metadata.get("format") or "").lower()
            resource_metadata["actual_data_file_name"] = actual_name or os.path.basename(saved_path)

            # Write per-resource metadata file named '<res_id>_metadata.json'
            try:
                with tempfile.NamedTemporaryFile("w", delete=False, dir=pkg_dir, suffix=".tmp") as tmp:
                    json.dump(resource_metadata, tmp, indent=2)
                    tmp_meta = tmp.name
                os.replace(tmp_meta, res_meta_path)
            except Exception as exc:
                context.log.warning(f"Failed to write resource metadata for {res_id} in package {pkg_id}: {exc}")
                # Attempt to remove the downloaded data file since metadata couldn't be written
                try:
                    if saved_path and os.path.exists(saved_path):
                        os.remove(saved_path)
                        context.log.info(f"Removed downloaded file {saved_path} because metadata write failed for resource {res_id} (package {pkg_id})")
                except Exception as exc_rm:
                    context.log.warning(f"Failed to remove downloaded file {saved_path} after metadata write failure for {res_id} (package {pkg_id}): {exc_rm}")

            processed += 1
            saved_packages.append(pkg_id)

    context.add_output_metadata({"packages_found": len(results), "packages_processed": processed})
    return saved_packages