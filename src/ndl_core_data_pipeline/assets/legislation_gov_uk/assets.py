"""
Downloads legislation data from legislation.gov.uk 2025 data feed.

Data feed index: https://www.legislation.gov.uk/all/2025/data.feed
http://legislation.gov.uk/all/data.feed
"""

import os
import json
import tempfile
import time
from typing import Optional
from dagster import asset, AssetExecutionContext, RetryPolicy, Backoff, Jitter
from lxml import etree
from ndl_core_data_pipeline.resources.api_client import RateLimitedApiClient
from ndl_core_data_pipeline.resources.time_utils import now_iso8601_utc, parse_to_iso8601_utc

RAW_DATA_PATH = "data/raw/legislation_gov_uk/2025"
os.makedirs(RAW_DATA_PATH, exist_ok=True)

START_URL = "https://www.legislation.gov.uk/all/2025/data.feed"

NETWORK_RETRY_POLICY = RetryPolicy(
    max_retries=3,
    delay=2,
    backoff=Backoff.EXPONENTIAL,
    jitter=Jitter.FULL
)

NS = {'ukm': 'http://www.legislation.gov.uk/namespaces/metadata'}

def _local_name(tag: Optional[str]) -> str:
    if not tag:
        return ''
    if isinstance(tag, str) and '}' in tag:
        return tag.split('}', 1)[1]
    return tag


def _safe_name(file_name: str) -> str:
    # Minimal sanitization for filesystem
    return file_name.replace('/', '_').replace(' ', '').strip()


@asset(group_name="legislation_gov_uk", retry_policy=NETWORK_RETRY_POLICY)
def legislation_gov_uk_2025(context: AssetExecutionContext, api_legislation: RateLimitedApiClient):
    """
    Crawl the legislation.gov.uk 2025 Atom data.feed starting page and follow rel="next" links.

    For each feed.entry extract: entry.id, title, link(s), updated, published, ukm:ISBN, ukm:Year, ukm:Number, summary.
    Find entry.link with type="application/xhtml+xml", fetch its href content and store as "text".

    Write each record as a JSON file to data/raw/legislation_gov_uk/2025/<ISBN>.json
    """
    next_url = START_URL
    total_saved = 0
    total_skipped = 0

    while next_url:
        context.log.info(f"Fetching feed page: {next_url}")
        page_text = fetch_text(api_legislation, next_url, context)
        if not page_text:
            context.log.error(f"Empty response for {next_url}, stopping")
            break

        try:
            parser = etree.XMLParser(recover=True)
            tree = etree.fromstring(page_text.encode('utf-8'), parser=parser)
        except Exception as e:
            context.log.error(f"Failed to parse feed XML from {next_url}: {e}")
            break

        # find all entry elements (namespace-agnostic)
        entries = tree.findall('.//{*}entry')
        context.log.info(f"Found {len(entries)} entries on page")

        for entry in entries:
            metadata = process_entry(entry, context, api_legislation)
            if not metadata:
                total_skipped += 1
                continue
            try:
                save_record(metadata['file_name'], metadata)
                total_saved += 1
            except Exception as e:
                context.log.error(f"Failed to save record for file={metadata['file_name']}: {e}")

        # Find next link rel="next"
        next_url = None
        for l in tree.findall('.//{*}link'):
            if (l.get('rel') or '').lower() == 'next':
                next_url = l.get('href')
                break

    context.add_output_metadata({
        'saved': total_saved,
        'skipped': total_skipped
    })

    return f"Saved {total_saved} items"

def process_entry(entry, context, api_legislation):
    """
    Process a single feed entry element to extract metadata and fetch text content.
    :param entry: Feed entry XML element
    :param context: Dagster asset execution context
    :param api_legislation: RateLimitedApiClient for legislation.gov.uk
    :return: Metadata dictionary
    """
    data = {}
    for child in entry.iterchildren():
        lname = _local_name(child.tag)
        # For simple text elements record their text
        if len(child) == 0:
            data[lname] = (child.text or '').strip() if child.text is not None else ''
        else:
            # for elements with children like link, etc. leave processing to more specific logic
            data.setdefault(lname, [])
            data[lname].append(child)

    # Basic fields
    entry_id = data.get('id') if isinstance(data.get('id'), str) else None
    title = data.get('title') if isinstance(data.get('title'), str) else None
    updated = data.get('updated') if isinstance(data.get('updated'), str) else None
    published = data.get('published') if isinstance(data.get('published'), str) else None
    summary = data.get('summary') if isinstance(data.get('summary'), str) else None
    # Some ukm fields are empty elements with attributes, e.g. <ukm:ISBN Value="978..." />
    isbn_element = entry.find('ukm:ISBN', namespaces=NS)
    isbn = isbn_element.get('Value') if isbn_element is not None else ""
    year_element = entry.find('ukm:Year', namespaces=NS)
    year = year_element.get('Value') if year_element is not None else ""
    number_element = entry.find('ukm:Number', namespaces=NS)
    number = number_element.get('Value') if number_element is not None else ""
    creation_date_element = entry.find('ukm:CreationDate', namespaces=NS)
    creation_date = creation_date_element.get('Value') if creation_date_element is not None else ""

    file_name = entry_id.replace("http://www.legislation.gov.uk/id/", "")

    # If target file already exists, skip processing to avoid re-downloading/re-writing
    fname_safe = _safe_name(file_name) or file_name
    target_path = os.path.join(RAW_DATA_PATH, f"{fname_safe}.json")
    if os.path.exists(target_path):
        context.log.info(f"Skipping entry because file already exists: {target_path}")
        return None

    xhtml_href = None
    main_link = None
    for link_el in entry.findall('.//{*}link'):
        href = link_el.get('href')
        ltype = link_el.get('type')
        rel = link_el.get('rel')
        if ltype and ltype.lower() == 'application/xhtml+xml':
            xhtml_href = href
        if rel and rel.lower() == 'self':
            main_link = href

    metadata = {
        'id': entry_id,
        'link': main_link,
        'title': title,
        'description': summary,
        'source': 'legislation.gov.uk',
        'public_time': parse_to_iso8601_utc(updated),
        'first_publish_time': parse_to_iso8601_utc(published),
        'ISBN': isbn,
        'year': year,
        'number': number,
        'collection_time': now_iso8601_utc(),
        "open_type": "Open Government",
        "license:": "Open Government Licence v3.0",
        "language": "en",
        "format": "xhtml",
        "file_name": file_name,
        "data_link": xhtml_href
    }

    context.log.info(f"Fetching XHTML for file={file_name}: {xhtml_href}")
    text_content = fetch_text(api_legislation, xhtml_href, context, allow_fail=True)
    metadata['text'] = text_content

    return metadata

def fetch_text(api_legislation: RateLimitedApiClient, url: str, context: AssetExecutionContext, allow_fail: bool = False) -> Optional[str]:
    """
    Fetch XHTML text content from the given URL and return as text to be processed later.
    :param api_legislation:
    :param url:
    :param context:
    :param allow_fail:
    :return:
    """
    rate = getattr(api_legislation, "rate_limit_per_second", None)
    if rate and rate > 0:
        time.sleep(1.0 / rate)
    try:
        session = api_legislation.get_session()
        resp = session.get(url, timeout=20)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        context.log.error(f"Failed to fetch {url}: {e}")
        if allow_fail:
            return None
        raise

def save_record(file_name: str, record: dict):
    fname = _safe_name(file_name) or file_name
    target = os.path.join(RAW_DATA_PATH, f"{fname}.json")
    # atomic write
    with tempfile.NamedTemporaryFile("w", delete=False, dir=RAW_DATA_PATH, suffix=".tmp", encoding="utf-8") as tmp:
        json.dump(record, tmp, ensure_ascii=False, indent=2)
        tmp_path = tmp.name
    os.replace(tmp_path, target)