from dagster import asset, AssetExecutionContext
from ndl_core_data_pipeline.resources.api_client import RateLimitedApiClient

CATEGORIES = []

# Useful data.gov.uk endpoints
#
# - All categories: https://ckan.publishing.service.gov.uk/api/3/action/package_search?facet.field=[%22theme-primary%22]&rows=0
# - All License types: https://ckan.publishing.service.gov.uk/api/3/action/package_search?facet.field=[%22license_id%22]&rows=0
# - List last 10 public "government" category datasets: https://ckan.publishing.service.gov.uk/api/action/package_search?fq=theme-primary:government%20AND%20license_id:(uk-ogl%20OR%20cc-by)&sort=metadata_modified%20desc&rows=10

@asset(group_name="data_gov_crawling")
def data_gov_feed_a(context: AssetExecutionContext, api_data_gov: RateLimitedApiClient):
    #
    #
    return ["data/raw/feed_a.xml.json"]

@asset(group_name="data_gov_crawling")
def data_gov_feed_b(context: AssetExecutionContext, api_data_gov: RateLimitedApiClient):
    # Logic for Feed B
    return ["data/raw/feed_b.csv.json"]