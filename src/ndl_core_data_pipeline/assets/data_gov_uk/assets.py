from dagster import asset, AssetExecutionContext
from ndl_core_data_pipeline.resources.api_client import RateLimitedApiClient

@asset(group_name="data_gov_crawling")
def data_gov_feed_a(context: AssetExecutionContext, api_data_gov: RateLimitedApiClient):
    # Logic for Feed A
    return ["data/raw/feed_a.xml.json"]

@asset(group_name="data_gov_crawling")
def data_gov_feed_b(context: AssetExecutionContext, api_data_gov: RateLimitedApiClient):
    # Logic for Feed B
    return ["data/raw/feed_b.csv.json"]