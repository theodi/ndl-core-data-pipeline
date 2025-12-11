from dagster import ConfigurableResource
import requests
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Optional


class RateLimitedApiClient(ConfigurableResource):
    base_url: str
    # If None or <= 0, rate limiting is disabled
    rate_limit_per_second: Optional[float] = None

    def get_session(self):
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def get(self, endpoint, params=None):
        if getattr(self, "rate_limit_per_second", None) and self.rate_limit_per_second > 0:
            time.sleep(1.0 / self.rate_limit_per_second)

        url = endpoint if endpoint.startswith("http") else f"{self.base_url}{endpoint}"

        session = self.get_session()
        print(f"Fetching: {url} | Params: {params}")
        response = session.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()