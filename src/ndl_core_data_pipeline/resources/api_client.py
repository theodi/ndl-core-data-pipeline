from dagster import ConfigurableResource
import requests
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class RateLimitedApiClient(ConfigurableResource):
    base_url: str
    rate_limit_per_second: float = 10.0

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
        time.sleep(1.0 / self.rate_limit_per_second)

        url = endpoint if endpoint.startswith("http") else f"{self.base_url}{endpoint}"

        session = self.get_session()
        print(f"Fetching: {url} | Params: {params}")
        response = session.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()