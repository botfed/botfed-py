import math
import random
import time
from web3.providers.rpc import HTTPProvider
from ..logger import get_logger

logger = get_logger(__name__)


class RotatingHTTPProvider(HTTPProvider):
    def __init__(self, urls, max_backoff=3600, min_backoff=60, jitter=True):
        """
        :param urls: List of RPC endpoint URLs
        :param max_backoff: Max backoff in seconds
        :param min_backoff: Minimum backoff in seconds
        :param jitter: Add randomness to avoid thundering herd
        """
        assert urls, "At least one RPC URL required"
        self.urls = urls
        self.max_backoff = max_backoff
        self.min_backoff = min_backoff
        self.jitter = jitter

        self.backoff_until = {url: 0 for url in urls}
        self.last_url = None
        super().__init__(endpoint_uri=urls[0])  # dummy init; overridden in use

    def _get_next_url(self):
        now = time.time()
        eligible = [url for url in self.urls if now >= self.backoff_until[url]]
        # return eligible[0] if eligible else None
        return random.choice(eligible) if eligible else None

    def _apply_backoff(self, url):
        now = time.time()
        prev_expiry = self.backoff_until[url]
        elapsed = (prev_expiry - now)

        if elapsed <= 0:
            next_backoff = self.min_backoff
        else:
            next_backoff = min(self.max_backoff, math.ceil(elapsed * 2))

        if self.jitter:
            next_backoff += random.uniform(0, 5)

        self.backoff_until[url] = now + next_backoff
        return round(next_backoff)

    def make_request(self, method, params):
        retry_max = 10
        count = 0
        while count < retry_max:
            for _ in range(len(self.urls)):
                url = self._get_next_url()
                if not url:
                    break

                self.endpoint_uri = url  # dynamically override
                try:
                    response = super().make_request(method, params)
                    if "error" in response:
                        raise Exception(response["error"])
                    self.last_url = url
                    return response
                except Exception as e:
                    wait_time = self._apply_backoff(url)
                    logger.debug(f"[RPC FAIL] {url} -> {e}. Backing off for {wait_time}s")
            count += 1
        raise Exception("All RPC endpoints failed or are in backoff")
