import json
import logging
from json import JSONDecodeError


import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from hyperliquid.utils.constants import MAINNET_API_URL
from hyperliquid.utils.error import ClientError, ServerError
from hyperliquid.utils.types import Any


class CustomHTTPAdapter(HTTPAdapter):
    def __init__(
        self, pool_connections=100, pool_maxsize=100, max_retries=3, pool_block=False
    ):
        self.pool_connections = pool_connections
        self.pool_maxsize = pool_maxsize
        self.max_retries = max_retries
        self.pool_block = pool_block
        super().__init__(
            pool_connections=pool_connections,
            pool_maxsize=pool_maxsize,
            max_retries=max_retries,
            pool_block=pool_block,
        )


def create_session(
    pool_connections=100, pool_maxsize=100, max_retries=3, pool_block=False
):
    session = requests.Session()
    adapter = CustomHTTPAdapter(
        pool_connections=pool_connections,
        pool_maxsize=pool_maxsize,
        max_retries=Retry(total=max_retries),
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


class API:
    def __init__(
        self,
        base_url=None,
    ):
        self.base_url = MAINNET_API_URL
        self.session = create_session()
        self.session.headers.update(
            {
                "Content-Type": "application/json",
            }
        )

        if base_url is not None:
            self.base_url = base_url

        self._logger = logging.getLogger(__name__)
        return

    def post(self, url_path: str, payload: Any = None) -> Any:
        if payload is None:
            payload = {}
        url = self.base_url + url_path

        response = self.session.post(url, json=payload)
        self._handle_exception(response)

        try:
            return response.json()
        except ValueError:
            return {"error": f"Could not parse JSON: {response.text}"}

    def _handle_exception(self, response):
        status_code = response.status_code
        if status_code < 400:
            return
        if 400 <= status_code < 500:
            try:
                err = json.loads(response.text)
            except JSONDecodeError:
                raise ClientError(
                    status_code, None, response.text, None, response.headers
                )
            error_data = None
            if "data" in err:
                error_data = err["data"]
            raise ClientError(
                status_code, err["code"], err["msg"], response.headers, error_data
            )
        raise ServerError(status_code, response.text)
