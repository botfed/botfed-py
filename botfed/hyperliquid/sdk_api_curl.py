import json
import logging
import pycurl
from io import BytesIO
from json import JSONDecodeError
import platform

from hyperliquid.utils.constants import MAINNET_API_URL
from hyperliquid.utils.error import ClientError, ServerError
from hyperliquid.utils.types import Any

is_mac_os = platform.system() == "Darwin"
ca_cert_path = "/etc/ssl/certs/ca-certificates.crt"


class API:
    def __init__(self, base_url=None):
        self.base_url = MAINNET_API_URL
        self._logger = logging.getLogger(__name__)
        if base_url is not None:
            self.base_url = base_url

    def post(self, url_path: str, payload: Any = None, non_blocking: bool = False) -> Any:
        if payload is None:
            payload = {}
        url = self.base_url + url_path

        response, response_available = self._curl_post(url, payload, non_blocking)
        if non_blocking and not response_available:
            return {"status": "Request sent, response not awaited"}

        self._handle_exception(response)

        try:
            return json.loads(response)
        except JSONDecodeError:
            return {"error": f"Could not parse JSON: {response}"}

    def _curl_post(self, url: str, payload: Any, non_blocking: bool, retries: int = 3, timeout_ms=1000) -> tuple:
        headers = ["Content-Type: application/json"]
        data = json.dumps(payload)
        global is_mac_os
        global ca_cert_path

        for attempt in range(retries):
            try:
                buffer = BytesIO()
                curl = pycurl.Curl()
                curl.setopt(pycurl.URL, url)
                curl.setopt(pycurl.POST, 1)
                curl.setopt(pycurl.POSTFIELDS, data)
                curl.setopt(pycurl.HTTPHEADER, headers)
                curl.setopt(pycurl.WRITEDATA, buffer)
                if not is_mac_os:
                    curl.setopt(pycurl.CAINFO, ca_cert_path)
                if non_blocking:
                    curl.setopt(pycurl.TIMEOUT_MS, 1)  # Set a very short timeout
                else:
                    curl.setopt(pycurl.TIMEOUT_MS, timeout_ms)  # Set a very short timeout
                curl.perform()
                response_code = curl.getinfo(pycurl.RESPONSE_CODE)
                curl.close()

                response = buffer.getvalue().decode('utf-8')
                if response or 200 <= response_code < 300:
                    return response, True
                else:
                    self._logger.warning(f"Attempt {attempt+1} failed with status {response_code}")
            except pycurl.error as e:
                self._logger.warning(f"Attempt {attempt+1} failed with error: {e}")
        return "", False

    def _handle_exception(self, response: str):
        try:
            response_json = json.loads(response)
        except JSONDecodeError:
            raise ServerError("Could not parse JSON response")

        if 'error' in response_json:
            status_code = response_json['error'].get('code', 500)
            if 400 <= status_code < 500:
                raise ClientError(
                    status_code, 
                    response_json['error'].get('code'), 
                    response_json['error'].get('msg'), 
                    None, 
                    response_json['error'].get('data')
                )
            raise ServerError(status_code, response_json['error'].get('msg'))


