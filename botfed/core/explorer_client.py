# botfed/core/explorer_client.py
import time
import requests
from typing import Any, Dict, Optional
from .eth_config import ETHERSCAN_V2_BASE, BASE_CHAIN_ID, BASESCAN_API_KEY


class ExplorerClient:
    """Thin wrapper over Etherscan Multichain V2. Injects chainid & apikey, retries."""

    def __init__(
        self,
        base_url: str = ETHERSCAN_V2_BASE,
        chain_id: int = BASE_CHAIN_ID,
        apikey: str = BASESCAN_API_KEY,
    ):
        if not apikey:
            raise RuntimeError("Missing BASESCAN_API_KEY / ETHERSCAN_API_KEY")
        self.base_url = base_url
        self.chain_id = chain_id
        self.apikey = apikey

    def get(
        self,
        params: Dict[str, Any],
        timeout: int = 30,
        retries: int = 3,
        backoff: float = 0.5,
    ) -> Dict[str, Any]:
        q = {"chainid": self.chain_id, "apikey": self.apikey, **params}
        last_exc: Optional[Exception] = None
        for attempt in range(retries):
            try:
                r = requests.get(self.base_url, params=q, timeout=timeout)
                r.raise_for_status()
                data = r.json()
                # Retry on NOTOK rate-limit-ish responses
                if (
                    isinstance(data, dict)
                    and data.get("status") == "0"
                    and data.get("message", "").upper() == "NOTOK"
                ):
                    last_exc = Exception(f"Etherscan NOTOK: {data.get('result')}")
                    time.sleep(backoff * (2**attempt))
                    continue
                return data
            except Exception as e:
                last_exc = e
                time.sleep(backoff * (2**attempt))
        raise RuntimeError(f"Etherscan V2 request failed: {last_exc}")

    def tokentx(self, address: str, **kwargs) -> Dict[str, Any]:
        return self.get(
            {"module": "account", "action": "tokentx", "address": address, **kwargs}
        )

    def tokennfttx(self, address: str, **kwargs) -> Dict[str, Any]:
        return self.get(
            {"module": "account", "action": "tokennfttx", "address": address, **kwargs}
        )

    def getabi(self, address: str) -> Dict[str, Any]:
        return self.get({"module": "contract", "action": "getabi", "address": address})
