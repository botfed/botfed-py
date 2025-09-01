import requests
import time
import os
from dotenv import load_dotenv

load_dotenv()
BASESCAN_API = "https://api.basescan.org/api"
USDC_CONTRACT = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"  # USDC on Base
API_KEY = os.getenv("BASESCAN_API_KEY")


def get_transfers(
    address: str,
    contract_address: str = USDC_CONTRACT,
    apikey: str = API_KEY,
    start_block: int = 0,
    end_block: int = 99999999,
    sort: str = "asc",
    max_pages: int = 10,
    sleep_sec: float = 0.2,
):
    """
    Returns a list of token transfers involving `address` for the given ERC-20 `contract_address` (e.g. USDC).
    Each transfer includes: timestamp, blockNumber, from, to, value, txHash
    """
    results = []
    page = 1
    offset = 100  # max per page

    while page <= max_pages:
        params = {
            "module": "account",
            "action": "tokentx",
            "address": address,
            "contractaddress": contract_address,
            "startblock": start_block,
            "endblock": end_block,
            "page": page,
            "offset": offset,
            "sort": sort,
            "apikey": apikey,
        }

        resp = requests.get(BASESCAN_API, params=params)
        data = resp.json()

        if data["status"] != "1" or "result" not in data:
            print(f"Stopped at page {page}: {data.get('message', 'Unknown error')}")
            break

        for tx in data["result"]:
            if tx["functionName"] != "transfer(address recipient,uint256 amount)":
                continue
            results.append(
                {
                    "timestamp": int(tx["timeStamp"]),
                    "blockNumber": int(tx["blockNumber"]),
                    "from": tx["from"],
                    "to": tx["to"],
                    "value": int(tx["value"]),
                    "txHash": tx["hash"],
                }
            )

        if len(data["result"]) < offset:
            break

        page += 1
        time.sleep(sleep_sec)

    return results


if __name__ == "__main__":
    import pandas as pd

    test_address = os.environ["AERO_MANAGER_GOV"]
    txs = get_transfers(test_address)
    df = pd.DataFrame(txs)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
    df["value"] /= 1e6
    print(df.head())
