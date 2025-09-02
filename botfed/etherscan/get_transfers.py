from __future__ import annotations

import os
import time
from typing import Dict, List

from dotenv import load_dotenv

from ..core.explorer_client import ExplorerClient  # Etherscan Multichain v2

load_dotenv()

USDC_CONTRACT = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"  # USDC on Base


def get_transfers(
    address: str,
    contract_address: str = USDC_CONTRACT,
    *,
    start_block: int = 0,
    end_block: int = 99_999_999,
    sort: str = "asc",
    max_pages: int = 10,
    sleep_sec: float = 0.2,
    page_size: int = 100,
    explorer: ExplorerClient | None = None,
) -> List[Dict]:
    """
    Return token transfers involving `address` for the given ERC-20 `contract_address` (e.g., USDC).
    Each transfer includes: timestamp, blockNumber, from, to, value, txHash.
    Filters to the standard transfer function signature.
    """
    if explorer is None:
        explorer = ExplorerClient()

    results: List[Dict] = []
    page = 1

    while page <= max_pages:
        resp = explorer.tokentx(
            address=address,
            contractaddress=contract_address,
            startblock=start_block,
            endblock=end_block,
            page=page,
            offset=page_size,
            sort=sort,
        )

        if (
            not isinstance(resp, dict)
            or resp.get("status") != "1"
            or "result" not in resp
        ):
            # v2 still uses {status,message,result}; break on no data / error
            break

        rows = resp["result"] or []

        for tx in rows:
            # Keep your original safeguard: only plain ERC20 transfer events
            if tx.get("functionName") != "transfer(address recipient,uint256 amount)":
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

        if len(rows) < page_size:
            break

        page += 1
        if sleep_sec:
            time.sleep(sleep_sec)

    return results


if __name__ == "__main__":
    import pandas as pd

    test_address = os.environ["AERO_MANAGER_GOV"]
    txs = get_transfers(test_address)
    df = pd.DataFrame(txs)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
    # USDC has 6 decimals
    df["value"] = df["value"] / 1_000_000
    print(df.head())
