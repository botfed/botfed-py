# botfed/aero/get_nfts.py
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Dict, List, Mapping, Tuple

from dotenv import load_dotenv
from web3 import Web3

from ..logger import get_logger
from ..core.explorer_client import ExplorerClient  # <- Etherscan v2 wrapper

logger = get_logger(__name__)

# --- env ---
load_dotenv()

# --- constants / contracts of interest ---
AERO_NFT_MANAGER = Web3.to_checksum_address(
    "0x827922686190790b37229fd06084350e74485b72"
)
nft_contracts = [AERO_NFT_MANAGER]

# --- io paths (repo-relative, stable regardless of CWD) ---
ROOT = Path(__file__).resolve().parents[3]
DATASETS_DIR = ROOT / "datasets"
DATASETS_DIR.mkdir(parents=True, exist_ok=True)

# --- etherscan v2 client ---
explorer = ExplorerClient()


def fetch_nft_transfers(
    address: str,
    *,
    offset: int = 1000,
    startblock: int = 0,
    endblock: int = 999_999_999,
    sort: str = "desc",
    max_pages: int = 10,
    sleep_between_pages_s: float = 1.0,
) -> List[Dict]:
    """
    Paginate account.tokennfttx for an address using Etherscan v2.
    Then filter results to contracts in `nft_contracts + [address]`
    (kept exactly as in your original logic).
    """
    contracts = nft_contracts + [address]
    all_results: List[Dict] = []

    for page in range(1, max_pages + 1):
        resp = explorer.tokennfttx(
            address=address,
            page=page,
            offset=offset,
            sort=sort,
            startblock=startblock,
            endblock=endblock,
        )

        if (
            not isinstance(resp, dict)
            or resp.get("status") != "1"
            or "result" not in resp
        ):
            logger.debug(
                f"Stopped at page {page}: {(resp or {}).get('message', 'unknown')}"
            )
            break

        page_items = resp["result"] or []
        filtered = [
            el
            for el in page_items
            if Web3.to_checksum_address(el["contractAddress"]) in contracts
        ]
        all_results.extend(filtered)

        if len(page_items) < offset:
            break

        if page < max_pages and sleep_between_pages_s:
            time.sleep(sleep_between_pages_s)

    return all_results


def get_pos_ids(
    eoa: str, startblock: int = 0, endblock: int = 999_999_999_999_99
) -> Mapping[int, Dict]:
    """
    Build a mapping {tokenId -> info} from filtered NFT transfers.
    Uses a simple JSON cache in datasets/nfts_{eoa}.json and limits pagination
    to a single page when cache exists (same as your original behavior).
    """
    cache_path = DATASETS_DIR / f"nfts_{eoa}.json"

    # Load existing cache
    if cache_path.exists():
        pos_ids = {int(k): v for k, v in json.loads(cache_path.read_text()).items()}
        max_pages = 1
    else:
        pos_ids = {}
        max_pages = 100

    # Fetch new transfers
    transfers = fetch_nft_transfers(
        eoa,
        max_pages=max_pages,
        startblock=startblock,
        endblock=endblock,
    )

    updated = False
    for tx in transfers:
        token_id = int(tx["tokenID"])
        if token_id not in pos_ids:
            pos_ids[token_id] = {
                "addr": tx["contractAddress"],
                "tokenName": tx["tokenName"],
                "tokenId": token_id,
                "tokenOwner": eoa,
            }
            updated = True

    if updated:
        cache_path.write_text(json.dumps(pos_ids, indent=2))

    return pos_ids


def classify_nft_transfers(
    transfers: List[Dict], eoa: str
) -> Tuple[List[Dict], List[Dict]]:
    incoming, outgoing = [], []
    eoa_l = eoa.lower()
    for tx in transfers:
        if tx.get("to", "").lower() == eoa_l:
            incoming.append(tx)
        elif tx.get("from", "").lower() == eoa_l:
            outgoing.append(tx)
    return incoming, outgoing


def print_summary(incoming: List[Dict], outgoing: List[Dict]) -> None:
    print(f"Incoming NFT transfers: {len(incoming)}")
    print(f"Outgoing NFT transfers: {len(outgoing)}")

    def summarize(transfers: List[Dict]) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for tx in transfers:
            key = f"{tx['tokenName']} ({tx['tokenID']})"
            counts[key] = counts.get(key, 0) + 1
            print(tx)
        return counts

    print("\nTop Incoming NFTs:")
    for name, count in summarize(incoming).items():
        print(f"{name}: {count}")

    print("\nTop Outgoing NFTs:")
    for name, count in summarize(outgoing).items():
        print(f"{name}: {count}")


if __name__ == "__main__":
    load_dotenv()
    EOA = os.environ["AERO_MANAGER_ADDRESS"]
    print(f"Fetching NFT transfers for {EOA}...\n")
    pos_ids = get_pos_ids(EOA)
    for pos_id, val in pos_ids.items():
        print(pos_id, val)
