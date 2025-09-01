import os
import requests
from typing import Mapping, Dict
import time
import json
from web3 import Web3
from dotenv import load_dotenv
from ..logger import get_logger


logger = get_logger(__name__)

# Load API key from .env
load_dotenv()
API_KEY = os.getenv("BASESCAN_API_KEY")
BASESCAN_API = "https://api.basescan.org/api"


AERO_NFT_MANAGER = Web3.to_checksum_address(
    "0x827922686190790b37229fd06084350e74485b72"
)
nft_contracts = [AERO_NFT_MANAGER]


def fetch_nft_transfers(
    address, offset=1000, startblock=0, endblock=999999999, sort="desc", max_pages=10
):
    contracts = nft_contracts + [address]
    all_results = []

    for page in range(1, max_pages + 1):
        params = {
            "module": "account",
            "action": "tokennfttx",
            "address": address,
            "page": page,
            "offset": offset,
            "startblock": startblock,
            "endblock": endblock,
            "sort": sort,
            "apikey": API_KEY,
        }
        response = requests.get(BASESCAN_API, params=params)
        data = response.json()

        if data["status"] != "1" or "result" not in data:
            print(f"Stopped at page {page}: {data.get('message', 'Unknown error')}")
            break

        filtered = [
            el
            for el in data["result"]
            if Web3.to_checksum_address(el["contractAddress"]) in contracts
        ]
        all_results.extend(filtered)

        if len(data["result"]) < offset:
            break

        if page < max_pages:
            time.sleep(1)

    return all_results


def get_pos_ids(eoa: str, startblock=0, endblock=99999999999999) -> Mapping[int, Dict]:
    os.makedirs("./datasets", exist_ok=True)
    cache_path = f"./datasets/nfts_{eoa}.json"

    # Load existing cache
    if os.path.exists(cache_path):
        with open(cache_path, "r") as f:
            pos_ids = {int(k): v for k, v in json.load(f).items()}
        max_pages = 1
    else:
        pos_ids = {}
        max_pages = 100

    # Fetch new transfers
    transfers = fetch_nft_transfers(eoa, max_pages=max_pages, startblock=startblock, endblock=endblock)

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

    # Write back to cache if new items found
    if updated:
        with open(cache_path, "w") as f:
            json.dump(pos_ids, f, indent=2)

    return pos_ids


def classify_nft_transfers(transfers, eoa):
    incoming = []
    outgoing = []
    eoa = eoa.lower()
    for tx in transfers:
        if tx["to"].lower() == eoa:
            incoming.append(tx)
        elif tx["from"].lower() == eoa:
            outgoing.append(tx)
    return incoming, outgoing


def print_summary(incoming, outgoing):
    print(f"Incoming NFT transfers: {len(incoming)}")
    print(f"Outgoing NFT transfers: {len(outgoing)}")

    def summarize(transfers):
        counts = {}
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
    import os
    import dotenv

    dotenv.load_dotenv()
    EOA = os.environ["AERO_MANAGER_ADDRESS"]
    print(f"Fetching NFT transfers for {EOA}...\n")
    pos_ids = get_pos_ids(EOA)
    for pos_id, val in pos_ids.items():
        print(pos_id, val)
