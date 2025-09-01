import os
import json
import requests
import time
from web3 import Web3
from dotenv import load_dotenv

from ..logger import get_logger

logger = get_logger(__name__)


TOKEN_BLACKLIST = [
    Web3.to_checksum_address("0x2FD15e8E29578beDE34E6FC8Ba1d7a83D12F0eea")  # KITTY
]

# --- Config ---
CACHE_FILE = "../data/token_safety_cache.json"
BASESCAN_API = "https://api.basescan.org/api"

# --- Load .env ---
load_dotenv()
API_KEY = os.getenv("BASESCAN_API_KEY")
EOA = os.getenv("LP_ADDR")
if not EOA:
    raise ValueError("Missing LP_ADDR in .env")

# --- Load persistent cache ---
if os.path.exists(CACHE_FILE):
    with open(CACHE_FILE, "r") as f:
        token_cache = json.load(f)
else:
    token_cache = {}


def save_cache():
    with open(CACHE_FILE, "w") as f:
        json.dump(token_cache, f, indent=2)


# --- Analyze Token (uses cache) ---
def analyze_token(token_address):
    token_address = token_address.lower()
    if token_address in token_cache:
        return token_cache[token_address]

    result = {"is_listed": False}

    # Step 1: Dexscreener
    is_listed, pair_info = check_dexscreener(token_address)
    result["is_listed"] = is_listed

    # Step 2: Honeypot
    if is_listed and pair_info:
        hp = check_honeypot(
            token_address, pair_info["pair_address"], pair_info["chain_id"]
        )
        if hp:
            result.update(hp)

    token_cache[token_address] = result
    save_cache()
    return result


# --- Dexscreener API ---
def check_dexscreener(token_address):
    url = f"https://api.dexscreener.com/latest/dex/tokens/{token_address}"
    try:
        r = requests.get(url, timeout=5)
        if r.status_code != 200:
            return False, None
        data = r.json()
        pairs = data.get("pairs", [])
        if not pairs:
            return False, None
        uniswap = [p for p in pairs if p["dexId"] == "uniswap"]
        first = uniswap[0] if uniswap else pairs[0]
        return True, {
            "pair_address": first["pairAddress"],
            "chain_id": str(first["chainId"]),
        }
    except Exception as e:
        logger.error(f"❌ Dexscreener error: {e}")
        return False, None


# --- Honeypot.is API ---
def check_honeypot(token, pair, chain_id):
    url = f"https://api.honeypot.is/v2/IsHoneypot?address={token}&pair={pair}&chainID={chain_id}"
    try:
        r = requests.get(url, timeout=5)
        if r.status_code != 200:
            return None
        data = r.json()
        return {
            "is_honeypot": data.get("honeypotResult", {}).get("isHoneypot", False),
            "risk_reason": data.get("honeypotResult", {}).get("honeypotReason", ""),
            "buy_tax": data.get("simulationResult", {}).get("buyTax", 0),
            "sell_tax": data.get("simulationResult", {}).get("sellTax", 0),
            "transfer_tax": data.get("simulationResult", {}).get("transferTax", 0),
        }
    except Exception as e:
        logger.error(f"❌ Honeypot API error: {e}")
        return None


# --- Basescan Token Transfers ---
def fetch_token_transfers(
    address, page=1, offset=100, startblock=0, endblock=99999999, sort="desc"
):
    params = {
        "module": "account",
        "action": "tokentx",
        "address": address,
        "page": page,
        "offset": offset,
        "startblock": startblock,
        "endblock": endblock,
        "sort": sort,
        "apikey": API_KEY,
    }
    r = requests.get(BASESCAN_API, params=params)
    data = r.json()
    if data["status"] != "1":
        logger.error(f"Error: {data.get('message')}")
        return []
    return data["result"]


def classify_transfers(transfers, eoa):
    incoming, outgoing = [], []
    eoa = eoa.lower()
    for tx in transfers:
        if tx["to"].lower() == eoa:
            incoming.append(tx)
        elif tx["from"].lower() == eoa:
            outgoing.append(tx)
    return incoming, outgoing


def get_unique_tokens(transfers):
    seen = {}
    for tx in transfers:
        c = tx["contractAddress"]
        if c not in seen:
            seen[c] = tx
    return seen


def fetch_token_transfers(
    address, offset=100, startblock=0, endblock=99999999, sort="desc", max_pages=10
):
    all_results = []

    for page in range(1, max_pages + 1):
        params = {
            "module": "account",
            "action": "tokentx",
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

        if data.get("status") != "1" or "result" not in data:
            logger.debug(
                f"Stopped at page {page}: {data.get('message', 'Unknown error')}"
            )
            break

        all_results.extend(data["result"])

        if len(data["result"]) < offset:
            break

        if page < max_pages:
            time.sleep(1)

    return all_results


def get_safe_tokens(eoa: str, startblock: int = 0, endblock: int = 999999999):
    os.makedirs("./datasets", exist_ok=True)
    cache_path = f"./datasets/token_transfers_{eoa}.json"

    # Load from cache if available
    if os.path.exists(cache_path):
        with open(cache_path) as f:
            cached = json.load(f)
        max_pages = 1
    else:
        cached = []
        max_pages = 100

    # Fetch new transfers
    transfers = fetch_token_transfers(
        eoa, max_pages=max_pages, startblock=startblock, endblock=endblock
    )

    # Combine and deduplicate
    all_transfers = cached + transfers
    seen = {}
    for tx in all_transfers:
        c = Web3.to_checksum_address(tx["contractAddress"])
        if c not in seen:
            seen[c] = tx

    safe = {}
    updated = False
    for addr, tx in seen.items():
        if addr in safe or Web3.to_checksum_address(addr) in TOKEN_BLACKLIST:
            continue
        result = analyze_token(addr)
        if result.get("is_listed") and not result.get("is_honeypot", False):
            safe[addr] = tx
            updated = True

    if updated:
        with open(cache_path, "w") as f:
            json.dump(list(seen.values()), f, indent=2)

    return safe


# --- Main ---
if __name__ == "__main__":
    print(f"\n📦 Fetching token transfers for {EOA}...\n")
    txs = fetch_token_transfers(EOA)
    incoming, outgoing = classify_transfers(txs, EOA)

    print("\n🔍 Checking tokens for listing + honeypot risk...\n")
    token_map = get_unique_tokens(txs)
    for addr, tx in token_map.items():
        result = analyze_token(addr)
        if result["is_listed"]:
            print(f"{addr} | {tx['tokenSymbol']}")
            print(f"  Honeypot: {'✅ YES' if result.get('is_honeypot') else '❌ NO'}")
            print(f"  Reason: {result.get('risk_reason', '')}")
            print(
                f"  Buy Tax: {result.get('buy_tax')}%, Sell Tax: {result.get('sell_tax')}%\n"
            )

    safe = get_safe_tokens(EOA)
    for tx, v in safe.items():
        print(tx, v["tokenSymbol"])
