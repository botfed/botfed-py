from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

import requests
from dotenv import load_dotenv
from web3 import Web3

from ..logger import get_logger
from ..core.explorer_client import ExplorerClient  # <-- v2 wrapper

logger = get_logger(__name__)

# --- Setup / env ---
load_dotenv()

EOA = os.getenv("LP_ADDR")
if not EOA:
    raise ValueError("Missing LP_ADDR in .env")

# Thin Etherscan v2 client (injects chainid+apikey, handles NOTOK/retries)
explorer = ExplorerClient()

# --- Token hard filters ---
TOKEN_BLACKLIST = {
    Web3.to_checksum_address("0x2FD15e8E29578beDE34E6FC8Ba1d7a83D12F0eea")  # KITTY
}

# --- Caching paths (project-relative) ---
ROOT = Path(__file__).resolve().parents[3]  # repo root (…/botfed)
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
CACHE_FILE = DATA_DIR / "token_safety_cache.json"

DATASETS_DIR = ROOT / "datasets"
DATASETS_DIR.mkdir(parents=True, exist_ok=True)

# --- Persistent cache for token safety lookups ---
if CACHE_FILE.exists():
    token_cache: Dict[str, Dict[str, Any]] = json.loads(CACHE_FILE.read_text())
else:
    token_cache = {}


def _save_cache() -> None:
    CACHE_FILE.write_text(json.dumps(token_cache, indent=2))


# ---------- External intel ----------
def check_dexscreener(token_address: str) -> Tuple[bool, Dict[str, str] | None]:
    """Return (is_listed, {'pair_address','chain_id'}) using Dexscreener."""
    url = f"https://api.dexscreener.com/latest/dex/tokens/{token_address}"
    try:
        r = requests.get(url, timeout=5)
        r.raise_for_status()
        data = r.json()
        pairs = data.get("pairs", []) or []
        if not pairs:
            return False, None

        # Prefer Uniswap on Base if present; otherwise first result.
        uniswap = [p for p in pairs if p.get("dexId") == "uniswap"]
        first = uniswap[0] if uniswap else pairs[0]

        return True, {
            "pair_address": first["pairAddress"],
            "chain_id": str(first["chainId"]),
        }
    except Exception as e:
        logger.error(f"❌ Dexscreener error: {e}")
        return False, None


def check_honeypot(token: str, pair: str, chain_id: str) -> Dict[str, Any] | None:
    """Honeypot.is v2 simulation report."""
    url = (
        "https://api.honeypot.is/v2/IsHoneypot"
        f"?address={token}&pair={pair}&chainID={chain_id}"
    )
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


# ---------- Etherscan v2 (through ExplorerClient) ----------
def fetch_token_transfers(
    address: str,
    *,
    startblock: int = 0,
    endblock: int = 999_999_999,
    sort: str = "desc",
    page_size: int = 100,
    max_pages: int = 100,
    inter_page_sleep_s: float = 1.0,
) -> List[Dict[str, Any]]:
    """Paginate /account/tokentx via Etherscan v2. Stops when a page < page_size."""
    all_results: List[Dict[str, Any]] = []
    for page in range(1, max_pages + 1):
        resp = explorer.tokentx(
            address=address,
            page=page,
            offset=page_size,
            sort=sort,
            startblock=startblock,
            endblock=endblock,
        )

        # Etherscan v2 still returns {status,message,result}
        if not isinstance(resp, dict) or resp.get("status") != "1":
            msg = (resp or {}).get("message", "unknown")
            logger.debug(f"Stopped at page {page}: {msg}")
            break

        chunk = resp.get("result") or []
        all_results.extend(chunk)

        if len(chunk) < page_size:
            break
        if page < max_pages and inter_page_sleep_s:
            time.sleep(inter_page_sleep_s)

    return all_results


# ---------- Helpers ----------
def classify_transfers(transfers: List[Dict[str, Any]], eoa: str):
    incoming, outgoing = [], []
    eoa_l = eoa.lower()
    for tx in transfers:
        if tx.get("to", "").lower() == eoa_l:
            incoming.append(tx)
        elif tx.get("from", "").lower() == eoa_l:
            outgoing.append(tx)
    return incoming, outgoing


def get_unique_tokens(transfers: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Map contractAddress -> first seen transfer row."""
    seen: Dict[str, Dict[str, Any]] = {}
    for tx in transfers:
        c = tx.get("contractAddress")
        if c and c not in seen:
            seen[c] = tx
    return seen


# ---------- Safety analysis (with cache) ----------
def analyze_token(token_address: str) -> Dict[str, Any]:
    """Check listing + honeypot risk, caching by lowercase address."""
    token_address_l = token_address.lower()
    if token_address_l in token_cache:
        return token_cache[token_address_l]

    result: Dict[str, Any] = {"is_listed": False}

    # 1) Listing intel (Dexscreener)
    is_listed, pair_info = check_dexscreener(token_address)
    result["is_listed"] = is_listed

    # 2) Honeypot simulation when listed
    if is_listed and pair_info:
        hp = check_honeypot(
            token_address, pair_info["pair_address"], pair_info["chain_id"]
        )
        if hp:
            result.update(hp)

    token_cache[token_address_l] = result
    _save_cache()
    return result


def get_safe_tokens(eoa: str, *, startblock: int = 0, endblock: int = 999_999_999):
    """Return {token_address -> sample_tx} for tokens that look tradable (not honeypots)."""
    cache_path = DATASETS_DIR / f"token_transfers_{eoa}.json"

    # Use existing on-disk history to cap new pagination
    if cache_path.exists():
        cached = json.loads(cache_path.read_text())
        max_pages = 1
    else:
        cached = []
        max_pages = 100

    new_transfers = fetch_token_transfers(
        eoa, startblock=startblock, endblock=endblock, max_pages=max_pages
    )

    # Combine and dedupe by contractAddress
    all_transfers = cached + new_transfers
    by_contract: Dict[str, Dict[str, Any]] = {}
    for tx in all_transfers:
        c_raw = tx.get("contractAddress")
        if not c_raw:
            continue
        c = Web3.to_checksum_address(c_raw)
        if c not in by_contract:
            by_contract[c] = tx

    safe: Dict[str, Dict[str, Any]] = {}
    updated = False
    for addr, tx in by_contract.items():
        if addr in TOKEN_BLACKLIST:
            continue
        result = analyze_token(addr)
        if result.get("is_listed") and not result.get("is_honeypot", False):
            safe[addr] = tx
            updated = True

    if updated:
        cache_path.write_text(json.dumps(list(by_contract.values()), indent=2))

    return safe


# ---------- CLI quick check ----------
if __name__ == "__main__":
    print(f"\n📦 Fetching token transfers for {EOA}...\n")
    txs = fetch_token_transfers(EOA)
    incoming, outgoing = classify_transfers(txs, EOA)

    print(f"Incoming: {len(incoming)} | Outgoing: {len(outgoing)}")

    print("\n🔍 Checking tokens for listing + honeypot risk...\n")
    token_map = get_unique_tokens(txs)
    for addr, tx in token_map.items():
        result = analyze_token(addr)
        if result.get("is_listed"):
            print(f"{addr} | {tx.get('tokenSymbol', '?')}")
            hp = result.get("is_honeypot")
            print(f"  Honeypot: {'✅ YES' if hp else '❌ NO'}")
            if result.get("risk_reason"):
                print(f"  Reason: {result['risk_reason']}")
            print(
                f"  Buy Tax: {result.get('buy_tax', 0)}%, "
                f"Sell Tax: {result.get('sell_tax', 0)}%, "
                f"Transfer Tax: {result.get('transfer_tax', 0)}%\n"
            )

    print("\n✅ Safe tokens (listed, not honeypot):")
    safe = get_safe_tokens(EOA)
    for taddr, tx in safe.items():
        print(taddr, tx.get("tokenSymbol", "?"))
