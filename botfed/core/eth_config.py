import os
import re
from dotenv import load_dotenv

load_dotenv()

# --- Explorer (Etherscan Multichain V2) ---
ETHERSCAN_V2_BASE = "https://api.etherscan.io/v2/api"
BASE_CHAIN_ID = 8453  # Base mainnet
BASESCAN_API_KEY_ENV = os.getenv("BASESCAN_API_KEY") or os.getenv("ETHERSCAN_API_KEY")

if not BASESCAN_API_KEY_ENV:
    raise RuntimeError("Missing BASESCAN_API_KEY (or ETHERSCAN_API_KEY) in environment")

BASESCAN_API_KEY = BASESCAN_API_KEY_ENV


# --- RPC (web3) endpoints: collect BASE_HTTP_URL_1..N ---
def _collect_endpoints(prefix: str) -> list[str]:
    rx = re.compile(rf"^{prefix}_(\d+)$")
    envs = []
    for k, v in os.environ.items():
        m = rx.match(k)
        if m and v:
            envs.append((int(m.group(1)), v))
    return [v for _, v in sorted(envs, key=lambda x: x[0])]


BASE_RPC_URLS = _collect_endpoints("BASE_HTTP_URL")
if not BASE_RPC_URLS:
    raise RuntimeError("No RPC URLs found. Set BASE_HTTP_URL_1 (and _2, _3...) in .env")
