from web3 import Web3, HTTPProvider
from .eth_config import BASE_RPC_URLS


def get_w3() -> Web3:
    # Simple: pick the first healthy URL; you can beef this up with health checks or round-robin.
    if not BASE_RPC_URLS:
        raise AssertionError("No RPC URLs found for chain BASE")
    return Web3(HTTPProvider(BASE_RPC_URLS[0], request_kwargs={"timeout": 30}))
