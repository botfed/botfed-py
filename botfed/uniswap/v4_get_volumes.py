import json
import csv
from web3 import Web3
from ..logger import get_logger

from .v4_vars import POOL_MANAGER_ADDR, POOL_ID_HEX, w3
from .abis.pool_manager import ABI as POOL_MANAGER_ABI

logger = get_logger(__name__)

SECS_IN_MIN = 60
SECS_IN_HOUR = 3600
SECS_IN_DAY = SECS_IN_HOUR * 24
DAYS_AGO = 7
BLOCKS_AGO_START = int(DAYS_AGO * SECS_IN_DAY)
SAMPLE_SIZE = int(SECS_IN_MIN * 10)
SAMPLE_FREQ = int(SECS_IN_HOUR)


def get_swap_events_at_block(
    w3, pool_manager_address, pool_manager_abi, pool_id, from_block, to_block
):
    contract = w3.eth.contract(address=pool_manager_address, abi=pool_manager_abi)

    # Ensure poolId is bytes32 (32-byte hex)
    if isinstance(pool_id, str):
        pool_id = Web3.to_bytes(hexstr=pool_id)

    # Create filter for that block and poolId (indexed topic)
    event_filter = contract.events.Swap.create_filter(
        fromBlock=from_block, toBlock=to_block, argument_filters={"id": pool_id}
    )

    logs = event_filter.get_all_entries()
    parsed = [dict(e["args"]) for e in logs]
    return parsed


# Recursively convert bytes to hex strings
def sanitize(obj):
    if isinstance(obj, dict):
        return {k: sanitize(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize(i) for i in obj]
    elif isinstance(obj, bytes):
        return Web3.to_hex(obj)
    else:
        return obj


def estimate_swap_vlm(w3, pool_id, block_end):
    logger.info("Estimating swap vlm")
    quote_curr = 1
    quote_dec = 6
    block_start = block_end - BLOCKS_AGO_START

    fpath = (
        f"./datasets/univ4_swaps_{Web3.to_hex(pool_id)}_{block_start}_{block_end}.json"
    )

    try:
        data = json.load(open(fpath, "r"))
        swaps = data["swaps"]
    except Exception as e:
        logger.error(e)

        swaps = []

        block = block_start
        while block < block_end:
            swaps += get_swap_events_at_block(
                w3,
                POOL_MANAGER_ADDR,
                POOL_MANAGER_ABI,
                pool_id,
                block,
                min(block + SAMPLE_SIZE, block_end),
            )
            block += SAMPLE_FREQ
        swaps = [sanitize(s) for s in swaps]
        json.dump({"swaps": swaps}, open(fpath, "w"))
    print(f"Got {len(swaps)} swap spanning {BLOCKS_AGO_START} blocks")
    vlm = sum([abs(s[f"amount{quote_curr}"]) for s in swaps]) / 10**quote_dec
    vlm_estimate = vlm * SAMPLE_FREQ / SAMPLE_SIZE
    vlm_daily = vlm_estimate / DAYS_AGO
    return vlm_estimate, vlm_daily, swaps


if __name__ == "__main__":
    vlm_estimate, vlm_daily, _ = estimate_swap_vlm(w3, POOL_ID_HEX, 19511913)
    print(
        f"Total VLM is ${vlm_estimate:,.0f}, Avg Daily VLM is ${vlm_daily:,.0f} over {DAYS_AGO} days"
    )
