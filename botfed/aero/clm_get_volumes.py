import json
import time
from web3 import Web3
from ..logger import get_logger

from .vars import get_w3, TEST_BLOCK, TEST_POOL_ID
from .abis.pool_clmm import ABI as POOL_MANAGER_ABI

logger = get_logger(__name__)

BLOCK_TIME_SECS = 2
SECS_IN_MIN = 60
SECS_IN_HOUR = 3600
SECS_IN_DAY = SECS_IN_HOUR * 24
DAYS_AGO = 7
BLOCKS_AGO_START = int(DAYS_AGO * SECS_IN_DAY / BLOCK_TIME_SECS)
SAMPLE_SIZE = int(SECS_IN_MIN * 10 / BLOCK_TIME_SECS)
SAMPLE_FREQ = int(SECS_IN_HOUR)
CHUNK_SIZE = 10_000


def get_swap_events_at_block(w3, pool_address, pool_abi, from_block, to_block):
    contract = w3.eth.contract(address=pool_address, abi=pool_abi)

    # Create filter for that block and poolId (indexed topic)
    event_filter = contract.events.Swap.create_filter(
        fromBlock=from_block, toBlock=to_block
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


def estimate_swap_vlm(w3, pool_id, block_end, quote_curr, quote_dec):
    logger.debug(f"Estimating swap vlm up to block {block_end}")
    block_start = block_end - BLOCKS_AGO_START
    all_swaps = []

    block_start = CHUNK_SIZE * (block_start // CHUNK_SIZE)

    for chunk_start in range(block_start, block_end, CHUNK_SIZE):
        chunk_end = min(chunk_start + CHUNK_SIZE - 1, block_end)
        fpath = f"./datasets/aero_swaps_{pool_id}_{chunk_start}_{chunk_end}.json"

        try:
            with open(fpath, "r") as f:
                data = json.load(f)
                swaps = data["swaps"]
            logger.debug(f"Loaded cached swaps for blocks {chunk_start}-{chunk_end}")
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.debug(f"Fetching swaps for blocks {chunk_start}-{chunk_end} - {e}")
            swaps = []
            block = chunk_start
            while block < chunk_end:
                swaps += get_swap_events_at_block(
                    w3,
                    pool_id,
                    POOL_MANAGER_ABI,
                    block,
                    min(block + SAMPLE_SIZE, chunk_end),
                )
                block += SAMPLE_FREQ
                time.sleep(1)
            swaps = [sanitize(s) for s in swaps]
            with open(fpath, "w") as f:
                json.dump({"swaps": swaps}, f)

        all_swaps.extend(swaps)

    logger.debug(f"Got {len(all_swaps)} swaps spanning {BLOCKS_AGO_START} blocks")

    vlm = sum([abs(s[f"amount{quote_curr}"]) for s in all_swaps]) / 10**quote_dec
    vlm_estimate = vlm * SAMPLE_FREQ / SAMPLE_SIZE
    vlm_daily = vlm_estimate / DAYS_AGO
    return vlm_estimate, vlm_daily, all_swaps


def estimate_swap_vlm_old(w3, pool_id, block_end, quote_curr, quote_dec):
    logger.debug(f"Estimating swap vlm for block {block_end}")
    block_start = block_end - BLOCKS_AGO_START

    fpath = f"./datasets/aero_swaps_{pool_id}_{block_start}_{block_end}.json"

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
                pool_id,
                POOL_MANAGER_ABI,
                block,
                min(block + SAMPLE_SIZE, block_end),
            )
            block += SAMPLE_FREQ
            time.sleep(1)
        swaps = [sanitize(s) for s in swaps]
        json.dump({"swaps": swaps}, open(fpath, "w"))
    logger.debug(f"Got {len(swaps)} swap spanning {BLOCKS_AGO_START} blocks")
    vlm = sum([abs(s[f"amount{quote_curr}"]) for s in swaps]) / 10**quote_dec
    vlm_estimate = vlm * SAMPLE_FREQ / SAMPLE_SIZE
    vlm_daily = vlm_estimate / DAYS_AGO
    return vlm_estimate, vlm_daily, swaps


if __name__ == "__main__":
    w3 = get_w3()
    vlm_estimate, vlm_daily, _ = estimate_swap_vlm(w3, TEST_POOL_ID, TEST_BLOCK)
    logger.debug(
        f"Total VLM is ${vlm_estimate:,.0f}, Avg Daily VLM is ${vlm_daily:,.0f} over {DAYS_AGO} days"
    )
