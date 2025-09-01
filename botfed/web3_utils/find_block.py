from web3 import Web3
import time
from typing import Optional, Literal


def find_block_by_timestamp(
    w3: Web3,
    target_timestamp: int,
    tolerance: int = 2,
    bias: Literal["left", "right"] = "left",
) -> Optional[int]:
    """
    Find a block number that corresponds to a given timestamp within tolerance.

    Args:
        w3: Web3 instance
        target_timestamp: Unix timestamp to search for
        tolerance: Tolerance in seconds (default: 2)
        bias: When multiple blocks are within tolerance, choose 'left' (earlier) or 'right' (later)

    Returns:
        Block number if found within tolerance, None otherwise
    """

    # Get latest block for upper bound
    latest_block = w3.eth.get_block("latest")
    latest_block_num = latest_block["number"]
    latest_timestamp = latest_block["timestamp"]

    # Check if target timestamp is too far in the future
    if target_timestamp > latest_timestamp + tolerance:
        print(f"Target timestamp {target_timestamp} is too far in the future")
        return None

    # Binary search bounds
    low = 0
    high = latest_block_num

    # Binary search to find approximate block
    while low <= high:
        mid = (low + high) // 2
        mid_block = w3.eth.get_block(mid)
        mid_timestamp = mid_block["timestamp"]

        time_diff = abs(mid_timestamp - target_timestamp)

        # If we found a block within tolerance, we need to check neighboring blocks
        if time_diff <= tolerance:
            return _find_best_block_in_range(w3, mid, target_timestamp, tolerance, bias)

        if mid_timestamp < target_timestamp:
            low = mid + 1
        else:
            high = mid - 1

    # If binary search didn't find anything within tolerance
    # Check the closest blocks from the search
    candidates = []

    # Check blocks around where the search ended
    for block_num in [max(0, high), min(latest_block_num, low)]:
        try:
            block = w3.eth.get_block(block_num)
            time_diff = abs(block["timestamp"] - target_timestamp)
            if time_diff <= tolerance:
                candidates.append((block_num, time_diff, block["timestamp"]))
        except:
            continue

    if not candidates:
        print(
            f"No block found within {tolerance} seconds of timestamp {target_timestamp}"
        )
        return None

    # Sort by time difference, then by block number based on bias
    candidates.sort(key=lambda x: (x[1], x[0] if bias == "left" else -x[0]))
    return candidates[0][0]


def _find_best_block_in_range(
    w3: Web3,
    center_block: int,
    target_timestamp: int,
    tolerance: int,
    bias: Literal["left", "right"],
) -> int:
    """
    Given a center block that's within tolerance, find the best block in the range.
    """
    candidates = []

    # Check a range of blocks around the center
    # We'll check up to 50 blocks in each direction (should be enough for most cases)
    search_range = 50
    latest_block_num = w3.eth.get_block("latest")["number"]

    start_block = max(0, center_block - search_range)
    end_block = min(latest_block_num, center_block + search_range)

    for block_num in range(start_block, end_block + 1):
        try:
            block = w3.eth.get_block(block_num)
            time_diff = abs(block["timestamp"] - target_timestamp)

            if time_diff <= tolerance:
                candidates.append((block_num, time_diff, block["timestamp"]))
        except:
            continue

    if not candidates:
        return center_block

    # Sort candidates by time difference first, then by bias preference
    if bias == "left":
        # Prefer earlier blocks when time difference is equal
        candidates.sort(key=lambda x: (x[1], x[0]))
    else:
        # Prefer later blocks when time difference is equal
        candidates.sort(key=lambda x: (x[1], -x[0]))

    return candidates[0][0]
