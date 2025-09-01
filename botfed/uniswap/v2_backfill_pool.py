import datetime as dt
import os
import json
from dotenv import load_dotenv
from web3 import Web3
import argparse
from dateutil import parser as dtparser
import pytz

load_dotenv()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--sdate", default="20250401", help="Start date in YYYYMMDD format (UTC)"
    )
    parser.add_argument(
        "--edate", default="20250402", help="End date in YYYYMMDD format (UTC)"
    )
    parser.add_argument(
        "--share", type=float, default=0.01, help="Virtual LP share (default=1%)"
    )
    return parser.parse_args()


# Load env vars
WEB3_PROVIDER = os.getenv("HTTP_URL")
POOL_ADDRESS = Web3.to_checksum_address("0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc")

web3 = Web3(Web3.HTTPProvider(WEB3_PROVIDER))

# Simplified ABI with only needed events
UNISWAP_V2_ABI = json.loads(
    """ 
    [{"inputs":[],"payable":false,"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"spender","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"},{"indexed":true,"internalType":"address","name":"to","type":"address"}],"name":"Burn","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"}],"name":"Mint","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount0In","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1In","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount0Out","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1Out","type":"uint256"},{"indexed":true,"internalType":"address","name":"to","type":"address"}],"name":"Swap","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint112","name":"reserve0","type":"uint112"},{"indexed":false,"internalType":"uint112","name":"reserve1","type":"uint112"}],"name":"Sync","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"},{"constant":true,"inputs":[],"name":"DOMAIN_SEPARATOR","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"MINIMUM_LIQUIDITY","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"PERMIT_TYPEHASH","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"address","name":"","type":"address"}],"name":"allowance","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"approve","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"}],"name":"burn","outputs":[{"internalType":"uint256","name":"amount0","type":"uint256"},{"internalType":"uint256","name":"amount1","type":"uint256"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"factory","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getReserves","outputs":[{"internalType":"uint112","name":"_reserve0","type":"uint112"},{"internalType":"uint112","name":"_reserve1","type":"uint112"},{"internalType":"uint32","name":"_blockTimestampLast","type":"uint32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"_token0","type":"address"},{"internalType":"address","name":"_token1","type":"address"}],"name":"initialize","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"kLast","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"}],"name":"mint","outputs":[{"internalType":"uint256","name":"liquidity","type":"uint256"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"nonces","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"permit","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"price0CumulativeLast","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"price1CumulativeLast","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"}],"name":"skim","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"uint256","name":"amount0Out","type":"uint256"},{"internalType":"uint256","name":"amount1Out","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"swap","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[],"name":"sync","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"token0","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"token1","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"transfer","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"transferFrom","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"}]
    """
)

# UNISWAP_V2_ABI = json.loads(
#     """
# [
#   {"anonymous":false,"inputs":[
#     {"indexed":true,"name":"sender","type":"address"},
#     {"indexed":false,"name":"amount0","type":"uint256"},
#     {"indexed":false,"name":"amount1","type":"uint256"}
#   ],"name":"Mint","type":"event"},
#   {"anonymous":false,"inputs":[
#     {"indexed":true,"name":"sender","type":"address"},
#     {"indexed":false,"name":"amount0","type":"uint256"},
#     {"indexed":false,"name":"amount1","type":"uint256"},
#     {"indexed":true,"name":"to","type":"address"}
#   ],"name":"Swap","type":"event"},
#   {"anonymous":false,"inputs":[
#     {"indexed":true,"name":"sender","type":"address"},
#     {"indexed":false,"name":"amount0","type":"uint256"},
#     {"indexed":false,"name":"amount1","type":"uint256"},
#     {"indexed":true,"name":"to","type":"address"}
#   ],"name":"Burn","type":"event"},
#   {"anonymous":false,"inputs":[
#     {"indexed":true,"name":"from","type":"address"},
#     {"indexed":true,"name":"to","type":"address"},
#     {"indexed":false,"name":"value","type":"uint256"}
#   ],"name":"Transfer","type":"event"}
# ]
# """
# )

contract = web3.eth.contract(address=POOL_ADDRESS, abi=UNISWAP_V2_ABI)

# Optional: filter range


def get_block_by_timestamp(target_dt, before=False, start_guess=None, end_guess=None):
    """
    Finds the block closest to the given UTC datetime.

    Args:
        target_dt (datetime): Target datetime (must be UTC).
        before (bool): If True, return the last block <= timestamp.
                       If False, return the first block >= timestamp.

    Returns:
        int: Block number
    """
    assert target_dt.tzinfo is not None, "Datetime must be timezone-aware (UTC)"

    target_ts = int(target_dt.timestamp())
    if end_guess is None:
        latest_block = web3.eth.get_block("latest")["number"]
    else:
        latest_block = end_guess
    low = start_guess if start_guess else 0
    high = latest_block
    closest_block = None

    while low <= high:
        mid = (low + high) // 2
        block = web3.eth.get_block(mid)
        block_ts = block["timestamp"]

        if block_ts < target_ts:
            low = mid + 1
        elif block_ts > target_ts:
            high = mid - 1
        else:
            return block["number"]  # exact match

        # Track the closest so far
        if closest_block is None or abs(block_ts - target_ts) < abs(
            web3.eth.get_block(closest_block)["timestamp"] - target_ts
        ):
            closest_block = block["number"]

    # Final choice: closest before or after
    low_block = web3.eth.get_block(low) if low <= latest_block else None
    high_block = web3.eth.get_block(high) if high >= 0 else None

    if before:
        if high_block and high_block["timestamp"] <= target_ts:
            return high_block["number"]
        elif low_block:
            return low_block["number"]
    else:
        if low_block and low_block["timestamp"] >= target_ts:
            return low_block["number"]
        elif high_block:
            return high_block["number"]

    # Fallback (shouldn't happen)
    return closest_block


def fetch_events(block_start: int, block_end: int, chunk_size: int = 1000):
    events = []
    event_types = {
        "Mint": contract.events.Mint,
        "Burn": contract.events.Burn,
        "Swap": contract.events.Swap,
        "Transfer": contract.events.Transfer,
    }

    for name, event in event_types.items():
        print(f"Fetching {name} events...")
        for chunk_start in range(block_start, block_end + 1, chunk_size):
            chunk_end = min(chunk_start + chunk_size - 1, block_end)
            try:
                logs = event().get_logs(fromBlock=chunk_start, toBlock=chunk_end)
                for log in logs:
                    entry = {
                        "action": (
                            name.lower()
                            if name not in ["Mint", "Burn"]
                            else ("add_lp" if name == "Mint" else "remove_lp")
                        ),
                        "blockNumber": log["blockNumber"],
                        "logIndex": log["logIndex"],
                        "txHash": log["transactionHash"].hex(),
                        "args": dict(log["args"]),
                    }
                    events.append(entry)
            except Exception as e:
                print(
                    f"Error fetching {name} logs for blocks {chunk_start}-{chunk_end}: {e}"
                )

    print(f"Fetched {len(events)} total events")
    return sorted(events, key=lambda x: (x["blockNumber"], x["logIndex"]))


def get_fname(s: str, d: dt.datetime):
    return f"../data/uniswap/v2/{s}/{d.year}/{d.strftime('%Y%m%d')}.json"


def load_events(symbol: str, sdate: dt.datetime, edate: dt.datetime):
    events = []
    while sdate <= edate:
        fname = get_fname(symbol, sdate)
        with open(fname, "r") as f:
            events += json.load(f)["events"]
        sdate += dt.timedelta(days=1)
    return events


if __name__ == "__main__":
    args = parse_args()

    # Parse datetime as UTC
    sdate = dt.datetime.strptime(args.sdate, "%Y%m%d").replace(tzinfo=dt.timezone.utc)
    edate = dt.datetime.strptime(args.edate, "%Y%m%d").replace(tzinfo=dt.timezone.utc)

    print(f"Resolving blocks for range {sdate} to {edate}...")

    symbol = "USDC_WETH"

    block_start = None
    block_end = None
    block_delta = int(60 * 60 * 24) // int(12)

    while sdate < edate:
        fname = get_fname(symbol, sdate)
        if block_end is None:
            block_start = get_block_by_timestamp(sdate)
            block_end = get_block_by_timestamp(sdate + dt.timedelta(days=1)) - 1
        else:
            block_start = block_end + 1
            block_end = (
                get_block_by_timestamp(
                    sdate + dt.timedelta(days=1),
                    start_guess=block_start,
                    end_guess=block_start + block_delta,
                )
                - 1
            )

        print(
            f"Fetching events from block {sdate} ({block_start}) to {sdate + dt.timedelta(days=1)} ({block_end})..."
        )

        base_dir = os.path.dirname(fname)
        if not os.path.exists(base_dir):
            print(base_dir)
            os.makedirs(base_dir)

        events = fetch_events(block_start, block_end)
        data = {
            "block_start": block_start,
            "block_end": block_end,
            "events": events,
        }
        with open(fname, "w") as f:
            json.dump(data, f, indent=2)
        print(f"Found {len(events)} events.")

        sdate += dt.timedelta(days=1)
