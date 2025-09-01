import os
import json
from dotenv import load_dotenv
from web3 import Web3
import argparse
from dateutil import parser as dtparser
import pytz

load_dotenv()


from .v2_backfill_pool import load_events

NULL_ADDRESS = "0x0000000000000000000000000000000000000000"


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


def get_block_by_timestamp(target_dt):
    target_ts = int(target_dt.timestamp())
    latest = web3.eth.get_block("latest")
    low = 0
    high = latest["number"]

    while low <= high:
        mid = (low + high) // 2
        block = web3.eth.get_block(mid)
        if block["timestamp"] < target_ts:
            low = mid + 1
        elif block["timestamp"] > target_ts:
            high = mid - 1
        else:
            return block["number"]
    return low if low < latest["number"] else latest["number"]


def get_total_lp_tokens_at_block(block_number):
    lp_token_contract = web3.eth.contract(
        address=POOL_ADDRESS,
        abi=[
            {
                "constant": True,
                "inputs": [],
                "name": "totalSupply",
                "outputs": [{"name": "", "type": "uint256"}],
                "payable": False,
                "stateMutability": "view",
                "type": "function",
            }
        ],
    )
    return lp_token_contract.functions.totalSupply().call(block_identifier=block_number)


def get_reserves_at_block(block_number):
    reserves = contract.functions.getReserves().call(block_identifier=block_number)
    return reserves


def simulate_passive_lp(
    events, decimals0=6, decimals1=18, initial_share=0.01, quote_is_0=True
):
    total_lp_tokens = 0
    virtual_lp_tokens = 0
    total_fees_token0 = 0
    total_fees_token1 = 0
    virtual_lp_fee_token0 = 0
    virtual_lp_fee_token1 = 0

    first_block = events[0]["blockNumber"]
    last_block = events[-1]["blockNumber"]

    print(first_block, last_block)

    # Simulate virtual LP minting at same time
    r0, r1, _ = get_reserves_at_block(first_block)
    total_lp_tokens = get_total_lp_tokens_at_block(first_block)
    virtual_lp_tokens = initial_share * total_lp_tokens
    pool_tvl_start = 2 * r0 / 10**decimals0
    virtual_tvl_start = initial_share * pool_tvl_start

    data = [
        {
            "block_num": first_block,
            "r0": r0,
            "r1": r1,
            "total_lp_tokens": total_lp_tokens,
            "virtual_lp_tokens": virtual_lp_tokens,
            "pool_tvl": pool_tvl_start,
            "virtual_tvl": virtual_tvl_start,
        }
    ]

    print(r0,r1, get_reserves_at_block(first_block))
    for e in events:
        args = e["args"]
        if e["action"] == "transfer" and args["from"] == NULL_ADDRESS:
            minted = args["value"]
            total_lp_tokens += minted
        elif e["action"] == "transfer" and args["to"] == NULL_ADDRESS:
            burned = args["value"]
            total_lp_tokens -= burned
        elif e["action"] == "add_lp":
            r0 += e["args"]["amount0"]
            r1 += e["args"]["amount1"]
        elif e["action"] == "remove_lp":
            r0 -= e["args"]["amount0"]
            r1 -= e["args"]["amount1"]
        elif e["action"] == "swap":
            # LPs earn 0.25% of swap input
            amount0In = args.get("amount0In", 0)
            amount1In = args.get("amount1In", 0)

            fee0 = int(amount0In * 30) // 10000
            fee1 = int(amount1In * 30) // 10000

            total_fees_token0 += fee0
            total_fees_token1 += fee1

            r0 += fee0 + amount0In
            r1 += fee1 + amount1In

            lp_share = virtual_lp_tokens / total_lp_tokens
            virtual_lp_fee_token0 += fee0 * lp_share
            virtual_lp_fee_token1 += fee1 * lp_share
        print(r0,r1, get_reserves_at_block(e['blockNumber']))
        pool_tvl = 2 * r0 / 10**decimals0
        virtual_tvl = pool_tvl * virtual_lp_tokens / total_lp_tokens
        data.append(
            {
                "block_num": e["blockNumber"],
                "r0": r0,
                "r1": r1,
                "total_lp_tokens": total_lp_tokens,
                "virtual_lp_tokens": virtual_lp_tokens,
                "pool_tvl": pool_tvl_start,
                "virtual_tvl": virtual_tvl,
                "total_fees_token0": total_fees_token0,
                "total_fees_token1": total_fees_token1,
                "virtual_lp_fee_token0": virtual_lp_fee_token0,
                "virtual_lp_fee_token1": virtual_lp_fee_token1,
            }
        )

    r0, r1, _ = get_reserves_at_block(last_block)
    assert r0 == data[-1]["r0"], (r0, data[-1]['r0'])
    total_lp_tokens_end = get_total_lp_tokens_at_block(last_block)
    pool_tvl_end = 2 * r0 / 10**decimals0
    virtual_tvl_end = pool_tvl_end * virtual_lp_tokens / total_lp_tokens_end
    earned_fees = (
        virtual_lp_fee_token0 + virtual_lp_fee_token1 * r0 / r1
    ) / 10**decimals0

    dt_secs = 12 * (last_block - first_block)
    secs_in_year = 365 * 24 * 60 * 60

    summary = {
        "pool_tvl_start": pool_tvl_start,
        "pool_tvl_end": pool_tvl_end,
        "virtual_lp_tokens": virtual_lp_tokens,
        "virtual_tvl_start": virtual_tvl_start,
        "virtual_tvl_end": virtual_tvl_end,
        "roi": (virtual_tvl_end / virtual_tvl_start - 1),
        "total_fees_token0": total_fees_token0 / 10**decimals0,
        "total_fees_token1": total_fees_token1 / 10**decimals1,
        "earned_token0": virtual_lp_fee_token0 / 10**decimals0,
        "earned_token1": virtual_lp_fee_token1 / 10**decimals1,
        "earned_fees_total": earned_fees,
        "earned_fees_roi": earned_fees / virtual_tvl_start,
        "earned_fees_apr": earned_fees / virtual_tvl_start * secs_in_year / dt_secs,
    }

    return data, summary


if __name__ == "__main__":
    args = parse_args()

    # Parse datetime as UTC
    sdate = dtparser.parse(args.sdate).astimezone(pytz.UTC)
    edate = dtparser.parse(args.edate).astimezone(pytz.UTC)

    events = load_events("USDC_WETH", sdate, edate)
    print(f"Found {len(events)} events.")

    print("Simulating passive LP fee earnings...")
    result = simulate_passive_lp(events, initial_share=args.share)

    print("\n=== Results ===")
    for k, v in result.items():
        print(f"{k}: {v}")
