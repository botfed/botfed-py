import time
import numpy as np
from web3 import Web3
import pandas as pd
from ..logger import get_logger
from .abis.pool_clmm import ABI as POOL_ABI
from .abis.multicall3 import ABI as MULTICALL3_ABI
from .helpers import get_quote_token, fetch_info_from_pool_id
from .vars import (
    TEST_POOL_ID,
    get_w3,
    TEST_BLOCK,
    MULTICALL3_ADDR,
    SECS_IN_DAY,
    SECS_PER_BLOCK,
)


logger = get_logger(__name__)

SAMPLE_FRE_MIN = 30
PERIODS_IN_DAY = int(24 * 60 / SAMPLE_FRE_MIN)


def lvr(sigma, sqrt_pb, sqrt_pa, sqrt_p):
    # s = sqrt_pa * sqrt_pb
    s = sqrt_p
    return sigma**2 / 8 * (1 - 0.5 * (s / sqrt_pb + sqrt_pa / s)) ** -1


def fetch_calls(codec, multicall, pool_contract, block: int):
    calls = [
        pool_contract.functions.liquidity()._encode_transaction_data(),
        pool_contract.functions.stakedLiquidity()._encode_transaction_data(),
        pool_contract.functions.slot0()._encode_transaction_data(),
        pool_contract.functions.rewardRate()._encode_transaction_data(),
        pool_contract.functions.fee()._encode_transaction_data(),
        pool_contract.functions.tickSpacing()._encode_transaction_data(),
    ]
    aggregate_call = multicall.functions.aggregate(
        [(pool_contract.address, call) for call in calls]
    )
    result = aggregate_call.call(block_identifier=block)

    _, return_data = result  # result is (blockNumber, [ret1, ret2])

    # Decode stakedLiquidity (returns uint128)
    liq = codec.decode(["uint128"], return_data[0])[0]
    staked_liq = codec.decode(["uint128"], return_data[1])[0]
    slot0 = codec.decode(
        ["uint160", "int24", "uint16", "uint16", "uint16", "bool"], return_data[2]
    )
    reward_rate = codec.decode(["uint256"], return_data[3])[0]
    fee = codec.decode(["uint24"], return_data[4])[0]
    tick_spacing = codec.decode(["int24"], return_data[5])[0]
    return liq, staked_liq, slot0, reward_rate, fee, tick_spacing


def sample_active_liq_pio(
    w3: Web3,
    pool_id: str,
    multicall_addr: str,
    block: int,
    fetch_calls=fetch_calls,
):
    codec = w3.codec
    pool = w3.eth.contract(address=pool_id, abi=POOL_ABI)
    multicall = w3.eth.contract(address=multicall_addr, abi=MULTICALL3_ABI)

    sleep_sec = 10
    while True:
        try:
            block_obj = w3.eth.get_block(block)
            active_liq, staked_liq, slot0, reward_rate, fee, tick_spacing = fetch_calls(
                codec, multicall, pool, block
            )
            break
        except Exception as e:
            logger.error(e)
            time.sleep(sleep_sec)
            sleep_sec += 10
    liq = {
        "timestamp": block_obj["timestamp"],
        "block": block,
        "active_tick": slot0[1],
        "active_liquidity": active_liq,
        "staked_liquidity": staked_liq,
        "reward_rate": reward_rate,
        "sqrtPriceX96": slot0[0],
        "fee": fee,
        "tick_spacing": tick_spacing,
    }

    return liq


def sample_active_liq(
    w3: Web3,
    pool_id: str,
    multicall_addr: str,
    block_start: int,
    block_end: int,
    block_spacing: int,
    fetch_calls=fetch_calls,
):
    block_spacing = max(1, int(block_spacing))
    codec = w3.codec
    pool = w3.eth.contract(address=pool_id, abi=POOL_ABI)
    multicall = w3.eth.contract(address=multicall_addr, abi=MULTICALL3_ABI)

    liqs = []

    block = block_start
    while block < block_end:
        logger.debug(f"{block} / {block_end}")
        sleep_sec = 10
        while True:
            try:
                block_obj = w3.eth.get_block(block)
                liq, staked_liq, slot0, reward_rate, fee, tick_spacing = fetch_calls(
                    codec, multicall, pool, block
                )
                break
            except Exception as e:
                logger.error(e)
                time.sleep(sleep_sec)
                sleep_sec += 10
        liqs.append(
            {
                "timestamp": block_obj["timestamp"],
                "block": block,
                "active_tick": slot0[1],
                "liq": liq,
                "staked_liq": staked_liq,
                "reward_rate": reward_rate,
                "sqrtPriceX96": slot0[0],
                "fee": fee,
                "tick_spacing": tick_spacing,
            }
        )

        block += block_spacing

    return liqs


def get_sample_active_liq(
    w3: Web3,
    pool_id: str,
    multicall_addr: str,
    block_start: int,
    block_end: int,
    block_spacing: int,
    fetch_calls=fetch_calls,
    chunk_size=int(10000),
):
    dfs = []

    block_start = chunk_size * (block_start // chunk_size)

    pool_info = fetch_info_from_pool_id(w3, pool_id, block_end)
    _, _, quote_pos = get_quote_token(pool_info.token0.symbol, pool_info.token1.symbol)
    quote_dec = (
        pool_info.token0.decimals if quote_pos == 0 else pool_info.token1.decimals
    )
    print(quote_pos, quote_dec)

    for chunk_start in range(block_start, block_end + 1, chunk_size):
        chunk_end = min(chunk_start + chunk_size - 1, block_end)
        if chunk_end == chunk_start:
            continue
        fpath = f"./datasets/aero_activ_liq_{pool_id}_{chunk_start}_{chunk_end}_{block_spacing}.csv"

        try:
            df = pd.read_csv(fpath)
            logger.debug(f"Loaded cached chunk: {chunk_start}-{chunk_end}")
        except FileNotFoundError:
            logger.debug(f"Fetching chunk: {chunk_start}-{chunk_end}")
            liqs = sample_active_liq(
                w3,
                pool_id,
                multicall_addr,
                chunk_start,
                chunk_end,
                block_spacing,
                fetch_calls=fetch_calls,
            )
            df = pd.DataFrame(liqs)
            df.to_csv(fpath, index=False)

        dfs.append(df)

    df = pd.concat(dfs, ignore_index=True)

    df["tick_lower"] = df["tick_spacing"] * (df["active_tick"] // df["tick_spacing"])
    df["tick_upper"] = df["tick_lower"] + df["tick_spacing"]
    # df["tick_lower"] = df["active_tick"] - 10*100
    # df["tick_upper"] = df["active_tick"] + 10*100
    df["sqrt_pb"] = 1.0001 ** (df["tick_upper"] / 2)
    df["sqrt_pa"] = 1.0001 ** (df["tick_lower"] / 2)
    if quote_pos == 1:
        liq_delta = 10**quote_dec / (df["sqrt_pb"] - df["sqrt_pa"])
    else:
        liq_delta = 10**quote_dec / (1 / df["sqrt_pa"] - 1 / df["sqrt_pb"])
    df["pct_liq"] = liq_delta / df["staked_liq"].astype(float)
    df["incentive_apr_tick"] = 100 * (
        df["pct_liq"] * df["reward_rate"] / 1e18 * (365 * 24 * 60 * 60)
    )
    return df


def calc_apr_range(df, range_pct: int):
    df["incentive_apr_%s" % range_pct] = (
        df["incentive_apr_tick"]
        * (1 - df["sqrt_pa"] / df["sqrt_pb"])
        / (
            np.exp((range_pct - 1) / 200)
            - df["sqrt_pa"] / df["sqrt_pb"] * np.exp(-(range_pct - 1) / 200)
        )
    )
    return df


def add_rolling(df, periods_in_day, days=7):
    window = periods_in_day * days
    df = calc_apr_range(df, 15)
    df["incentive_apr_15_%sd_roll_mean" % days] = (
        df["incentive_apr_15"].rolling(window=window, min_periods=1).mean()
    )
    df["inv_staked_liq_%sd_roll_mean" % days] = (
        (df["staked_liq"].astype(float) ** -1)
        .rolling(window=window, min_periods=1)
        .mean()
    )
    df["pct_liq_%sd_roll_mean" % days] = (
        df["pct_liq"].rolling(window=window, min_periods=1).mean()
    )
    return df


if __name__ == "__main__":

    w3 = get_w3()

    pool_id = "0xBE00fF35AF70E8415D0eB605a286D8A45466A4c1"
    pool_id = TEST_POOL_ID
    block_end = TEST_BLOCK
    days = 7
    block_start = int(block_end - SECS_IN_DAY * days / SECS_PER_BLOCK)
    block_spacing = int(60 * SAMPLE_FRE_MIN / SECS_PER_BLOCK)

    df = get_sample_active_liq(
        w3, pool_id, MULTICALL3_ADDR, block_start, block_end, block_spacing
    )

    days = 2
    window = PERIODS_IN_DAY * days
    df = calc_apr_range(df, 15)
    df["incentive_apr_15_%sd_roll_mean" % days] = (
        df["incentive_apr_15"].rolling(window=window, min_periods=1).mean()
    )
    df["staked_liq_%sd_roll_mean" % days] = (
        df["staked_liq"].rolling(window=window, min_periods=1).mean()
    )
    df["pct_liq_%sd_roll_mean" % days] = (
        df["pct_liq"].rolling(window=window, min_periods=1).mean()
    )
    print(df.dropna())
    print(df["incentive_apr_15"].median())
