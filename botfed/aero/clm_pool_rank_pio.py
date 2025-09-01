from typing import List
import pandas as pd
from web3 import Web3
import traceback
from ..uniswap.v4_eval_pool import calc_range_aprs
from .clm_active_liq_history import (
    sample_active_liq_pio,
)
from .vars import get_w3, MULTICALL3_ADDR, SECS_IN_DAY, STABLECOINS
from .helpers import fetch_info_from_pool_id, get_quote_token
from .fetch_price_vol import fetch_price_vol
from ..logger import get_logger

logger = get_logger(__name__)


def eval_pool_pio(w3, pool_id: str, block: int, time_to_exp: int = 14):
    pool_info = fetch_info_from_pool_id(w3, pool_id, block)
    quote_token, base_token, quote_pos = get_quote_token(
        pool_info.token0.symbol, pool_info.token1.symbol
    )
    quote_token_dec = (
        pool_info.token0.decimals if quote_pos == 0 else pool_info.token1.decimals
    )
    base_token_dec = (
        pool_info.token1.decimals if quote_pos == 0 else pool_info.token0.decimals
    )
    liq = sample_active_liq_pio(w3, pool_id, MULTICALL3_ADDR, block)
    df = pd.DataFrame([liq])

    quote_token_price, _ = (
        fetch_price_vol(quote_token, "USDC")
        if quote_token.upper() not in STABLECOINS
        else (1, 0)
    )
    try:
        _, vol_365 = fetch_price_vol(base_token, quote_token)
    except Exception as e:
        DEFAULT_VOL = 2
        logger.error(
            f"Error fetching price_vol for {base_token}, assigning default vol of {DEFAULT_VOL} msg= {e}"
        )
        # some tokens don't exist in our db, probably because they are too janky,
        # so just assign a high default vol
        vol_365 = DEFAULT_VOL
    mu_365 = 0  # 0.35
    reward_token_price, _ = fetch_price_vol("AERO", "USDC")
    reward_token_dec = 18
    incentives_24 = (
        pool_info.rewardRate * SECS_IN_DAY * reward_token_price / 10**reward_token_dec
    )
    results = calc_range_aprs(
        liq["active_tick"],
        liq["active_liquidity"],
        time_to_exp,
        mu_365,
        vol_365,
        0,
        incentives_24,
        quote_pos,
        quote_token_dec,
        base_token_dec,
        pool_info.tickSpacing,
        quote_token_price,
    )
    df = pd.DataFrame(results)
    df["pool_name"] = pool_info.name()
    df["vol_365"] = vol_365
    return df


def do_pool_rank(w3: Web3, pool_ids: List[str], block: int):
    from concurrent.futures import ThreadPoolExecutor, as_completed

    w3 = get_w3()

    df_all = pd.DataFrame()

    # for pool_id in pool_ids:
    #     df = eval_pool(w3, pool_id, block_start, block, sample_freq, periods_in_day)
    #     df_all = pd.concat([df_all, df])
    #     break

    def task(pool_id):
        return eval_pool_pio(w3, pool_id, block)

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(task, pool_id): pool_id for pool_id in pool_ids}

        for future in as_completed(futures):
            pool_id = futures[future]
            try:
                df = future.result()
                df_all = pd.concat([df_all, df])
            except Exception as e:
                traceback.print_exc()
                print(f"Error processing {pool_id}: {e}")

    df_all["net_apr_safety"] = df_all["range_apr"] / df_all["hedge_apr"]
    return df_all


if __name__ == "__main__":
    from .vars import watch_pools as pool_ids

    CHUNK_SIZE = 10_000
    BLOCKS_IN_DAY = int(SECS_IN_DAY // 2)

    w3 = get_w3()
    block = w3.eth.block_number
    block = CHUNK_SIZE * (block // CHUNK_SIZE)
    # block_end = block_end - block_end % BLOCKS_IN_DAY
    # block_end = block_end - BLOCKS_IN_DAY
    # block_end = BLOCKS_IN_DAY * (block_end // BLOCKS_IN_DAY)
    # block_end = 32006580
    # block_end = 32122122
    # block_end = 32180682  # June 28 550PM
    # block_end = 32297266  # July 1 10AM
    df_all = do_pool_rank(w3, pool_ids, block)
    df_all.to_csv("../data/pools.csv")

    sort_by = "sharpe_unhedged"
    sort_by = "sharpe_hedged"
    sort_by = "net_apr"

    df_all_100 = (
        df_all[df_all["band_sigma_7"] >= 0.9]
        .sort_values(sort_by, ascending=False)
        .groupby("pool_name", as_index=False)
        .first()
    ).sort_values(by=sort_by, ascending=False)
    df_all_20 = (
        df_all[df_all["band_sigma_7"] < 0.9]
        .sort_values(sort_by, ascending=False)
        .groupby("pool_name", as_index=False)
        .first()
    ).sort_values(by=sort_by, ascending=False)
    print(df_all_100)
    print(df_all_20)
    filt = (
        (df_all["sharpe_hedged"] >= 2)
        & (df_all["net_apr"] >= 15)
        & (df_all["time_in_range"] >= 0.66)
        & (df_all["net_apr_safety"] >= 1.4)
        & (df_all["incentives_daily"] >= 1e3)
    )
    df_filt = (
        df_all[filt]
        .sort_values("net_apr", ascending=False)
        .groupby("pool_name", as_index=False)
        .first()
        .sort_values("net_apr", ascending=False)
    )
    print(df_filt)
