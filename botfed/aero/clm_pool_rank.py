from typing import List
import pandas as pd
from web3 import Web3
import traceback
from ..uniswap.v4_eval_pool import calc_range_aprs
from .clm_get_volumes import estimate_swap_vlm
from .clm_active_liq_history import get_sample_active_liq, add_rolling
from .vars import get_w3, MULTICALL3_ADDR, SECS_IN_DAY, SECS_PER_BLOCK, SECS_IN_MIN
from .helpers import fetch_info_from_pool_id, get_quote_token
from .fetch_price_vol import fetch_price_vol
from ..logger import get_logger

logger = get_logger(__name__)


def eval_pool(
    w3,
    pool_id,
    block_start,
    block_end,
    sample_freq,
    periods_in_day,
    days=7,
    fee_pct: float = 0,
):
    pool_info = fetch_info_from_pool_id(w3, pool_id, block_end)
    quote_token, base_token, quote_pos = get_quote_token(
        pool_info.token0.symbol, pool_info.token1.symbol
    )
    quote_token_dec = (
        pool_info.token0.decimals if quote_pos == 0 else pool_info.token1.decimals
    )
    base_token_dec = (
        pool_info.token1.decimals if quote_pos == 0 else pool_info.token0.decimals
    )
    df = get_sample_active_liq(
        w3,
        pool_id,
        MULTICALL3_ADDR,
        block_start,
        block_end,
        sample_freq,
        quote_pos,
        quote_token_dec,
    )
    df = add_rolling(df, periods_in_day, days=days)

    active_tick, inv_active_liq = df.iloc[-1][
        ["active_tick", "inv_staked_liq_%sd_roll_mean" % days]
    ]
    quote_token_price, _ = (
        fetch_price_vol(quote_token, "USDC")
        if quote_token not in ["USDC", "USDT"]
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
    _, vlm_24h, _ = estimate_swap_vlm(
        w3, pool_id, block_end, quote_pos, quote_token_dec
    )
    mu_365 = 0  # 0.35
    time_to_exp = 7
    reward_token_price, _ = fetch_price_vol("AERO", "USDC")
    reward_token_dec = 18
    fee = df["fee"].mean()
    fees_24h = vlm_24h * fee / 1e6 * fee_pct
    incentives_24 = (
        pool_info.rewardRate * SECS_IN_DAY * reward_token_price / 10**reward_token_dec
    ) / quote_token_price
    results = calc_range_aprs(
        active_tick,
        1 / inv_active_liq,
        time_to_exp,
        mu_365,
        vol_365,
        fees_24h,
        incentives_24,
        quote_pos,
        quote_token_dec,
        base_token_dec,
        pool_info.tickSpacing,
    )
    df = pd.DataFrame(results)
    df["pool_name"] = pool_info.name()
    df["vol_365"] = vol_365
    return df


def do_pool_rank(w3: Web3, pool_ids: List[str], block_end: int, sample_days=1):
    from concurrent.futures import ThreadPoolExecutor, as_completed

    w3 = get_w3()

    days = sample_days
    block_start = block_end - int(days * SECS_IN_DAY / SECS_PER_BLOCK)

    sample_freq_min = 30
    sample_freq = sample_freq_min * SECS_IN_MIN / SECS_PER_BLOCK
    periods_in_day = int(24 * 60 / sample_freq_min)

    df_all = pd.DataFrame()

    # for pool_id in pool_ids:
    #     df = eval_pool(w3, pool_id, block_start, block_end, sample_freq, periods_in_day)
    #     df_all = pd.concat([df_all, df])
    #     break

    def task(pool_id):
        return eval_pool(
            w3, pool_id, block_start, block_end, sample_freq, periods_in_day, sample_days
        )

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

    return df_all


if __name__ == "__main__":
    from .vars import watch_pools as pool_ids

    CHUNK_SIZE = 10_000
    BLOCKS_IN_DAY = int(SECS_IN_DAY // 2)

    w3 = get_w3()
    block_end = w3.eth.block_number
    block_end = CHUNK_SIZE * (block_end // CHUNK_SIZE)
    # block_end = block_end - block_end % BLOCKS_IN_DAY
    # block_end = block_end - BLOCKS_IN_DAY
    # block_end = BLOCKS_IN_DAY * (block_end // BLOCKS_IN_DAY)
    # block_end = 32006580
    # block_end = 32122122
    # block_end = 32180682  # June 28 550PM
    # block_end = 32297266  # July 1 10AM
    df_all = do_pool_rank(w3, pool_ids, block_end)

    print(df_all)
    sort_by = "sharpe_unhedged"
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
