import numpy as np
from typing import Mapping, List
import pandas as pd
from .positions import (
    get_exp_with_rewards,
    get_positions_batch,
    get_pools_batch,
    get_balances,
    fetch_token_infos,
)
from .get_tokens import get_safe_tokens
from ..binance.universe import coin_to_coin
from .helpers import (
    fetch_pool_infos,
    CLMMSnapNoToken,
)
from .vars import TOKEN_WHITELIST
from ..logger import get_logger

logger = get_logger(__name__)


from concurrent.futures import ThreadPoolExecutor, as_completed


def sample_positions_threaded(
    w3,
    eoa,
    position_ids,
    sample_freq: int,
    block_start: int,
    block_end: int,
    max_workers=2,
):
    def fetch_block(block):
        exp, _ = get_exp_with_rewards(w3, eoa, position_ids, block=block)
        exp_sum = exp.sum(axis=0)
        exp_sum["block"] = block
        exp_sum["timestamp"] = w3.eth.get_block(block)["timestamp"]
        return pd.DataFrame([exp_sum])

    blocks = list(range(block_start, block_end + 1, sample_freq))
    dfs = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_block = {
            executor.submit(fetch_block, block): block for block in blocks
        }
        for future in as_completed(future_to_block):
            try:
                df = future.result()
                dfs.append(df)
            except Exception as e:
                print(f"Error at block {future_to_block[future]}: {e}")

    df_final = pd.concat(dfs, ignore_index=True)
    return df_final.sort_values("block").reset_index(drop=True)


def sample_balances(
    w3, eoa, sample_freq: int, block_start: int, block_end: int, whitelist: bool = True
):
    block = block_end
    df = pd.DataFrame()
    safe_tokens = get_safe_tokens(eoa)
    if whitelist:
        logger.warning("Using whitelist might miss some tokens ...")
        safe_tokens = {a: v for a, v in safe_tokens.items() if a in TOKEN_WHITELIST.values()}

    tokens = fetch_token_infos(w3, [el for el in safe_tokens], block_end)
    tokens = {token.address: token for token in tokens}


    while block >= block_start:
        ts = w3.eth.get_block(block)["timestamp"]
        bal = get_balances(w3, eoa, tokens, block)
        bal = {
            tokens[addr].symbol: val / 10 ** tokens[addr].decimals
            for addr, val in bal.items()
        }

        bal["block"] = block
        bal["timestamp"] = ts

        df = pd.concat([df, pd.DataFrame([bal])], ignore_index=True)
        block -= sample_freq

    df = df.sort_values("block").reset_index(drop=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    return df


def sample_positions(
    w3,
    eoa,
    position_ids,
    sample_freq: int,
    block_start: int,
    block_end: int,
    wallet: bool=True,
    whitelist: bool=True,
):
    block = block_end
    df = pd.DataFrame()
    safe_tokens = get_safe_tokens(eoa, startblock=block_start, endblock=block_end)
    if whitelist:
        logger.warning("Using whitelist, might miss some tokens")
        safe_tokens = {a: v for a,v in safe_tokens.items() if a in TOKEN_WHITELIST}
    pools_df = pd.DataFrame()

    positions = get_positions_batch(w3, position_ids, block_end)
    positions = [pos for pos in positions if pos[1] is not None]
    pool_data = [(pos[1][2], pos[1][3], pos[1][4]) for pos in positions]
    pool_addresses = get_pools_batch(w3, pool_data, block=block_end)
    pool_infos: List[CLMMSnapNoToken] = fetch_pool_infos(w3, pool_addresses, block_end)
    pool_tokens = {
        p.pool_id: {"token0": p.token0, "token1": p.token1} for p in pool_infos
    }

    while block >= block_start:
        ts = w3.eth.get_block(block)["timestamp"]
        exp, _, pool_infos = get_exp_with_rewards(
            w3,
            eoa,
            position_ids,
            block,
            safe_tokens,
            wallet=wallet,
            pool_tokens=pool_tokens,
        )  # exp: index = position_ids, columns = symbols
        pool_infos: Mapping[str, CLMMSnapNoToken] = pool_infos

        exp_sum = exp.sum(
            axis=0
        )  # Sum over position_ids, resulting in a Series with symbols as index
        exp_sum["block"] = block
        exp_sum["timestamp"] = ts

        df = pd.concat([df, pd.DataFrame([exp_sum])], ignore_index=True)
        p_df = pd.DataFrame(
            [
                {
                    "name": p.pool_id,
                    "liq": p.liquidity,
                    "sliq": p.stakedLiquidity,
                    "amt0": p.amt0,
                    "amt1": p.amt1,
                    "L": np.sqrt(float(p.amt0) * float(p.amt1)),
                    "block": p.block_number,
                    "timestamp": ts,
                }
                for p in pool_infos.values()
            ]
        )
        pools_df = pd.concat([pools_df, p_df])

        block -= sample_freq

    df = df.sort_values("block").reset_index(drop=True)
    df["timestamp"] = pd.to_datetime(pd.to_numeric(df["timestamp"]), unit="s", utc=True)
    pools_df["timestamp"] = pd.to_datetime(
        pd.to_numeric(pools_df["timestamp"]), unit="s", utc=True
    )
    return df, pools_df


def sample_positions_lookback(
    w3, eoa, position_ids, lookback_periods, sample_freq_min=30, wallet=True
):
    # blocks_in_day = 24 * 60 * 60 // 2
    block_end = w3.eth.block_number
    sample_freq = (sample_freq_min * 60) // 2
    block_start = block_end - sample_freq * lookback_periods
    df, pool_df = sample_positions(
        w3, eoa, position_ids, sample_freq, block_start, block_end, wallet=wallet
    )
    return df, pool_df


def get_notionals(df, prices):
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df["timestamp"] = df["timestamp"].dt.floor("min")
    df.set_index("timestamp", inplace=True)
    df = df.drop(["block"], axis=1)
    common_index = df.index.intersection(prices.index)
    prices = prices.loc[common_index]
    df = df.loc[common_index]
    positions = pd.DataFrame({coin_to_coin(c): df[c] for c in df.columns})
    notionals = prices * positions.reindex(columns=positions.columns)
    return notionals
