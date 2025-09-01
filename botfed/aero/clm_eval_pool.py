import json
from web3 import Web3
from ..logger import get_logger
from .abis.pool_clmm import ABI as POOL_ABI
from .abis.multicall3 import ABI as MULTICALL3_ABI
from .vars import TEST_POOL_ID, get_w3, TEST_BLOCK, MULTICALL3_ADDR

from .clm_get_volumes import estimate_swap_vlm
from ..uniswap.v4_eval_pool import calc_range_aprs
from .helpers import fetch_info_from_pool_id, CLMMSnap, get_quote_token
from .fetch_price_vol import fetch_price_vol


logger = get_logger(__name__)

w3 = get_w3()
codec = w3.codec


def decode_tick_bitmap(bitmap_int, word_pos, tick_spacing):
    initialized_ticks = []
    for bit_index in range(256):
        if (bitmap_int >> bit_index) & 1:
            tick_idx = (word_pos * 256 + bit_index) * tick_spacing
            initialized_ticks.append(tick_idx)
    return initialized_ticks


def get_incentives(w3: Web3, pool_id: str, block: int):
    pool = w3.eth.contract(address=pool_id, abi=POOL_ABI)
    rewards_per_second = pool.functions.rewardRate().call(block_identifier=block)
    rewards_per_day = 24 * 60 * 60 * rewards_per_second / 1e18
    return rewards_per_day


def fetch_pool_meta(multicall, pool_contract, block):
    calls = [
        pool_contract.functions.liquidity()._encode_transaction_data(),
        pool_contract.functions.slot0()._encode_transaction_data(),
        pool_contract.functions.tickSpacing()._encode_transaction_data(),
    ]
    aggregate_call = multicall.functions.aggregate(
        [(pool_contract.address, call) for call in calls]
    )
    _, return_data = aggregate_call.call(block_identifier=block)
    # Decode stakedLiquidity (returns uint128)
    liq = codec.decode(["uint128"], return_data[0])[0]
    slot0 = codec.decode(
        ["uint160", "int24", "uint16", "uint16", "uint16", "bool"], return_data[1]
    )
    tick_spacing = codec.decode(["int24"], return_data[2])[0]

    return liq, slot0, tick_spacing


def fetch_tick_bitmaps(words, multicall, pool_contract, block):
    calls = [
        pool_contract.functions.tickBitmap(word_pos)._encode_transaction_data()
        for word_pos in words
    ]
    aggregate_call = multicall.functions.aggregate(
        [(pool_contract.address, call) for call in calls]
    )
    _, return_data = aggregate_call.call(block_identifier=block)
    # Decode stakedLiquidity (returns uint128)
    bitmaps = [
        (words[i], codec.decode(["uint256"], return_data[i])[0])
        for i in range(len(words))
    ]
    return bitmaps


def fetch_ticks(ticks, multicall, pool_contract, block):
    calls = [
        pool_contract.functions.ticks(tick)._encode_transaction_data() for tick in ticks
    ]
    aggregate_call = multicall.functions.aggregate(
        [(pool_contract.address, call) for call in calls]
    )
    _, return_data = aggregate_call.call(block_identifier=block)
    # Decode stakedLiquidity (returns uint128)
    result = [
        codec.decode(
            [
                "uint128",
                "int128",
                "int128",
                "uint256",
                "uint256",
                "uint256",
                "int56",
                "uint160",
                "uint32",
                "bool",
            ],
            return_data[i],
        )
        for i in range(len(ticks))
    ]
    return result


def get_liq_curve_multicall(w3: Web3, pool_id: str, range_pct=(-1, 1), block="latest"):
    pool = w3.eth.contract(address=pool_id, abi=POOL_ABI)
    multicall = w3.eth.contract(address=MULTICALL3_ADDR, abi=MULTICALL3_ABI)
    active_liquidity, slot0, tick_spacing = fetch_pool_meta(multicall, pool, block)
    active_tick = slot0[1]
    # each tick covers one bip
    tick_lower = active_tick + int(1e2 * range_pct[0])
    tick_upper = active_tick + int(1e2 * range_pct[1])
    word_lower = tick_lower // tick_spacing // 256
    word_upper = tick_upper // tick_spacing // 256
    initialized_ticks = []
    words = [w for w in range(word_lower, word_upper + 1)]
    bitmaps = fetch_tick_bitmaps(words, multicall, pool, block)
    for word_pos, bitmap in bitmaps:
        initialized_ticks += decode_tick_bitmap(bitmap, word_pos, tick_spacing)
    initialized_ticks = sorted(initialized_ticks)
    logger.info(f"Got {len(initialized_ticks)} initialized ticks.")
    ticks = [
        tick for tick in initialized_ticks if tick >= tick_lower and tick <= tick_upper
    ]
    liq_adj = None
    liq_curve = []
    liquidity = 0
    tick_infos = fetch_ticks(ticks, multicall, pool, block)
    for idx, tick_info in enumerate(tick_infos):
        liq_gross, liq_net = tick_info[0:2]
        liquidity += liq_net
        if liq_adj is None:
            next_tick = ticks[idx + 1]
            if next_tick > active_tick:
                liq_adj = active_liquidity - liquidity
        liq_curve.append((ticks[idx], liquidity, liq_gross, liq_net))
    # do adjustment
    liq_curve = [
        (tick, liq + liq_adj, liq_gross, liq_net)
        for tick, liq, liq_gross, liq_net in liq_curve
    ]

    return liq_curve, active_tick, active_liquidity


def get_liq_curve_cached(w3, pool_id, range_pct=(-10, 10), block="latest"):
    if block == "latest":
        block = w3.eth.block_number
    fpath = f"./datasets/aero_{pool_id}_liq_curve_{block}_r{int(range_pct[1])}.json"
    logger.info(f"Getting liq curve {fpath}")
    try:
        data = json.load(open(fpath, "r"))
        liq_curve, active_tick, active_liq = (
            data["liq_curve"],
            data["active_tick"],
            data["active_liq"],
        )
    except Exception as e:
        print(e)
        liq_curve, active_tick, active_liq = get_liq_curve_multicall(
            w3,
            pool_id,
            block=block,
            range_pct=range_pct,
        )
        json.dump(
            {
                "liq_curve": liq_curve,
                "active_tick": active_tick,
                "active_liq": active_liq,
            },
            open(fpath, "w"),
        )

    return liq_curve, active_tick, active_liq


def eval_pool(
    w3: Web3,
    pool_addr: str,
    block: int,
    max_range_pct: int = 20,
    risk_neutral: bool = True,
    mu_365: float = 0,
    fee_pct: float = 0.5,
):
    pool_info: CLMMSnap = fetch_info_from_pool_id(w3, pool_addr, block_number=block)
    pool = w3.eth.contract(address=pool_addr, abi=POOL_ABI)
    token0 = pool_info.token0
    token1 = pool_info.token1
    symbol0, dec0 = token0.symbol, token0.decimals
    symbol1, dec1 = token1.symbol, token1.decimals

    quote_curr = None
    quote_curr_pos = None
    quote_curr, base_curr, quote_curr_pos = get_quote_token(symbol0, symbol1)
    assert quote_curr is not None and base_curr is not None
    quote_curr_dec = dec1 if quote_curr == symbol1 else dec0
    logger.info(pool_info)
    logger.info(
        f"QuoteCurr={quote_curr}, Symbol0={symbol0}, Symbol1={symbol1} QuoteCurrPos={quote_curr_pos}"
    )
    _, vol_365 = fetch_price_vol(base_curr, quote_curr)
    reward_price, _ = fetch_price_vol("AERO", "USDC")
    logger.info(f"Vol={100*vol_365:.2f}% Reward token price={reward_price:0.4f}")

    incentives_24h = get_incentives(w3, pool_addr, block) * reward_price
    _, vlm_daily, _ = estimate_swap_vlm(
        w3, pool_addr, block, quote_curr_pos, quote_curr_dec
    )
    lc, active_tick, active_liquidity = get_liq_curve_cached(
        w3, pool_addr, block=block, range_pct=(-max_range_pct, max_range_pct)
    )

    staked_liquidity = pool.functions.stakedLiquidity().call(block_identifier=block)
    multiplier = active_liquidity / staked_liquidity
    # print("multiplier", multiplier)

    # print("offical active liq", pool.functions.liquidity().call(block_identifier=block))
    fees_24h = fee_pct * (vlm_daily * pool_info.fee) / 1e6  # fee is in pips
    print("Pool fee", pool_info.fee)

    dfs = {}
    time_to_exps = [7]
    for time_to_exp in time_to_exps:
        dfs[time_to_exp] = calc_range_aprs(
            active_tick,
            active_liquidity,
            time_to_exp,
            mu_365,
            vol_365,
            fees_24h,
            incentives_24h,
            quote_curr_pos,
            quote_curr_dec,
            pool_info.tickSpacing,
            risk_neutral=risk_neutral,
        )
        dfs[time_to_exp]["time_to_exp"] = time_to_exp
    return dfs


if __name__ == "__main__":

    w3 = get_w3()

    pool_id = "0xBE00fF35AF70E8415D0eB605a286D8A45466A4c1"  # AERO/USDC
    pool_id = "0x4e962BB3889Bf030368F56810A9c96B83CB3E778"  # cbBTC/USDC
    pool_id = "0x70aCDF2Ad0bf2402C957154f944c19Ef4e1cbAE1"  # cbBTC/WETH
    pool_id = TEST_POOL_ID
    block = TEST_BLOCK
    block = w3.eth.block_number
    # block = 32002409
    block = 32006580

    dfs = eval_pool(w3, pool_id, block)
    print(dfs[7])
