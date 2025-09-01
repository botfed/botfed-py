import json
import numpy as np
from .v4_get_pool_state import (
    decode_tick_bitmap,
    Decimal,
    get_pool_state,
    FEE_TO_SPACING,
    get_contract,
    STATE_VIEW_ABI,
    STATE_VIEW_ADDR,
    POOL_ID,
    get_w3,
)

from ..logger import get_logger

logger = get_logger(__name__)


def get_tick_info(state_view, pool_id, tick):
    tick_info = state_view.functions.getTickInfo(pool_id, tick).call(
        block_identifier=block
    )
    liq_gross, liq_net = tick_info[0], tick_info[1]
    return liq_gross, liq_net


def get_liq_curve(w3, pool_id, range_pct=(-10, 10), block="latest"):
    state_view = get_contract(w3, STATE_VIEW_ADDR, STATE_VIEW_ABI)
    active_liquidity = state_view.functions.getLiquidity(pool_id).call(
        block_identifier=block
    )
    slot0 = get_pool_state(state_view, pool_id, block=block)
    print(slot0)
    fee = slot0[-1]
    tick_spacing = FEE_TO_SPACING[fee]
    active_tick = slot0[1]
    # each tick covers one bip
    tick_lower = active_tick + int(1e2 * range_pct[0])
    tick_upper = active_tick + int(1e2 * range_pct[1])
    word_lower = tick_lower // tick_spacing // 256
    word_upper = tick_upper // tick_spacing // 256
    initialized_ticks = []
    for word_pos in range(word_lower, word_upper + 1):
        bitmap = state_view.functions.getTickBitmap(pool_id, word_pos).call(
            block_identifier=block
        )
        initialized_ticks += decode_tick_bitmap(bitmap, word_pos, tick_spacing)
    logger.info(f"Got {len(initialized_ticks)} initialized ticks.")
    initialized_ticks = sorted(initialized_ticks)
    ticks = [
        tick for tick in initialized_ticks if tick >= tick_lower and tick <= tick_upper
    ]
    liq_adj = None
    liq_curve = []
    liquidity = 0
    for idx, tick in enumerate(ticks):
        liq_gross, liq_net = get_tick_info(state_view, pool_id, tick)
        liquidity += liq_net
        if liq_adj is None:
            next_tick = ticks[idx + 1]
            if next_tick > active_tick:
                liq_adj = active_liquidity - liquidity
        liq_curve.append((tick, liquidity, liq_gross, liq_net))
    # do adjustment
    liq_curve = [
        (tick, liq + liq_adj, liq_gross, liq_net)
        for tick, liq, liq_gross, liq_net in liq_curve
    ]

    return liq_curve, active_tick, active_liquidity


def get_liq_curve_cached(w3, pool_id, range_pct=10, block="latest"):
    if block == "latest":
        block = w3.eth.block_number
    fpath = f"./datasets/uni_{pool_id}_liq_curve_{block}_r{int(range_pct)}.json"
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
        liq_curve, active_tick, active_liq = get_liq_curve(
            w3,
            pool_id,
            block=block,
            range_pct=(-range_pct, range_pct),
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


# def liq_to_dollar(ticks, tick_spacing, liq_curve, active_tick, quote_curr_pos):
#
#     tick_lower = None
#     tick_lower_idx = None
#     for idx, tick in enumerate(ticks):
#         if tick_lower is None:
#             next_tick = ticks[idx + 1]
#             if next_tick > active_tick:
#                 tick_lower = tick
#                 tick_lower_idx = idx
#                 break
#     liq = liq_curve[tick_lower_idx]
#     p_a = 1.0001**tick_lower
#     p_b = 1.0001 ** (tick_lower + tick_spacing)
#     if quote_curr_pos == 1:
#         amt = liq * (np.sqrt(p_b) - np.sqrt(p_a))
#     else:
#         amt = liq * (1 / np.sqrt(p_a) - 1 / np.sqrt(p_b))
#     return amt




# def liq_to_quote_amt(active_tick, active_liq, quote_curr_pos):
#     """
#     Compute amount of quote token in active tick range of CLMM.
#
#     Args:
#         ticks: Array of tick boundaries.
#         tick_spacing: Tick spacing (int).
#         liq_curve: Array of liquidity at each tick.
#         active_tick: Current active tick (int).
#         quote_curr_pos: 0 if quote token is token0, 1 if token1.
#
#     Returns:
#         Amount of quote token in active range.
#     """
#
#     sqrt_p_a = 1.0001 ** ((active_tick - 1) / 2)
#     sqrt_p_b = 1.0001 ** ((active_tick + 1) / 2)
#
#     if quote_curr_pos == 1:
#         # Quote token is token1, amount1 formula:
#         amt = active_liq * (sqrt_p_b - sqrt_p_a)
#     else:
#         # Quote token is token0, amount0 formula:
#         amt = active_liq * (1 / sqrt_p_a - 1 / sqrt_p_b)
#
#     return amt


def liq_in_range(ticks, liq_curve, tick_lower, tick_upper):
    total_l = Decimal(0)

    for idx, tick in enumerate(ticks):
        if tick >= tick_lower and tick <= tick_upper:
            total_l += Decimal(int(liq_curve[idx]))
    return total_l


if __name__ == "__main__":
    import matplotlib.pyplot as plt
    import numpy as np
    import sys

    w3 = get_w3()
    block = 19787002
    lc, active_tick, active_liquidty = get_liq_curve_cached(
        w3, POOL_ID, block=block, range_pct=20
    )
    ticks, liquidity, liq_gross, liq_net = zip(*lc)

    liq_net = np.array(liq_net)
    liquidity = np.array(liquidity)

    prices = [1.0001**tick for tick in ticks]
    active_price = 1.0001**active_tick

    amt0 = liq_to_quote_amt(ticks, liquidity, active_tick) / 1e6

    dollars_per_liq = Decimal(amt0 / active_liquidty)

    # compute total tvl in range
    delta_pct = 10
    tick_lower = active_tick - int(delta_pct * 100)
    tick_upper = active_tick + int(delta_pct * 100)

    total_liq = liq_in_range(ticks, liquidity, tick_lower, tick_upper)

    tvl = dollars_per_liq * total_liq
    print(f"Total TVL in range ${tvl:,.2f} +- {delta_pct}")

    dollar_liq_active = float(active_liquidty * dollars_per_liq)
    print(f"Active TVL ${dollar_liq_active:,.2f}")

    incentives_24h = 563e3 / 14 / 2
    incentives_apr = 365 * incentives_24h / dollar_liq_active
    fees_24h = 36e3
    fees_apr = 365 * fees_24h / dollar_liq_active
    apr_active = fees_apr + incentives_apr

    tick_spacing = 10
    num_buckets = (tick_upper - tick_lower) / tick_spacing
    range_apr = apr_active / num_buckets
    range_incentives_apr = incentives_apr / num_buckets
    range_fees_apr = fees_apr / num_buckets
    print(
        f"Range APR: {100*range_apr:.2f}% Fees APR {100*range_fees_apr: .2f}% Incentives APR {100*range_incentives_apr:.2f}% Num buckets = {num_buckets}"
    )

    # idx = np.where(np.array(ticks) == active_tick)[0][0]
    # print(active_tick)
    # print(idx)
    # delta = 2
    # print(liq_net[idx - delta : idx + delta])
    # print(liquidity[idx - delta : idx + delta])
    try:
        input("Proceed w/ plot? [Enter]")
        width = (prices[-1] - prices[0]) / len(prices)
        plt.figure(figsize=(12, 6))
        plt.bar(
            prices, np.array(liquidity) * dollars_per_liq, width=width, align="edge"
        )
        plt.axvline(x=active_price, color="red", linestyle="--", label="Active Price")
        plt.title("Uniswap V4 Liquidity Distribution (Bar Graph)")
        plt.xlabel("Price")
        plt.ylabel("Liquidity")
        plt.grid(True, linestyle="--", alpha=0.5)
        plt.legend()
        plt.tight_layout()
        plt.show()
    except KeyboardInterrupt:
        sys.exit(1)
