from dataclasses import dataclass
import numpy as np
from decimal import Decimal
import pandas as pd

from .v4_get_volumes import estimate_swap_vlm
from .v4_get_liq_curve import get_liq_curve_cached
from .v3_replication_cost import calc_range_payoffs
from ..uniswap.sde import calc_incentives_apr
from ..aero.helpers import liq_to_quote_amt


@dataclass
class PoolMetaInfo:
    pool_id: str
    symbol0: str
    symbol1: str
    fee_pips: int
    vlm_24h: float
    incentives_24h: float


def calc_range_aprs(
    active_tick,
    active_liquidity,
    time_to_exp,
    mu_365,
    vol_365,
    fees_24h,
    incentives_24h,
    quote_curr_pos: int,
    quote_curr_dec: int,
    base_curr_dec: int,
    tick_spacing: int,
    quote_token_price: float,
    risk_neutral: bool = True,
):

    active_liquidity = Decimal(active_liquidity)
    incentives_24h = Decimal(incentives_24h)
    fees_24h = Decimal(fees_24h)

    amt0 = liq_to_quote_amt(tick_spacing, active_liquidity, active_tick, quote_curr_pos)
    amt0_usd = float(amt0 / 10**quote_curr_dec) * quote_token_price
    dollars_per_liq = Decimal(amt0_usd) / active_liquidity

    vol_7 = vol_365 * np.sqrt(7 / 365)

    ranges = np.linspace(0.25, 3, 20) * 1e4 * vol_7

    # Floor to nearest multiple of tick_spacing and convert to integers
    ranges = ((2 * ranges) // tick_spacing) * (tick_spacing) // 2
    ranges = ranges.astype(int)

    # Remove duplicates
    ranges = np.unique(ranges)
    ranges = ranges[ranges > 0]

    results = []
    # if quote_curr_pos == 0:
    #     active_tick = -active_tick
    # tvl = dollars_per_liq * total_liq

    left_tick = (active_tick // tick_spacing) * tick_spacing

    active_price = 1.0001 ** active_tick

    for delta_bps in ranges:
        # compute total tvl in range
        delta_bps = int(delta_bps)
        tick_lower = left_tick - delta_bps
        tick_upper = tick_lower + 2 * delta_bps

        sqrt_pa = 1.0001 ** (tick_lower / 2)
        sqrt_pb = 1.0001 ** (tick_upper / 2)

        if quote_curr_pos == 1:
            liq_delta = float(10**quote_curr_dec / (sqrt_pb - sqrt_pa)) / quote_token_price
        else:
            liq_delta = float(10**quote_curr_dec / (1 / sqrt_pa - 1 / sqrt_pb)) / quote_token_price
        pct_liq = Decimal(liq_delta) / active_liquidity
        incentives_apr_365 = float(365 * incentives_24h * pct_liq)


        (
            hedging_apr,
            min_incentive_apr,
            time_in_range,
            _,
            hedged_vol,
            _,
            unhedged_vol,
        ) = calc_range_payoffs(
            1,
            sqrt_pa ** 2 / active_price,
            sqrt_pb ** 2 / active_price,
            time_to_exp, mu_365, vol_365, risk_neutral=risk_neutral
        )
        earned_incentives_apr_365, incentives_vol_365, time_inrange, time_inrange_std = calc_incentives_apr(
            1,
            mu_365,
            vol_365,
            sqrt_pa ** 2 / active_price,
            sqrt_pb ** 2 / active_price,
            time_to_exp,
            incentives_apr_365,
        )

        # total_liq = liq_in_range(ticks, liquidity, tick_lower, tick_upper)

        # print(liq_delta, pct_liq)

        dollar_liq_active = float(active_liquidity * dollars_per_liq)

        # incentives_apr = float(365 * incentives_24h * pct_liq) * time_in_range
        # print('inrange vs earned', incentives_apr, earned_incentives_apr_365)
        # print(incentives_apr, incentives_apr_365)
        fees_apr = float(365 * fees_24h * pct_liq) * time_in_range
        range_apr = float(fees_apr + earned_incentives_apr_365)
        net_apr = range_apr + hedging_apr

        price_lower = (1.0001 ** tick_lower) ** (2 * quote_curr_pos - 1)
        price_upper = (1.0001 ** tick_upper) ** (2 * quote_curr_pos - 1)
        if quote_curr_pos == 0:
            price_lower, price_upper = price_upper, price_lower 
        price_lower = price_lower * 10 ** base_curr_dec / 10 ** quote_curr_dec
        price_upper = price_upper * 10 ** base_curr_dec / 10 ** quote_curr_dec

        results.append(
            {
                "band_pct": delta_bps / 100,
                "band_sigma_7": round(delta_bps / vol_7 / 10000, 2),
                "time_in_range": time_in_range,
                "time_in_range_std": time_inrange_std,
                "net_apr": 100 * (range_apr + hedging_apr),
                "range_apr": range_apr * 100,
                "hedge_apr": -hedging_apr * 100,
                "min_required_apr": min_incentive_apr * 100,
                "price_lower": price_lower,
                "price_upper": price_upper,
                "tick_active": active_tick,
                "tick_lower": tick_lower,
                "tick_upper": tick_upper,
                "staked_liquidity": active_liquidity,
                "range_incentives_apr": earned_incentives_apr_365 * 100,
                "incentives_apr": incentives_apr_365 * 100,
                "range_fees_apr": fees_apr * 100,
                "fees_daily": float(fees_24h),
                "incentives_daily": float(incentives_24h),
                "dollar_liq_active": dollar_liq_active,
                "pct_liq_dollar": pct_liq,
                "hedged_vol": 100 * hedged_vol,
                "unhedged_vol": 100 * unhedged_vol,
                "incentives_vol": 100 * incentives_vol_365,
                "sharpe_unhedged": net_apr
                / np.sqrt(unhedged_vol**2 + incentives_vol_365**2),
                "sharpe_hedged": net_apr
                / np.sqrt(hedged_vol**2 + incentives_vol_365**2),
            }
        )

    return pd.DataFrame(results)


if __name__ == "__main__":

    from .v4_get_pool_state import POOL_ID, get_w3

    vol_365 = 0.5
    mu_365 = 0.35
    time_to_exp = 7
    block = 19787002
    risk_neutral = True
    max_range_pct = 20

    w3 = get_w3()
    _, vlm_daily, _ = estimate_swap_vlm(w3, POOL_ID, block)

    incentives_24h = 550e3 / 14

    pool_info = PoolMetaInfo(POOL_ID, "ETH", "USDC", 500, vlm_daily, incentives_24h)

    lc, active_tick, active_liquidity = get_liq_curve_cached(
        w3, pool_info.pool_id, block=block, range_pct=max_range_pct
    )

    df = calc_range_aprs(
        pool_info,
        lc,
        active_tick,
        active_liquidity,
        time_to_exp,
        mu_365,
        vol_365,
        risk_neutral=risk_neutral,
        max_range_pct=max_range_pct,
    )
    print(df)
