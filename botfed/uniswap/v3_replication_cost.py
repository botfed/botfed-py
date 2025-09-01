import numpy as np
from .v3_payoff import (
    v3_cost_replicating_portfolio,
    bs_put_price,
)
from .sde import pct_in_range


def calc_range_payoffs(entry_price, price_lower, price_upper, time_to_expiry_days, mu_365, vol_365, risk_neutral):

    T = time_to_expiry_days / 365

    hedged_payoff, hedged_std, unhedged_payoff, unhedged_std = v3_cost_replicating_portfolio(
        1, price_lower, price_upper, mu_365, vol_365, time_to_expiry_days, M=10
    )

    payoff = hedged_payoff if risk_neutral else unhedged_payoff

    dw = np.sqrt(time_to_expiry_days / 365)
    apr_hedging = payoff / dw ** 2
    hedged_vol = hedged_std / dw
    unhedged_vol = unhedged_std / dw

    time_in_range = pct_in_range(
        entry_price, mu_365, vol_365, price_lower, price_upper, time_to_expiry_days
    )

    min_incentive_apr = -apr_hedging / time_in_range

    return (
        apr_hedging,
        min_incentive_apr,
        time_in_range,
        -hedged_payoff,
        hedged_vol,
        -unhedged_payoff,
        unhedged_vol
    )


if __name__ == "__main__":
    vol_365 = 0.7
    time_to_exp = 7
    band_pct = 1
    payoff_apr, min_incentive_apr, time_in_range, hedged_payoff = calc_range_payoffs(
        time_to_exp, vol_365, band_pct
    )
    T = time_to_exp / 365
    put_price = bs_put_price(1, 1, T, 0, vol_365)
    vol_period = vol_365 * np.sqrt(time_to_exp / 365)
    print(f"Put price: {put_price:.4f} vs Hedging cost {hedged_payoff:.4f}")
    print(
        f"Time in range {100*time_in_range:.2f}% for Range / Sigma : {2*band_pct / vol_period / 100: .2f} and Range = {2*band_pct: .2f}%"
    )
    print(f"Min Reward/Fee APR needed: {100*min_incentive_apr:.2f} % ")
