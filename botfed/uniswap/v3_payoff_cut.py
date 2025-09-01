import numpy as np

from .v3_payoff import (
    v3_in_range_lp_value,
    v3_payoff_vec,
    v3_cost_replicating_portfolio,
)


def v3_abrupt_hedged(
    entry_price, price_lower, price_upper, mu_365, sigma_365, period, N=10**4, M=100
):
    steps = int(M)
    dt = period / 365 / M
    sigma = sigma_365 * np.sqrt(dt)
    mu = mu_365 * dt - (sigma**2) / 2
    log_ret = np.random.randn(N, steps) * sigma + mu
    prices = np.exp(np.cumsum(log_ret, axis=1))
    prices = entry_price * prices / prices[:, [0]]
    L = 1 / v3_in_range_lp_value(entry_price, price_lower, price_upper)
    equity_lower = v3_in_range_lp_value(price_lower, price_lower, price_upper, L=L)
    # when the previously observed price is below, enter the hedged position, otherwise
    # we are out
    shares = np.concatenate(
        [
            np.zeros((N, 1)),
            np.where(prices[:, :-1] < price_lower, equity_lower / price_lower, 0),
        ],
        axis=1,
    )
    shares = np.where(prices < price_lower, equity_lower / price_lower, 0)
    price_changes = prices[:, 1:] - prices[:, :-1]
    pnl = shares[:, :-1] * price_changes
    net_pnl = np.sum(pnl, axis=1)

    S_T = prices[:, -1]
    theoretical = (
        v3_payoff_vec(S_T, entry_price, price_lower, price_upper) - entry_price
    )
    lp_values = (
        v3_payoff_vec(prices, entry_price, price_lower, price_upper) - entry_price
    )
    lp_pnl = lp_values[:, 1:] - lp_values[:, :-1]
    hedging_errors = theoretical - net_pnl
    i = 10
    lp_pnl_synthetic_cut = lp_pnl - pnl
    lp_pnl_synthetic_cut[np.abs(lp_pnl_synthetic_cut) < 1e-10]  = 0
    lp_values_cut = (
        v3_payoff_cut_vec(prices, entry_price, price_lower, price_upper) - entry_price
    )
    lp_pnl_cut = lp_values_cut[:, 1:] - lp_values_cut[:, :-1]

    delta = lp_pnl_synthetic_cut - lp_pnl_cut
    print(prices[i, :])
    print(lp_pnl_cut[i,:])
    print(lp_pnl_synthetic_cut[i,:])
    print(delta[i, :])
    print(lp_pnl.shape, lp_pnl_synthetic_cut.shape)

    delta = np.cumsum(delta, axis=1)
    print(delta[i])

    print(np.mean(lp_pnl_cut)/dt, np.mean(lp_pnl_synthetic_cut)/dt)

    return (
        np.mean(hedging_errors),
        np.std(hedging_errors),
        np.mean(theoretical),
        np.std(theoretical),
    )


def v3_payoff_cut_vec(prices, entry_price, price_lower, price_upper):
    """
    Assume 1 unit of equity (measured in terms of the quote asset) is invested at entry price

    Returns the equity at current_price for a concentrated liquidty position in the range [price_lower, price_upper]
    """
    L = 1 / v3_in_range_lp_value(entry_price, price_lower, price_upper)
    equity_lower = v3_in_range_lp_value(price_lower, price_lower, price_upper, L=L)
    equity_upper = v3_in_range_lp_value(price_upper, price_lower, price_upper, L=L)

    payoff = np.where(
        prices <= price_lower,
        equity_lower,
        np.where(
            prices < price_upper,
            v3_in_range_lp_value(prices, price_lower, price_upper, L=L),
            equity_upper,
        ),
    )

    return payoff


def v3_payoff_uncut(
    entry_price, price_lower, price_upper, mu_365, sigma_365, period, N=10**4, M=100
):
    steps = int(M)
    dt = period / 365 / M
    sigma = sigma_365 * np.sqrt(dt)
    mu = mu_365 * dt - (sigma**2) / 2
    log_ret = np.random.randn(N, steps) * sigma + mu
    prices = np.exp(np.cumsum(log_ret, axis=1))
    prices = entry_price * prices / prices[:, [0]]

    S_T = prices[:, -1]
    theoretical = (
        v3_payoff_vec(S_T, entry_price, price_lower, price_upper) - entry_price
    )

    return (
        np.mean(theoretical),
        np.std(theoretical),
    )


def v3_payoff_cut(
    entry_price, price_lower, price_upper, mu_365, sigma_365, period, N=10**4, M=100
):
    steps = int(M)
    dt = period / 365 / M
    sigma = sigma_365 * np.sqrt(dt)
    mu = mu_365 * dt - (sigma**2) / 2
    log_ret = np.random.randn(N, steps) * sigma + mu
    prices = np.exp(np.cumsum(log_ret, axis=1))
    prices = entry_price * prices / prices[:, [0]]

    S_T = prices[:, -1]
    theoretical = (
        v3_payoff_cut_vec(S_T, entry_price, price_lower, price_upper) - entry_price
    )

    return (
        np.mean(theoretical),
        np.std(theoretical),
    )


if __name__ == "__main__":
    sigma_365 = 0.68
    mu_365 = 0
    entry_price = 1
    price_lower = 0.9
    price_upper = 1.1
    period = 7

    ret, vol = v3_payoff_cut(
        entry_price, price_lower, price_upper, mu_365, sigma_365, period
    )

    ret_365 = ret * 365 / period
    vol_365 = vol * np.sqrt(365 / period)

    print("Cut")
    print(
        f"APR {100 * ret_365: .2f}%, VOL {100*vol_365:.2f}%, Sharpe {ret_365/vol_365}"
    )
    ret, vol = v3_payoff_uncut(
        entry_price, price_lower, price_upper, mu_365, sigma_365, period
    )

    ret_365 = ret * 365 / period
    vol_365 = vol * np.sqrt(365 / period)
    print("Uncut")
    print(
        f"APR {100 * ret_365: .2f}%, VOL {100*vol_365:.2f}%, Sharpe {ret_365/vol_365}"
    )

    ret, vol, _, _ = v3_cost_replicating_portfolio(
        entry_price, price_lower, price_upper, mu_365, sigma_365, period
    )

    ret_365 = ret * 365 / period
    vol_365 = vol * np.sqrt(365 / period)
    print("Uncut")
    print(
        f"APR {100 * ret_365: .2f}%, VOL {100*vol_365:.2f}%, Sharpe {ret_365/vol_365}"
    )

    ret, vol, _, _ = v3_abrupt_hedged(
        entry_price, price_lower, price_upper, mu_365, sigma_365, period
    )

    ret_365 = ret * 365 / period
    vol_365 = vol * np.sqrt(365 / period)
    print("Abrupt Hedge")
    print(
        f"APR {100 * ret_365: .2f}%, VOL {100*vol_365:.2f}%, Sharpe {ret_365/vol_365}"
    )
