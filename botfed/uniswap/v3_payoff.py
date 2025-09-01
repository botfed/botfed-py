import numpy as np


from scipy.stats import norm


def bs_put_price(S, K, T, r, sigma):
    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    return K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)


np.random.seed(42)  # or any integer


def v3_cost_replicating_portfolio(
    entry_price, price_lower, price_upper, mu_365, sigma_365, period, N=10**4, M=100
):
    steps = int(M)
    dt = period / 365 / M
    sigma = sigma_365 * np.sqrt(dt)
    mu = mu_365 * dt - (sigma**2) / 2
    log_ret = np.random.randn(N, steps) * sigma + mu
    prices = np.exp(np.cumsum(log_ret, axis=1))
    prices = entry_price * prices / prices[:, [0]]
    shares = v3_derivative_payoff_vec(prices, entry_price, price_lower, price_upper)
    price_changes = prices[:, 1:] - prices[:, :-1]
    pnl = shares[:, :-1] * price_changes
    net_pnl = np.sum(pnl, axis=1)

    S_T = prices[:, -1]
    theoretical = (
        v3_payoff_vec(S_T, entry_price, price_lower, price_upper) - entry_price
    )
    hedging_errors = theoretical - net_pnl

    # print("prices", prices[-1, :])
    # print("shares", shares[-1, :])
    # print("pnl", pnl[-1, :])
    # print(np.sum(pnl[-1:]))
    # print("theoretical", theoretical[-1])
    # print(f"Mean theoreitcal pnl", np.mean(theoretical))
    # print(f"Mean replicating pnl", np.mean(net_pnl))

    # print(f"Mean hedged {np.mean(hedging_errors)}")
    # print(f"Stdev of hedged {np.std(hedging_errors)}")
    # print(
    #     f"Sharpe theoretical",
    #     np.mean(theoretical) / np.std(theoretical),
    # )
    # print(f"Sharpe hedged", np.mean(hedging_errors) / np.std(hedging_errors))
    # return np.mean(hedging_errors)
    return (
        np.mean(hedging_errors),
        np.std(hedging_errors),
        np.mean(theoretical),
        np.std(theoretical),
    )


def v3_in_range_lp_value(p, price_lower, price_upper, L=1):
    return L * (2 * np.sqrt(p) - p / np.sqrt(price_upper) - np.sqrt(price_lower))


def v3_derivative_in_range_lp_value(p, price_upper, L=1):
    return L * (1 / np.sqrt(p) - 1 / np.sqrt(price_upper))


def v3_derivative_payoff_vec(prices, entry_price, price_lower, price_upper):
    """
    Assume 1 unit of equity (measured in terms of the quote asset) is invested at entry price

    Returns the equity at current_price for a concentrated liquidty position in the range [price_lower, price_upper]
    """
    L = 1 / v3_in_range_lp_value(entry_price, price_lower, price_upper)
    equity_lower = v3_in_range_lp_value(price_lower, price_lower, price_upper, L=L)

    payoff = np.where(
        prices <= price_lower,
        equity_lower / price_lower,
        np.where(
            prices < price_upper,
            v3_derivative_in_range_lp_value(prices, price_upper, L=L),
            0,
        ),
    )

    return payoff


def v3_payoff_vec(prices, entry_price, price_lower, price_upper):
    """
    Assume 1 unit of equity (measured in terms of the quote asset) is invested at entry price

    Returns the equity at current_price for a concentrated liquidty position in the range [price_lower, price_upper]
    """
    L = 1 / v3_in_range_lp_value(entry_price, price_lower, price_upper)
    equity_lower = v3_in_range_lp_value(price_lower, price_lower, price_upper, L=L)
    equity_upper = v3_in_range_lp_value(price_upper, price_lower, price_upper, L=L)

    payoff = np.where(
        prices <= price_lower,
        equity_lower * prices / price_lower,
        np.where(
            prices < price_upper,
            v3_in_range_lp_value(prices, price_lower, price_upper, L=L),
            equity_upper,
        ),
    )

    return payoff


def v3_payoff_vec_naive(prices, entry_price, price_lower, price_upper):
    """
    Assume 1 unit of equity (measured in terms of the quote asset) is invested at entry price

    Returns the equity at current_price for a concentrated liquidty position in the range [price_lower, price_upper]
    """
    equity_lower = np.sqrt(price_lower / entry_price)
    equity_upper = np.sqrt(price_upper / entry_price)

    payoff = np.where(
        prices <= price_lower,
        equity_lower * prices / price_lower,
        np.where(prices < price_upper, np.sqrt(prices / entry_price), equity_upper),
    )

    return payoff


def v3_put_premium(
    entry_price, price_lower, price_upper, mu_365, sigma_365, periods, N=10**6
):

    log_mu_365 = mu_365 - sigma_365**2 / 2

    mu = log_mu_365 * (periods / 365)
    sigma = sigma_365 * np.sqrt(periods / 365)

    log_returns = np.random.randn(N) * sigma + mu
    prices = np.exp(log_returns) * entry_price

    payoffs = v3_payoff_vec(prices, entry_price, price_lower, price_upper)
    # payoffs = v3_payoff_vec_naive(prices, entry_price, price_lower, price_upper)

    ret = np.mean(payoffs) - 1
    vol = np.std(payoffs)

    apr_365 = ret * 365 / periods
    ret_365 = (1 + ret) ** (365 / periods) - 1
    vol_365 = vol * np.sqrt(365 / periods)

    return apr_365, ret_365, vol_365


if __name__ == "__main__":

    half_ranges_in_vol = np.linspace(0, 5, 21)
    half_ranges_in_vol[0] = 0.1
    vol_365 = 0.7
    periods = 365
    ar_365 = 0.0
    mu_365 = np.log(1 + ar_365)

    mu_full_range_theoretical = mu_365 / 2 - vol_365**2 / 8

    for half_range_vol in half_ranges_in_vol:
        vol_period = vol_365 * np.sqrt(7 / 365)
        price_lower = np.exp(-vol_period * half_range_vol)
        price_upper = np.exp(vol_period * half_range_vol)
        range = price_upper - price_lower

        apr, ret, vol = v3_put_premium(
            1, price_lower, price_upper, mu_365, vol_365, periods
        )
        mu_365_sample = np.log(1 + ret)
        print(
            f"{half_range_vol:.2f} {100*range:.2f}% {100*apr:.2f}% {100*ret:.2f}%, {100*mu_full_range_theoretical : .2f}% {100*vol:.2f}%"
        )

    put_price = bs_put_price(1, 1, periods / 365, 0, vol_365)
    put_price_365 = (1 - put_price) ** (365 / periods) - 1
    print(f"Put price: {100*put_price_365:.2f}")
