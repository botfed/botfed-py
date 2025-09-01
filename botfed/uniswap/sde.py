import numpy as np


def calc_incentives_apr(
    entry_price,
    mu_365,
    sigma_365,
    price_lower,
    price_upper,
    time_to_expiry_days,
    incentive_apr,
    N=10**4,
    M=100,
):
    steps = int(M)
    dt = time_to_expiry_days / 365 / M
    sigma = sigma_365 * np.sqrt(dt)
    mu = mu_365 * dt - (sigma**2) / 2
    log_ret = np.random.randn(N, steps) * sigma + mu
    prices = np.exp(np.cumsum(log_ret, axis=1))
    prices = entry_price * prices / prices[:, [0]]
    in_range = (prices >= price_lower) & (prices <= price_upper)
    incentives_earned = np.sum(in_range * incentive_apr * dt, axis=1)
    incentives_apr_365 = np.mean(incentives_earned) * 365 / time_to_expiry_days
    incentives_vol_365 = np.std(incentives_earned) * np.sqrt(365 / time_to_expiry_days)
    time_inrange = in_range.mean()
    time_inrange_std = np.log(1+ in_range.mean(axis=1)).std()
    return incentives_apr_365, incentives_vol_365, time_inrange, np.exp(time_inrange_std) - 1


def pct_in_range(
    entry_price,
    mu_365,
    sigma_365,
    price_lower,
    price_upper,
    time_to_expiry_days,
    N=10**4,
    M=1000,
):
    steps = int(M)
    dt = time_to_expiry_days / 365 / M
    sigma = sigma_365 * np.sqrt(dt)
    mu = mu_365 * dt - (sigma**2) / 2
    log_ret = np.random.randn(N, steps) * sigma + mu
    prices = np.exp(np.cumsum(log_ret, axis=1))
    prices = entry_price * prices / prices[:, [0]]
    return ((prices >= price_lower) & (prices <= price_upper)).mean()


def num_shares(s):
    return s


def pct_change(arr):
    arr = np.asarray(arr)
    return (arr[1:] - arr[:-1]) / arr[:-1]


def calc_log_ret(arr):
    arr = np.asarray(arr)
    return np.log(arr[1:] / arr[:-1])


def single_path(M=100, num_years=1, sigma_365=0.1):
    T = num_years
    steps = M * T
    dt = 1 / (M * 365)
    sigma = sigma_365 * np.sqrt(dt)
    mu = -(sigma**2) / 2
    log_ret = np.random.randn(steps) * sigma + mu
    prices = np.exp(np.cumsum(log_ret))
    prices = prices / prices[0]
    shares = prices
    price_changes = prices[1:] - prices[:-1]
    pnl = np.concatenate([[0], shares[:-1] * price_changes])
    equity = np.cumsum(pnl)

    # theoretical payoff

    S_T = prices[:]
    theoretical = 0.5 * (S_T**2 - 1)
    hedging_errors = equity - theoretical

    print("Hedging error", hedging_errors[-1])
    # print(f"Mean hedging errors {np.mean(hedging_errors) / T}")
    # print(f"Stdev of errors {np.std(hedging_errors) / np.sqrt(T)}")


def many_paths(N=10**4, M=100, num_years=1, sigma_365=0.1):
    T = num_years
    steps = M * T
    dt = 1 / (M * 365)
    sigma = sigma_365 * np.sqrt(dt)
    mu = -(sigma**2) / 2
    log_ret = np.random.randn(N, steps) * sigma + mu
    prices = np.exp(np.cumsum(log_ret, axis=1))
    prices = prices / prices[:, [0]]
    shares = prices
    price_changes = prices[:, 1:] - prices[:, :-1]
    pnl = shares[:, :-1] * price_changes
    net_pnl = np.sum(pnl, axis=1)

    # theoretical payoff

    S_T = prices[:, -1]
    theoretical = 0.5 * (S_T**2 - 1)
    hedging_errors = net_pnl - theoretical

    print(f"Mean hedging errors {np.mean(hedging_errors) / T}")
    print(f"Mean theoretical pnl {np.mean(theoretical)}")
    print(f"Mean theoretical sharpe {np.mean(theoretical)/ np.std(theoretical)}")
    print(f"Mean hedge pnl {np.mean(net_pnl)}")
    print(f"Mean hedge sharpe {np.mean(net_pnl)/ np.std(net_pnl)}")
    print(f"Stdev of errors {np.std(hedging_errors) / np.sqrt(T)}")
    print(
        f"Mean error sharpe {-np.mean(hedging_errors) / np.std(hedging_errors) * np.sqrt(T)}"
    )


if __name__ == "__main__":
    # single_path(M=10)
    # single_path(M=100)
    # single_path(M=1000)
    # single_path(M=10000)
    # single_path(M=100000)
    # single_path(M=1000000)
    # single_path(M=10000000)
    many_paths(M=1000)
