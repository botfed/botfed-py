import requests
import pandas as pd
import time
import datetime as dt
from ..universe import coin_to_binance_contract


def fetch_klines_from_timestamp(symbol, start_time: int, end_time: int, interval="1d"):
    base_url = "https://api.binance.com"
    endpoint = "/api/v3/klines"
    limit = 1000  # Binance API max per request
    all_data = []

    while True:
        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit,
            "startTime": start_time,
            "endTime": end_time,
        }

        response = requests.get(base_url + endpoint, params=params)
        data = response.json()

        if not isinstance(data, list) or not data:
            break  # Stop if no more data

        all_data.extend(data)

        # Update start_time for next batch
        start_time = data[-1][0] + 1  # Move past last timestamp
        time.sleep(0.5)  # Respect API rate limits

        # Stop if we already have enough data
        if start_time >= end_time:
            break

    # Convert to DataFrame
    df = pd.DataFrame(
        all_data,
        columns=[
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_volume",
            "trades",
            "taker_base_vol",
            "taker_quote_vol",
            "ignore",
        ],
    )

    # Convert columns to appropriate types
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")
    df["date"] = df["close_time"]
    df["date"] = pd.to_datetime(df["date"], utc=True)
    for col in [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_volume",
        "taker_base_vol",
        "taker_quote_vol",
    ]:
        df[col] = df[col].astype(float)
    df["twap"] = 0.25 * (df["open"] + df["low"] + df["high"] + df["close"])

    return df


def get_dfs(
    symbols, sdate: str = "20250101", edate: str = "20250201", interval: str = "1d"
):
    dfs = {}
    for s in symbols:
        symbol = coin_to_binance_contract(s)
        dfs[s] = fetch_klines_date_range(symbol, sdate, edate, interval=interval)
        dfs[s]["symbol"] = s
    return dfs


def fetch_klines_date_range(
    symbol,
    sdate: dt.datetime,
    edate: dt.datetime,
    interval: str = "1d",
):
    """
    Fetches historical kline (OHLCV) data from Binance for the given date range.

    :param symbol: The trading pair symbol (e.g., 'UNIETH').
    :param start_date: Start date in 'YYYYMMDD' format.
    :param end_date: End date in 'YYYYMMDD' format.
    :param interval: Kline interval (e.g., '1d' for daily, '1h' for hourly).
    :return: Pandas DataFrame with historical OHLCV data.
    """
    symbol = symbol.replace("/", "").replace("_", "")
    if "USDT" != symbol[-4:]:
        symbol = symbol + "USDT"

    # Convert date strings to timestamps in milliseconds
    # for some reason, fetches one day less than expected, so adjust sdate:
    # sdate = sdate - dt.timedelta(days=1)
    start_time = int(sdate.timestamp() * 1e3)
    end_time = int(edate.timestamp() * 1e3)

    return fetch_klines_from_timestamp(symbol, start_time, end_time, interval=interval)


def fetch_klines(symbol, interval="1d", start_years_ago=4):
    """
    Fetches at least `start_years_ago` years of historical kline (OHLCV) data from Binance.

    :param symbol: The trading pair symbol (e.g., 'UNIETH').
    :param interval: Kline interval (e.g., '1d' for daily, '1h' for hourly).
    :param start_years_ago: How many years of history to fetch.
    :return: Pandas DataFrame with historical OHLCV data.
    """
    millis_per_day = 24 * 60 * 60 * 1000
    millis_per_year = 365 * millis_per_day

    end_time = int(time.time() * 1000)  # Current time in milliseconds
    start_time = end_time - (start_years_ago * millis_per_year)  # 4 years ago
    return fetch_klines_from_timestamp(symbol, start_time, end_time, interval=interval)


def main():
    df = fetch_klines("ETHUSDT", interval="1d", start_years_ago=4)

    print(df)
    df.to_csv("./datasets/ohlcv_data.csv")


if __name__ == "__main__":
    main()
