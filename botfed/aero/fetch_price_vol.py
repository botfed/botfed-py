import pandas as pd
import numpy as np

import requests
import pandas as pd
import numpy as np
from ..coingecko.get_price_history import get_price_history
from ..binance.universe import coin_to_binance_contract
from .vars import STABLECOINS


def fetch_price_vol_gc(symbol, quote_symbol):
    df = get_price_history(symbol)

    if quote_symbol.upper() in STABLECOINS:
        df = pd.DataFrame({"price": df["price_usd"]}, index=df.index)
    else:

        df2 = get_price_history(quote_symbol)
        df = pd.DataFrame({"price": df["price_usd"] / df2["price_usd"]}, index=df.index)
    df["vol_365"] = np.log(df["price"] / df["price"].shift(1)).rolling(
        window=30
    ).std() * np.sqrt(365)
    df = df.dropna()
    return df.iloc[-1][["price", "vol_365"]]


def get_perp_price_series(asset, interval="1d", limit=365):
    """
    Fetch daily close price series for a Binance Futures perpetual contract settled in USDT.
    Assumes asset is quoted vs USDT, i.e., BTC → BTCUSDT.
    """
    symbol = coin_to_binance_contract(asset)
    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}

    resp = requests.get(url, params=params)
    resp.raise_for_status()
    data = resp.json()

    df = pd.DataFrame(
        data,
        columns=[
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_asset_volume",
            "number_of_trades",
            "taker_buy_base_volume",
            "taker_buy_quote_volume",
            "ignore",
        ],
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    df["close"] = df["close"].astype(float)
    df = df.set_index("timestamp")[["close"]]
    df.rename(columns={"close": "price_usd"}, inplace=True)
    return df


def fetch_norm_price_series(base_asset, quote_asset, interval="1d", limit=365):
    """
    Fetch daily price series for base and quote asset from Binance Futures.
    Compute base/quote price series and 1Y annualized volatility.
    """
    df_base = get_perp_price_series(base_asset, interval=interval, limit=limit)
    if quote_asset.upper() not in STABLECOINS:
        df_quote = get_perp_price_series(quote_asset, interval=interval, limit=limit)
    else:
        df_quote = pd.DataFrame(
            {"price_usd": [1.0] * len(df_base.index)}, index=df_base.index
        )

    df = pd.DataFrame(index=df_base.index)
    df["price_base_quote"] = df_base["price_usd"] / df_quote["price_usd"]
    df["price_base_usd"] = df_base["price_usd"]
    df["price_quote_usd"] = df_quote["price_usd"]
    return df


def fetch_price_vol(base_asset, quote_asset, window=30, interval='1m', limit=600):
    """
    Fetch daily price series for base and quote asset from Binance Futures.
    Compute base/quote price series and 1Y annualized volatility.
    """
    df = fetch_norm_price_series(base_asset, quote_asset, interval=interval, limit=limit)
    df["log_ret"] = np.log(df["price_base_quote"] / df["price_base_quote"].shift(1))
    df["lr2"] = df["log_ret"] ** 2
    df["rvar"] = df["lr2"].ewm(span=window, adjust=False).mean() * 365 * 24 * 60
    # df["vol_365"] = df["log_ret"].rolling(window=window).std() * np.sqrt(365)
    df['vol_365'] = np.sqrt(df['rvar'])
    df = df.dropna()

    latest_price = df.iloc[-1]["price_base_quote"]
    latest_vol = df.iloc[-1]["vol_365"]
    return latest_price, latest_vol


if __name__ == "__main__":
    result = fetch_price_vol("BRETT", "ETH")
    print(result)
