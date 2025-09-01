from .fetch_klines import fetch_klines


def fetch_klines_curr(symbol, curr="ETHUSDT", interval="1d", start_years_ago=4):
    """
    Fetches OHLCV data for `symbol` and `curr` and computes the OHLC of `symbol` in terms of `curr`.

    :param symbol: The trading pair symbol (e.g., 'UNIUSDT').
    :param curr: The currency pair for conversion (e.g., 'ETHUSDT').
    :param interval: Kline interval (e.g., '1d' for daily, '1h' for hourly).
    :param start_years_ago: How many years of history to fetch.
    :return: Pandas DataFrame with OHLCV data in terms of `curr` (e.g., UNIETH).
    """
    df_symbol = fetch_klines(symbol, interval=interval, start_years_ago=start_years_ago)
    df_curr = fetch_klines(curr, interval=interval, start_years_ago=start_years_ago)

    # Ensure timestamps align
    df_symbol.set_index("open_time", inplace=True)
    df_curr.set_index("open_time", inplace=True)

    # Join the data on timestamps
    df = df_symbol.join(
        df_curr[["open", "high", "low", "close"]],
        lsuffix="_symbol",
        rsuffix="_curr",
        how="inner",
    )

    # Convert OHLC to `curr` base
    df["open"] = df["open_symbol"] / df["open_curr"]
    df["high"] = df["high_symbol"] / df["high_curr"]
    df["low"] = df["low_symbol"] / df["low_curr"]
    df["close"] = df["close_symbol"] / df["close_curr"]
    df['twap'] = (df['close'] + df['open'] + df['high'] + df['low']) / 4.0
    df["quote_curr"] = curr

    # Keep other columns from the original `symbol` dataset
    df = df[
        [
            'quote_curr',
            'twap',
            "open",
            "high",
            "low",
            "close",
            "volume",
            "quote_volume",
            "trades",
            "taker_base_vol",
            "taker_quote_vol",
        ]
    ]

    df.index.name = 'date'

    return df


def main():
    df = fetch_klines_curr("UNIUSDT", "ETHUSDT", interval="1d", start_years_ago=4)

    print(df)


if __name__ == "__main__":
    main()
