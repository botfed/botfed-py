import time
import logging
import pandas as pd
import datetime as dt
from dateutil.relativedelta import relativedelta
import os
import concurrent.futures
import numpy as np
from .binance_common import check_file, ticker_outpath
from .binance_markets import get_bin_uni_local
from ..binance.universe import coin_to_binance_contract, binance_contract_to_coin
from .binance_backfill_perps import backfill_tickers, get_outdir

ROOT_DIR = "../data"


def make_df(dfs, sample="1T"):
    sample = "1T" if sample == "1m" else sample
    df_all = pd.DataFrame()
    for coin in dfs:
        df = dfs[coin].copy()
        df = df.sort_index()[["symbol", "open", "high", "low", "close", "volume"]]
        df["twap"] = (df["open"] / 2 + df["close"] / 2 + df["high"] + df["low"]) / 3
        df["twap_forward"] = df["twap"].shift(-1)
        group = pd.DataFrame()
        group[["close", "twap"]] = df[["close", "twap"]].resample(sample).last()
        group["volume"] = df["volume"].resample(sample).sum()
        group["r"] = np.log(1 + group["close"].pct_change())
        group["volume_ema_10"] = group["volume"].ewm(span=10).mean()
        group[["open", "twap_forward"]] = (
            df[["open", "twap_forward"]].resample(sample).first()
        )
        group["low"] = df["low"].resample(sample).min()
        group["high"] = df["high"].resample(sample).max()
        if sample == "1T":
            group["ret_twap"] = group["twap"].pct_change()
            group["ret_twap_forward"] = group["ret_twap"].shift(-2)
        else:
            group["ret_twap"] = group["twap"] / group["twap_forward"] - 1
            group["ret_twap_forward"] = group["ret_twap"].shift(-1)
        group["range"] = group["high"] - group["low"]
        group["symbol"] = coin
        df_all = pd.concat([df_all, group])
    return df_all


def ticker_to_coin(ticker):
    return binance_contract_to_coin(ticker.split("_")[0] + "USDT")


def load_ticker(
    ticker, sdate: dt.datetime, edate: dt.datetime, interval="1h", asset="perps"
):
    if edate is None:
        edate = dt.datetime.now(tz=dt.timezone.utc)
    s = ticker_to_coin(ticker)
    df = process_coin(s, sdate, edate, "../data", asset, interval=interval)
    df = df.reset_index().rename(columns={"timestamp": "date"})
    df = df[df["date"] <= edate]
    return df


def process_coin(
    coin: str, sdate: dt.datetime, edate: dt.datetime, root_dir, type_, interval="1m"
):
    ticker = coin_to_binance_contract(coin)
    if type_ == "spot":
        ticker = ticker.replace("USDT", "_USDT")
    date = sdate
    df = None

    if interval != "1m":
        date = date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        delta = relativedelta(months=1)
    else:
        delta = dt.timedelta(days=1)

    while date.date() <= edate.date():
        fpath = ticker_outpath(
            ticker,
            date,
            os.path.join(root_dir, f"binance_ohlcv/{type_}/{interval.lower()}"),
        )
        date = date + delta
        min_lines = 24 * 60 if interval == "1m" else 1
        if not check_file(fpath, min_lines=min_lines):
            backfill_tickers(
                [ticker], sdate, edate, outdir=get_outdir(interval), interval=interval
            )
            time.sleep(1)
        if not check_file(fpath):
            continue
        if df is None:
            df = pd.read_csv(fpath)
        else:
            df = pd.concat([df, pd.read_csv(fpath)])
    if df is None or df.empty:
        return None
    df["symbol"] = coin
    # old version wtf is this?
    # df["timestamp"] = pd.to_datetime(df["open_time"], unit="ms")
    df["timestamp"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    df = df.reset_index().set_index("timestamp")
    try:
        df = make_df({coin: df}, sample=interval)
    except Exception as e:
        print(coin, e)
        return None
    df["ret_cc"] = np.log(1 + df["close"].pct_change())
    df["ret_oc"] = np.log(df["close"] / df["open"])
    df["vol_30"] = df["ret_cc"].ewm(span=48 * 60).std()
    df = df[df.index >= sdate]
    return df


def build_dfs(
    coins,
    sdate: dt.datetime,
    edate: dt.datetime,
    root_dir=ROOT_DIR,
    type_="perps",
    interval="1m",
    concat=False,
):
    dfs = {}

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(
                process_coin, coin, sdate, edate, root_dir, type_, interval
            ): coin
            for coin in coins
        }

        for future in concurrent.futures.as_completed(futures):
            coin = futures[future]
            try:
                df = future.result()
                if df is not None:
                    dfs[coin] = df
            except Exception as e:
                print(f"Error processing {coin}: {e}")
    if not concat:
        return dfs
    else:
        return pd.concat(dfs.values()).sort_index()


def fetch_all_tickers(
    sdate, edate, limit=1000, minmc=1, rootdir=ROOT_DIR, type_="perps"
):
    unifile = os.path.join(rootdir, "cm_universe/cm_universe_latest.csv")
    universe = pd.read_csv(unifile)
    universe = universe[universe["market_cap"] > minmc * 1e6]
    universe = universe.head(limit)
    universe = universe[["symbol", "market_cap", "tags"]]
    bin_uni = get_bin_uni_local(rootdir)
    # not interested in stablecoins
    universe = universe[~universe["tags"].str.contains("stablecoin")]
    tickers = [el.upper() + "/USDT" for el in universe["symbol"]]
    logging.info(f"Found {len(tickers)} tickers from CM")
    tickers = [el["symbol"] for el in bin_uni["spot_usdt"] if el["symbol"] in tickers]
    logging.info(f"Found {len(tickers)} tickers which are also in Binance")
    dfs = build_dfs(tickers, sdate, edate, rootdir, type_=type_)
    return dfs


if __name__ == "__main__":
    sdate = dt.datetime(2024, 1, 1)
    edate = dt.datetime(2024, 4, 20)
    dfs = build_dfs(["BTC_USDT", "WIF_USDT"], sdate, edate)
    for coin in dfs:
        print(dfs[coin])
    dfs = fetch_all_tickers(sdate, edate, rootdir="../data")
    for coin in dfs:
        print(dfs[coin])


def load_spot_ohlcv_daily(
    start_date, end_date, symbols=None, data_dir="../data/binance_ohlcv/spot_daily"
):
    dfs = {}
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    # If symbols is not provided, load all symbols
    if symbols is None:
        symbols = [
            d for d in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, d))
        ]

    for symbol in symbols:
        all_data = []
        symbol_dir = os.path.join(data_dir, symbol.replace("/", "_"))
        if not os.path.exists(symbol_dir):
            print(f"Directory for {symbol} not found. Skipping.")
            continue

        for filename in os.listdir(symbol_dir):
            file_path = os.path.join(symbol_dir, filename)
            try:
                # Parse the month from the filename
                file_month = dt.datetime.strptime(filename.split(".")[0], "%Y-%m")

                # Load the file only if it's within the date range
                if (
                    file_month < start_date.to_period("M").start_time
                    or file_month > end_date.to_period("M").end_time
                ):
                    continue

                df = pd.read_csv(file_path, parse_dates=["timestamp"])
                df["symbol"] = symbol.replace("_", "/")
                all_data.append(df)
            except Exception as e:
                print(f"Error loading {file_path}: {e}")
                continue

        if not all_data:
            continue
        data = pd.concat(all_data, ignore_index=True)
        data = data[(data["timestamp"] >= start_date) & (data["timestamp"] <= end_date)]
        dfs[symbol] = data.set_index("timestamp").sort_index()
    return dfs
