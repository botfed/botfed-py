import concurrent
import os
import pandas as pd
import datetime as dt
from concurrent.futures import ThreadPoolExecutor
import random
from .binance_common import check_file, ticker_outpath
import requests
import time
from dateutil.relativedelta import relativedelta
from ..universe.bin import load_uni
from ..logger import get_logger


logger = get_logger(__name__)


base_url = "https://fapi.binance.com"


def get_outdir(interval: str):
    return os.path.join(
        "../data/",
        "binance_ohlcv/perps/" + interval.lower(),
    )


def fetch_klines(ticker, interval_str, since=0, until=0):
    endpoint = "fapi/v1/klines"
    url = f"{base_url}/{endpoint}"
    params = {
        "symbol": ticker,
        "interval": interval_str,
        "startTime": int(since),
        "endTime": int(until),
        "limit": 1500,
    }
    response = requests.get(url, params=params)
    return response.json()


def fetch_ohlcv(ticker: str, from_ts, to_ts, interval="1m"):
    # note the first element is the open time:
    # https://binance-docs.github.io/apidocs/futures/en/#historical-blvt-nav-kline-candlestick
    ohlcv = []
    last_ts = from_ts
    hashes = {}
    while last_ts < to_ts:
        new_ohlcv = fetch_klines(ticker, interval, since=last_ts, until=to_ts)
        try:
            new_ohlcv = [
                el for el in new_ohlcv if el[0] not in hashes and float(el[0]) < to_ts
            ]
        except Exception as e:
            logger.error(f"{ticker} error={e}, got resp {new_ohlcv}")
            break
        if len(new_ohlcv) == 0:
            break
        ohlcv.extend(new_ohlcv)
        hashes.update({el[0]: True for el in new_ohlcv})
        last_ts = ohlcv[-1][0]
    return ohlcv


def interval_to_minutes(interval):
    if interval.endswith("m"):
        return int(interval[:-1])
    if interval.endswith("h"):
        return int(interval[:-1]) * 60
    if interval.endswith("d"):
        return int(interval[:-1]) * 60 * 24
    if interval.endswith("w"):
        return int(interval[:-1]) * 60 * 24 * 7
    if interval.endswith("M"):
        return int(interval[:-1]) * 60 * 24 * 30
    raise ValueError(f"Unknown interval {interval}")


def fetch_to_file(
    ticker,
    start_t,
    end_t,
    outfile,
    interval="1m",
    conditional_write=True,
    sleep=10,
):
    interval_min = interval_to_minutes(interval)
    if conditional_write and check_file(outfile, interval_min):
        logger.debug(f"Skipping {outfile}")
        return outfile
    try:
        ohlcv = fetch_ohlcv(ticker, start_t, end_t, interval=interval)
    except Exception as e:
        logger.error("Got error", e)
        return
    columns = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_asset_volume",
        "num_trades",
        "taker_buy_base_asset_volume",
        "taker_buy_quote_asset_volume",
        "ignore",
    ]
    df = pd.DataFrame(
        ohlcv,
        columns=columns,
    )
    # df['symbol'] = ticker
    logger.debug(f"Writing {outfile}")
    outdir = os.path.dirname(os.path.abspath(outfile))
    os.makedirs(outdir, exist_ok=True)
    df.to_csv(outfile, index=False)
    time.sleep(sleep)
    return outfile


def backfill_tickers(
    tickers, sdate: dt.datetime, edate: dt.datetime, outdir=".", interval="1m"
):
    args = []
    if isinstance(sdate, str):
        sdate = dt.datetime.strptime(sdate, "%Y%m%d").replace(tzinfo=dt.timezone.utc)
        edate = dt.datetime.strptime(edate, "%Y%m%d").replace(tzinfo=dt.timezone.utc)
    if interval == "1m":
        min_lines = 24 * 60
    else:
        min_lines = 2
    uni = load_uni()
    if interval.endswith("m"):
        step_size = dt.timedelta(days=1)
    else:
        # truncate to beginning of month
        step_size = relativedelta(months=1)
    for ticker in tickers:
        try:
            created_at = dt.datetime.fromtimestamp(
                uni[uni["id"] == ticker]["created"].values[0] / 1000, dt.timezone.utc
            )
        except IndexError:
            created_at = sdate
        rdate = max(created_at, sdate)
        if not interval.endswith("m"):
            rdate = rdate.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        else:
            rdate = rdate.replace(hour=0, minute=0, second=0, microsecond=0)
        while rdate.date() <= edate.date():
            outfile = ticker_outpath(ticker, rdate, outdir)
            if not check_file(outfile, min_lines=min_lines):
                args.append(
                    (
                        ticker,
                        rdate.timestamp() * 1000,
                        (rdate + step_size).timestamp() * 1000,
                        ticker_outpath(ticker, rdate, outdir),
                    )
                )
            else:
                logger.debug(f"Skipping {outfile}")
            rdate = rdate + step_size
            if rdate > dt.datetime.now(dt.timezone.utc):
                break

    num_jobs = len(args)
    completed = 0
    # random.shuffle(args)

    with ThreadPoolExecutor() as exec:
        logger.debug(f"Running with {exec._max_workers} workers")
        logger.debug(f"launching {num_jobs} jobs")
        jobs = [
            exec.submit(fetch_to_file, arg[0], arg[1], arg[2], arg[3], interval, False)
            for arg in args
        ]
        for _ in concurrent.futures.as_completed(jobs):
            completed += 1
            logger.debug(f"Completed {completed}/{num_jobs}")


if __name__ == "__main__":
    import logging
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("tickers", help="tickers to backfill, comma separated")
    parser.add_argument("--sdate", type=str, help="start date")
    parser.add_argument("--edate", type=str, help="end date (inclusive)")
    parser.add_argument(
        "--outdir",
        type=str,
        help="output dir",
        default=".",
        required=False,
    )
    parser.add_argument(
        "-d",
        "--debug",
        help="Print lots of debugging statements",
        action="store_const",
        dest="loglevel",
        const=logging.DEBUG,
        default=logging.WARNING,
    )
    parser.add_argument(
        "-v",
        "--verbose",
        help="Be verbose",
        action="store_const",
        dest="loglevel",
        const=logging.INFO,
    )
    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel)
    backfill_tickers(
        args.tickers.split(","), args.sdate, args.edate, outdir=args.outdir
    )
