import concurrent
import time
import os
import pandas as pd
import logging
import datetime as dt
from concurrent.futures import ThreadPoolExecutor
import random
import requests
from .binance_common import check_file, ticker_outpath


base_url = "https://api.binance.com"


def fetch_klines(ticker: str, interval_str, since=0, until=0):
    endpoint = "/api/v3/klines"
    url = f"{base_url}{endpoint}"
    params = {
        "symbol": ticker,
        "interval": interval_str,
        "startTime": int(since),
        "endTime": int(until),
        "limit": 1000,
    }
    response = requests.get(url, params=params)
    return response.json()


def fetch_ohlcv(ticker: str, from_ts, to_ts, interval_min=1):
    # note the first element is the open time:
    # https://binance-docs.github.io/apidocs/futures/en/#historical-blvt-nav-kline-candlestick
    interval_str = "%sm" % interval_min
    ohlcv = []
    last_ts = from_ts
    hashes = {}
    while last_ts < to_ts:
        new_ohlcv = fetch_klines(ticker, interval_str, since=last_ts, until=to_ts)
        new_ohlcv = [
            el for el in new_ohlcv if el[0] not in hashes and float(el[0]) < to_ts
        ]
        if len(new_ohlcv) == 0:
            break
        ohlcv.extend(new_ohlcv)
        hashes.update({el[0]: True for el in new_ohlcv})
        last_ts = ohlcv[-1][0]
    return ohlcv


def fetch_to_file(
    ticker, start_t, end_t, outfile, conditional_write=True, interval_min=1, sleep=3
):
    if conditional_write and check_file(outfile, interval_min):
        return outfile
    try:
        ohlcv = fetch_ohlcv(ticker, start_t, end_t, interval_min=interval_min)
    except Exception as e:
        logging.error(e)
        return
    df = pd.DataFrame(
        ohlcv,
        columns=[
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_asset_volume",
            "number_of_trades",
            "taker_buy_base_asset_volume",
            "taker_buy_quote_asset_volume",
            "unused",
        ],
    )
    # df['symbol'] = ticker
    logging.info(f"Writing {outfile}")
    outdir = os.path.dirname(os.path.abspath(outfile))
    os.makedirs(outdir, exist_ok=True)
    df.to_csv(outfile, index=False)
    time.sleep(sleep)
    return outfile


def backfill_tickers(tickers, sdate, edate, outdir="."):
    args = []
    sdate = dt.datetime.strptime(sdate, "%Y%m%d").replace(tzinfo=dt.timezone.utc)
    edate = dt.datetime.strptime(edate, "%Y%m%d").replace(tzinfo=dt.timezone.utc)
    for ticker in tickers:
        num = (edate - sdate).days + 1
        for i in range(num):
            rdate = sdate + dt.timedelta(days=i)
            outfile = ticker_outpath(ticker, rdate, outdir)
            if check_file(outfile):
                continue
            args.append(
                (
                    ticker,
                    rdate.timestamp() * 1000,
                    (rdate + dt.timedelta(days=1)).timestamp() * 1000,
                    ticker_outpath(ticker, rdate, outdir),
                )
            )

    num_jobs = len(args)
    completed = 0
    # random.shuffle(args)

    with ThreadPoolExecutor() as exec:
        logging.info(f"Running with {exec._max_workers} workers")
        logging.info(f"launching {num_jobs} jobs")
        jobs = [
            exec.submit(fetch_to_file, arg[0], arg[1], arg[2], arg[3]) for arg in args
        ]
        for _ in concurrent.futures.as_completed(jobs):
            completed += 1
            logging.info(f"Completed {completed}/{num_jobs}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("tickers", help="tickers to backfill, comma separated")
    parser.add_argument("--sdate", type=str, help="start date")
    parser.add_argument("--edate", type=str, help="end date (inclusive)")
    parser.add_argument("--outdir", type=str, help="output dir", default=".")
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
