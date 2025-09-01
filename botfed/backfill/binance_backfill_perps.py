import logging
import os
import pandas as pd
from .binance_ohlcv_perps import backfill_tickers
from ..universe.bin import load_uni
from ..binance.universe import coin_to_binance_contract
from .binance_ohlcv_perps import get_outdir


def backfill_uni(sdate, edate, outdir, tickers=None, interval="1m"):
    if tickers is None:
        uni = load_uni("perps")
        # uni = uni[uni["notional"] >= 1e5]
        tickers = uni["id"].tolist()
        tickers = [el for el in tickers if "USDT" == el[-4:]]
        # not interested in stablecoins
        logging.info(f"Found {len(tickers)} tickeers")
    else:
        tickers = [coin_to_binance_contract(el) for el in tickers]
    tickers = sorted(tickers)
    backfill_tickers(tickers, sdate, edate, outdir=outdir, interval=interval)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--sdate", type=str, help="start date")
    parser.add_argument("--edate", type=str, help="end date (inclusive)")
    parser.add_argument("--rootdir", type=str, help="output dir", default="../data/")
    parser.add_argument(
        "--tickers", type=str, help="tickers to backfill, comma separated", default=None
    )
    parser.add_argument(
        "--interval",
        type=str,
        help="interval to backfill, 1m, 10m, 1h, 1d, etc (pandas syntax)",
        default="1m",
    )
    parser.add_argument(
        "-d",
        "--debug",
        help="Print lots of debugging statements",
        action="store_const",
        dest="loglevel",
        const=logging.DEBUG,
        default=logging.INFO,
    )
    parser.add_argument(
        "-q",
        "--verbose",
        help="Be verbose",
        action="store_const",
        dest="loglevel",
        const=logging.ERROR,
    )
    args = parser.parse_args()
    assert args.interval.lower() in ["1m", "10m", "1h", "1d"]
    logging.basicConfig(level=args.loglevel)
    outdir = get_outdir(args.interval.lower())
    backfill_uni(
        args.sdate,
        args.edate,
        outdir,
        tickers=args.tickers.split(",") if args.tickers is not None else None,
        interval=args.interval.lower(),
    )
