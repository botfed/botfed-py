import logging
import os
import pandas as pd
from .binance_ohlcv_spot import backfill_tickers
from ..universe.bin import load_uni


def backfill_uni(sdate, edate, outdir):
    uni = load_uni("spot")
    uni = uni[uni['notional'] > 1e5]
    tickers = uni["id"].tolist()
    tickers = [el for el in tickers if "USDT" == el[-4:]]
    # not interested in stablecoins
    logging.info(f"Found {len(tickers)} tickeers")
    backfill_tickers(tickers, sdate, edate, outdir=outdir)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--sdate", type=str, help="start date")
    parser.add_argument("--edate", type=str, help="end date (inclusive)")
    parser.add_argument("--rootdir", type=str, help="output dir", default="../data/")
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
    outdir = os.path.join(args.rootdir, "binance_ohlcv/spot")
    backfill_uni(
        args.sdate,
        args.edate,
        outdir,
    )
