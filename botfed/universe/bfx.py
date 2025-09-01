import pandas as pd
import datetime as dt
import logging
from ..bfx.api import BFXApi
from ..binance.universe import coin_to_binance_contract


OUTDIR = "../data/universe_bfx/"


def load_uni(outdir=OUTDIR):
    return pd.read_csv(f"{outdir}/latest.csv")


def market_id_to_coin(market_id):
    return market_id.split("-")[0]


def coin_to_bfx_contract(coin):
    return coin_to_binance_contract(coin).replace("USDT", "-USD")


if __name__ == "__main__":
    import os
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--outdir",
        type=str,
        default=OUTDIR,
    )
    parser.add_argument(
        "--minvlm",
        type=int,
        default=int(1),
        help="Minimum volume to consider (1000s USD)",
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

    api = BFXApi()
    keys = ["ticker", "oi_ntl", "24h_volume", "fair_price"]
    stats = api.get_market_data()
    stats = [
        {key: el[key] for key in keys}
        for el in stats
        if float(el["24h_volume"]) > args.minvlm * 1000
    ]
    date = dt.datetime.now().strftime("%Y%m%d")
    df = pd.DataFrame.from_records(stats)
    outfile = os.path.join(args.outdir, f"{date}.csv")
    if not os.path.exists(os.path.dirname(outfile)):
        os.makedirs(os.path.dirname(outfile))
    df.to_csv(outfile, index=True)
    if os.path.exists(os.path.join(args.outdir, "latest.csv")):
        os.remove(os.path.join(args.outdir, "latest.csv"))
    os.symlink(f"{date}.csv", os.path.join(args.outdir, "latest.csv"))
