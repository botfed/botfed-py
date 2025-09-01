import pandas as pd
import datetime as dt
import logging


def load_uni(outdir="../data/universe_hyp/"):
    return pd.read_csv(f"{outdir}/latest.csv")


def ticker_to_coin(ticker):
    return ticker

if __name__ == "__main__":
    import os
    import argparse
    import dotenv
    from ..hyperliquid.api import HyperLiquidApi

    dotenv.load_dotenv()

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--outdir",
        type=str,
        default="../data/universe_hyp/",
    )
    parser.add_argument(
        "--minvlm",
        type=int,
        default=int(1),
        help="Minimum volume to consider (1000s USD)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of coins to top N by volume",
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
        "-p",
        "--print",
        help="Print list of symbols if set otherwise save to file",
        action="store_true",
        dest="print",
    )
    args = parser.parse_args()

    api = HyperLiquidApi()
    keys = ["name", "dayNtlVlm", "markPx", "openInterest"]
    stats = api.get_market_stats()
    stats = [
        {key: el[key] for key in keys}
        for el in stats
        if float(el["dayNtlVlm"]) > args.minvlm * 1000
    ]
    stats = sorted(stats, key=lambda x: float(x["dayNtlVlm"]), reverse=True)
    if args.limit:
        stats = stats[: args.limit]
    if args.print:
        for symbol in stats:
            print(symbol["name"])
    if not args.print:
        date = dt.datetime.now().strftime("%Y%m%d")
        df = pd.DataFrame.from_records(stats)
        df["oi_ntl"] = df["openInterest"].astype(float) * df["markPx"].astype(float)
        outfile = os.path.join(args.outdir, f"{date}.csv")
        if not os.path.exists(os.path.dirname(outfile)):
            os.makedirs(os.path.dirname(outfile))
        df.to_csv(outfile, index=True)
        if os.path.exists(os.path.join(args.outdir, "latest.csv")):
            os.remove(os.path.join(args.outdir, "latest.csv"))
        os.symlink(f"{date}.csv", os.path.join(args.outdir, "latest.csv"))
