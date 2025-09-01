import datetime as dt
import pandas as pd
import os
from . import local_data as ld


def get_all_tickers(root_dir=ld.ROOT_DIR, type_="perps"):
    directory = os.path.join(root_dir, f"binance_ohlcv/{type_}")
    tickers = [
        d for d in os.listdir(directory) if os.path.isdir(os.path.join(directory, d))
    ]
    return tickers


def get_outfile(sdate, outdir):
    return f"{outdir}/{sdate.strftime('uni_%Y%m%d')}.csv"


OUTDIR = "../data/universe/bin/perps"


def backfill_uni(sdate, edate):
    tickers = sorted(get_all_tickers())
    listings = []
    dfs = {}
    for ticker in tickers:
        tmp = ld.build_dfs([ticker], sdate, edate)
        if not tmp:
            continue
        df = tmp[ticker]
        dfs[ticker] = df
        if df.empty:
            continue
        list_ts_ms = df.iloc[0]["open_time"]
        list_date = dt.datetime.fromtimestamp(list_ts_ms / 1000, tz=dt.timezone.utc)
        listings.append((ticker, list_date))

    while sdate <= edate:
        uni = [
            {"symbol": ticker, "first_kline": ts}
            for ticker, ts in listings
            if ts <= sdate
        ]
        for el in uni:
            ticker = el["symbol"]
            df = dfs[ticker]
            df = df[
                (df["open_time"] < sdate.timestamp() * 1000)
                & (df["open_time"] >= 1000 * (sdate - dt.timedelta(days=1)).timestamp())
            ]
            el["vlm_24h"] = (df["volume"] * (df["open"] + df["close"]) / 2).sum()
            df = dfs[ticker]
            df = df[
                (df["open_time"] < sdate.timestamp() * 1000)
                & (
                    df["open_time"]
                    >= 1000 * (sdate - dt.timedelta(days=30)).timestamp()
                )
            ]
            el["vlm_30d"] = (df["volume"] * (df["open"] + df["close"]) / 2).sum()
        outfile = get_outfile(sdate, OUTDIR)
        if not os.path.exists(os.path.dirname(outfile)):
            os.makedirs(os.path.dirname(outfile))
        pd.DataFrame(uni).to_csv(outfile, index=False)
        sdate = sdate + dt.timedelta(days=1)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--sdate", type=str, help="start date")
    parser.add_argument("--edate", type=str, help="end date (inclusive)")
    args = parser.parse_args()

    sdate = dt.datetime.strptime(args.sdate, "%Y%m%d").replace(tzinfo=dt.timezone.utc)
    edate = dt.datetime.strptime(args.edate, "%Y%m%d").replace(tzinfo=dt.timezone.utc)

    backfill_uni(sdate, edate)
