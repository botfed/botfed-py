import datetime as dt
import pandas as pd
import os
import pandera as pa
from pandera.typing import DataFrame
from dateutil.relativedelta import relativedelta
from ...binance.universe import coin_to_binance_contract
from .. import local_data as ld

OUTDIR = "../data/universe/bin/spot"


def get_outfile(sdate: dt.datetime, outdir: str):
    return f"{outdir}/{sdate.year}/{sdate.strftime('%Y%m%d')}.csv"


def get_all_tickers(root_dir=ld.ROOT_DIR, subdir="binance_ohlcv/spot/1d"):
    directory = os.path.join(root_dir, f"{subdir}")
    tickers = [
        d for d in os.listdir(directory) if os.path.isdir(os.path.join(directory, d))
    ]
    return tickers


class OHLCVSchema(pa.DataFrameModel):

    symbol: pa.typing.Series[str]
    open: pa.typing.Series[float]
    high: pa.typing.Series[float]
    low: pa.typing.Series[float]
    close: pa.typing.Series[float]
    volume: pa.typing.Series[float]
    date: pa.typing.Series[pd.DatetimeTZDtype(tz="UTC")]


class UniSchema(pa.DataFrameModel):

    symbol: pa.typing.Series[str]
    date: pa.typing.Series[pd.DatetimeTZDtype(tz="UTC")]
    listed_at: pa.typing.Series[pd.DatetimeTZDtype(tz="UTC")]
    delisted_at: pa.typing.Series[pd.DatetimeTZDtype(tz="UTC")]
    dollar_volume_30d_avg: pa.typing.Series[float]
    volume_30d_avg: pa.typing.Series[float]
    volume: pa.typing.Series[float]
    dollar_volume: pa.typing.Series[float]
    twap: pa.typing.Series[float]


@pa.check_types
def load_ticker(
    ticker: str,
    sdate: dt.datetime,
    edate: dt.datetime,
    root_dir: str = "../data/binance_ohlcv/spot/",
    interval: str = "1d",
) -> DataFrame[OHLCVSchema]:
    if ticker[-5:] != "_USDT":
        ticker = coin_to_binance_contract(ticker).replace("USDT", "_USDT")
    root_dir += interval
    if edate is None:
        edate = dt.datetime.now(tz=dt.timezone.utc)
    sdate = sdate
    dir = os.path.join(root_dir, ticker)
    df = pd.DataFrame()
    d = sdate
    while d <= edate:
        try:
            tmp = pd.read_csv(os.path.join(dir, f"{d.strftime('%Y%m')}.csv"))
            df = pd.concat([df, tmp])
        except FileNotFoundError:
            continue
        finally:
            d += relativedelta(months=1)
            d = dt.datetime(d.year, d.month, 1, tzinfo=dt.timezone.utc)
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"], utc=True, format="ISO8601")
        df["symbol"] = ticker
        df = df[(df["date"] >= sdate) & (df["date"] <= edate)]
        df = df.drop_duplicates(subset="date", keep="first")
    else:  # Return an empty DataFrame with correct dtypes for schema validation
        return pd.DataFrame(
            {
                "symbol": pd.Series(dtype="str"),
                "open": pd.Series(dtype="float"),
                "high": pd.Series(dtype="float"),
                "low": pd.Series(dtype="float"),
                "close": pd.Series(dtype="float"),
                "volume": pd.Series(dtype="float"),
                "date": pd.Series(dtype="datetime64[ns, UTC]"),
            }
        )
    return df


def get_dfs(tickers, sdate: dt.datetime, edate: dt.datetime):
    return {ticker: load_ticker(ticker, sdate, edate) for ticker in tickers}


@pa.check_types
def backfill_uni(sdate: dt.datetime, edate: dt.datetime):
    tickers = sorted(get_all_tickers())
    tickers = [t for t in tickers if t[-7:] != "UP_USDT" and t[-9:] != "DOWN_USDT"]
    df_all = pd.DataFrame()
    for ticker in tickers:
        df = load_ticker(ticker, sdate, edate)
        if df.empty:
            continue
        df["twap"] = df[["open", "high", "low", "close"]].mean(axis=1)
        df["dollar_volume"] = df["volume"] * df["twap"]
        df["listed_at"] = df.iloc[0]["date"]
        delisted_at = df.iloc[-1]["date"]
        df["delisted_at"] = (
            delisted_at
            if delisted_at < edate
            else dt.datetime(3000, 1, 1, 1, tzinfo=dt.timezone.utc)
        )
        df["dollar_volume_30d_avg"] = (
            df["dollar_volume"].rolling(window=30, min_periods=1).mean()
        )
        df["volume_30d_avg"] = df["volume"].rolling(window=30, min_periods=1).mean()
        df["symbol"] = ticker
        df: UniSchema[DataFrame] = df[
            [
                "symbol",
                "date",
                "listed_at",
                "delisted_at",
                "dollar_volume_30d_avg",
                "volume_30d_avg",
                "volume",
                "dollar_volume",
                "twap",
            ]
        ]
        df_all = pd.concat([df_all, df])

    while sdate <= edate:
        df: UniSchema[DataFrame] = df_all[
            df_all["date"].dt.date == sdate.date()
        ].sort_values(by="dollar_volume_30d_avg", ascending=False)
        if not df.empty:
            outfile = get_outfile(sdate, OUTDIR)
            if not os.path.exists(os.path.dirname(outfile)):
                os.makedirs(os.path.dirname(outfile))
            df.to_csv(outfile, index=False)
        sdate += dt.timedelta(days=1)


@pa.check_types
def load_uni(sdate: dt.timedelta, edate: dt.timedelta) -> DataFrame[UniSchema]:
    df_all = pd.DataFrame()
    while sdate <= edate:
        outfile = get_outfile(sdate, OUTDIR)
        df_all = pd.concat([df_all, pd.read_csv(outfile)])
        sdate += dt.timedelta(days=1)
    df_all["date"] = pd.to_datetime(df_all["date"], utc=True, format="ISO8601")
    df_all["listed_at"] = pd.to_datetime(
        df_all["listed_at"], utc=True, format="ISO8601"
    )
    df_all["delisted_at"] = pd.to_datetime(
        df_all["delisted_at"], utc=True, format="ISO8601"
    )
    return df_all


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--sdate", type=str, help="start date")
    parser.add_argument(
        "--edate",
        type=str,
        help="end date (inclusive)",
        default=dt.datetime.now().strftime("%Y%m%d"),
    )
    args = parser.parse_args()

    sdate = dt.datetime.strptime(args.sdate, "%Y%m%d").replace(tzinfo=dt.timezone.utc)
    edate = dt.datetime.strptime(args.edate, "%Y%m%d").replace(tzinfo=dt.timezone.utc)

    backfill_uni(sdate, edate)
