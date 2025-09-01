import ccxt
import datetime as dt
import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from ..binance.data.fetch_klines import fetch_klines_date_range

# Initialize Binance exchange
binance = ccxt.binance()

# Directory to save the data
outdir = "../data/binance_ohlcv/spot"
if not os.path.exists(outdir):
    os.makedirs(outdir)


# Function to fetch and save OHLCV data
def fetch_and_save_ohlcv(
    symbol, sdate: dt.datetime, edate: dt.datetime = None, timeframe="1d"
):
    if edate is None:
        edate = dt.datetime.now(tz=dt.timezone.utc)
    try:
        df = fetch_klines_date_range(symbol, sdate, edate, interval=timeframe)
        # Save to CSV files organized by symbol and month
        for month, month_df in df.groupby(df["date"].dt.tz_localize(None).dt.to_period("M")):
            month_str = month.strftime("%Y%m")
            symbol_dir = os.path.join(outdir, timeframe, symbol.replace("/", "_"))
            if not os.path.exists(symbol_dir):
                os.makedirs(symbol_dir)
            file_path = os.path.join(symbol_dir, f"{month_str}.csv")
            month_df.to_csv(file_path, index=False)
            print(f"Saved {symbol} data for {month_str} to {file_path}")
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")


def backfill_spot(sdate: dt.datetime, edate: dt.datetime, interval='1d'):

    # Get all available trading pairs
    markets = binance.load_markets()
    symbols = [symbol for symbol in markets.keys() if symbol.endswith("/USDT")]

    # Use ThreadPoolExecutor to download data in parallel
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(fetch_and_save_ohlcv, *(symbol, sdate, edate, interval)) for symbol in symbols]
        for future in as_completed(futures):
            future.result()  # Retrieve results to catch exceptions

    print("Data download completed.")


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
    parser.add_argument("--interval", type=str, help="interval 1d, 1h, 1m, etc", default='1d')
    args = parser.parse_args()

    sdate = dt.datetime.strptime(args.sdate, "%Y%m%d").replace(tzinfo=dt.timezone.utc)
    edate = dt.datetime.strptime(args.edate, "%Y%m%d").replace(tzinfo=dt.timezone.utc)

    backfill_spot(sdate, edate, interval=args.interval)
