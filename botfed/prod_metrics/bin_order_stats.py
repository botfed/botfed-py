import pandas as pd
import numpy as np
from binance.client import Client
from datetime import datetime, timedelta, timezone
import datetime as dt
import time
import os
from tardis_dev import datasets, get_exchange_details
from dotenv import load_dotenv
from typing import List, Dict, Set, Tuple
import warnings

warnings.filterwarnings("ignore")


class OrderAnalyzer:
    def __init__(
        self, binance_api_key: str, binance_api_secret: str, tardis_api_key: str
    ):
        """Initialize clients and parameters"""
        self.lag_ms_list = [1, 10, 50, 100, 1000]
        self.binance_client = Client(binance_api_key, binance_api_secret)
        self.tardis_api_key = tardis_api_key
        self.markout_periods = [1, 10, 60]  # seconds
        os.environ["TARDIS_API_KEY"] = tardis_api_key

    def get_orders(self, symbol: str, start_time: int, end_time: int) -> pd.DataFrame:
        """Fetch all orders for a symbol within a specific time range."""
        orders = []
        current_ts = start_time
        while current_ts < end_time:
            try:
                batch = self.binance_client.futures_get_all_orders(
                    symbol=symbol, startTime=current_ts, endTime=end_time, limit=1000
                )

                if not batch:
                    break

                orders.extend(batch)
                current_ts = batch[-1]["updateTime"] + 1
                time.sleep(0.1)

            except Exception as e:
                print(f"Error fetching orders: {e}")
                time.sleep(1)
                break

        if not orders:
            return pd.DataFrame()

        df_orders = pd.DataFrame(orders)
        df_orders["orderTime"] = pd.to_datetime(df_orders["time"], unit="ms")
        df_orders["orderUpdateTime"] = pd.to_datetime(
            df_orders["updateTime"], unit="ms"
        )
        df_orders["price"] = df_orders["price"].astype(float)
        df_orders["taker_side"] = np.where(df_orders["side"] == "BUY", "SELL", "BUY")
        # print(df_orders)
        return df_orders[
            [
                "orderId",
                "orderTime",
                "orderUpdateTime",
                "status",
                "price",
                "taker_side",
                "side",
            ]
        ]

    def merge_quotes_with_multiple_lags(
        self,
        other_df,
        quotes_df,
        other_time_col="orderTime",
    ):
        """
        Merge trades with quotes at multiple lag intervals efficiently.

        Parameters:
        -----------
        other_df : pd.DataFrame
            DataFrame containing trades with 'time' column
        quotes_df : pd.DataFrame
            DataFrame containing quotes with 'timestamp' column and quote fields
        lag_ms_list : list
            List of lag milliseconds to compute

        Returns:
        --------
        pd.DataFrame
            Trades merged with quotes at different lags
        """
        # Ensure DataFrames are sorted
        quotes_df = quotes_df.reset_index().sort_values("timestamp")
        other_df = other_df.sort_values(other_time_col)

        # Columns to merge from quotes
        quote_cols = ["timestamp", "mid_price", "spread_bps", "bid_price", "ask_price"]

        # Create a base DataFrame for results
        results = other_df.copy().reset_index(drop=True)

        # Add lagged merges one by one
        for lag_ms in self.lag_ms_list:
            # Create suffix based on lag
            suffix = f"_{lag_ms}ms" if lag_ms > 0 else ""

            # Add lagged timestamp column
            quotes_df["ts_lagged"] = quotes_df["timestamp"] + pd.Timedelta(
                f"{lag_ms}ms"
            )

            # Perform asof merge
            lag_result = pd.merge_asof(
                results,
                quotes_df[quote_cols + ["ts_lagged"]],
                left_on="orderTime",
                right_on="ts_lagged",
                direction="backward",
            ).reset_index(drop=True)

            # Rename columns with lag suffix and add to results
            for col in quote_cols:
                if col != "timestamp":  # Skip timestamp to avoid redundancy
                    results[f"{col}{suffix}"] = lag_result[col]
            # Calculate shortfall_bps based on trade side
            results[f"shortfall_bps{suffix}"] = np.where(
                results["taker_side"].str.upper() == "BUY",
                (results["price"] - results[f"mid_price{suffix}"])
                / results[f"mid_price{suffix}"]
                * 10000,
                (results[f"mid_price{suffix}"] - results["price"])
                / results[f"mid_price{suffix}"]
                * 10000,
            )
        return results

    def get_tardis_quotes(self, symbol: str, date: str) -> Dict[str, pd.DataFrame]:
        """Download book ticker data from Tardis CSV endpoint"""
        try:
            from_dt = datetime.strptime(date, "%Y-%m-%d")
            to_dt = from_dt + timedelta(days=1)
            to_date = to_dt.strftime("%Y-%m-%d")
            # Download book ticker CSV for the specific date
            datasets.download(
                exchange="binance-futures",
                data_types=["book_ticker"],
                from_date=date,
                to_date=date,
                symbols=[symbol],
                download_dir="tardis_data",
                api_key=self.tardis_api_key,
            )

            # Load the downloaded CSV
            csv_path = f"tardis_data/binance-futures_book_ticker_{date}_{symbol}.csv.gz"
            if not os.path.exists(csv_path):
                print(f"No book ticker data found for {symbol} on {date}")
                return {}

            df_ticks = pd.read_csv(csv_path)
            df_ticks["timestamp"] = pd.to_datetime(df_ticks["timestamp"], unit="us")
            df_ticks.set_index("timestamp", inplace=True)

            # Add mid price and spread calculations using the correct column names
            df_ticks["mid_price"] = (df_ticks["bid_price"] + df_ticks["ask_price"]) / 2
            df_ticks["spread_bps"] = (
                (df_ticks["ask_price"] - df_ticks["bid_price"])
                / df_ticks["mid_price"]
                * 10000
            )

            # Create resampled dataframes for different markout periods
            resampled_quotes = {"ticks": df_ticks}
            for seconds in self.markout_periods:
                freq = f"{seconds}S"
                resampled = df_ticks.resample(freq).last().ffill()
                resampled[f"ret_std_{seconds}s"] = (
                    resampled["mid_price"]
                    .pct_change()
                    .rolling(window=100 * seconds, min_periods=10)
                    .std()
                ).clip(lower=1e-4)
                resampled_quotes[seconds] = resampled

            return resampled_quotes

        except Exception as e:
            print(f"Error downloading Tardis data for {symbol} on {date}: {e}")
            return {}

    def get_date_chunks(self, start_date: str, end_date: str) -> List[tuple]:
        """Split date range into 7-day chunks"""
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        chunks = []
        chunk_start = start

        while chunk_start <= end:
            chunk_end = min(chunk_start + timedelta(days=6), end)
            chunks.append(
                (chunk_start.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d"))
            )
            chunk_start = chunk_end + timedelta(days=1)

        return chunks

    def get_symbols_from_trades(self, start_time: str, end_time: str) -> List[str]:
        """Get all symbols that have trades in the specified date range"""
        symbols = set()
        chunks = self.get_date_chunks(start_time, end_time)

        print("Scanning for traded symbols...")
        for chunk_start, chunk_end in chunks:
            start_ts = int(
                datetime.strptime(chunk_start, "%Y-%m-%d")
                .replace(tzinfo=timezone.utc)
                .timestamp()
                * 1000
            )
            end_ts = int(
                (
                    datetime.strptime(chunk_end, "%Y-%m-%d").replace(
                        tzinfo=timezone.utc
                    )
                    + timedelta(days=1)
                ).timestamp()
                * 1000
            )

            current_ts = start_ts
            while current_ts < end_ts:
                try:
                    batch = self.binance_client.futures_account_trades(
                        startTime=current_ts, endTime=end_ts, limit=1000
                    )

                    if not batch:
                        break

                    for trade in batch:
                        symbols.add(trade["symbol"])

                    current_ts = batch[-1]["time"] + 1
                    time.sleep(0.1)

                except Exception as e:
                    print(f"Error scanning trades: {e}")
                    time.sleep(1)
                    break

        return list(symbols)

    def make_symbol_df(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
    ) -> Dict:
        """Analyze a single symbol across multiple date chunks"""
        print(f"\nAnalyzing {symbol}...")

        chunks = self.get_date_chunks(start_date, end_date)
        all_results = []

        for chunk_start, chunk_end in chunks:
            print(f"Processing chunk {chunk_start} to {chunk_end}...")
            start_ts = int(
                datetime.strptime(chunk_start, "%Y-%m-%d").timestamp() * 1000
            )
            end_ts = int(
                (
                    datetime.strptime(chunk_end, "%Y-%m-%d") + timedelta(days=1)
                ).timestamp()
                * 1000
            )

            orders = self.get_orders(symbol, start_ts, end_ts)
            if orders.empty:
                continue

            current_date = datetime.strptime(chunk_start, "%Y-%m-%d")
            end_date_obj = datetime.strptime(chunk_end, "%Y-%m-%d")

            while current_date <= end_date_obj:
                date_str = current_date.strftime("%Y-%m-%d")

                quotes_dict = self.get_tardis_quotes(symbol, date_str)

                if quotes_dict:
                    daily_orders = orders[
                        orders["orderTime"].dt.date == current_date.date()
                    ]

                    if not daily_orders.empty:
                        results = self.merge_quotes_with_multiple_lags(
                            daily_orders,
                            quotes_dict["ticks"],
                        )
                        if not results.empty:
                            all_results.append(results)

                current_date += timedelta(days=1)

        if not all_results:
            print(f"No valid data found for {symbol}")
            return None

        combined_results = pd.concat(all_results, ignore_index=True)

        return combined_results


def main():
    load_dotenv()

    binance_api_key = os.getenv("BIN_API_KEY")
    binance_api_secret = os.getenv("BIN_API_SECRET")
    tardis_api_key = os.getenv("TARDIS_API_KEY")

    if not all([binance_api_key, binance_api_secret, tardis_api_key]):
        raise ValueError(
            "Please set BINANCE_API_KEY, BINANCE_API_SECRET, and TARDIS_API_KEY environment variables"
        )

    analyzer = OrderAnalyzer(binance_api_key, binance_api_secret, tardis_api_key)

    start_date = "2024-11-02"  # Adjust as needed
    end_date = "2024-11-02"  # Adjust as needed

    print(f"Analyzing trades from {start_date} to {end_date} (inclusive)")
    print(
        f"Computing markouts at {', '.join(f'{s}s' for s in analyzer.markout_periods)}"
    )
    print("Including execution quality analysis relative to midquote")

    symbols = analyzer.get_symbols_from_trades(start_date, end_date)
    print(f"Found {len(symbols)} symbols with trades: {', '.join(sorted(symbols))}")


if __name__ == "__main__":
    main()
