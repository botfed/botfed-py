import pandas as pd
import numpy as np
from binance.client import Client
from datetime import datetime, timedelta
import time
import os
from typing import List, Dict, Set
import dotenv
import warnings

warnings.filterwarnings("ignore")

dotenv.load_dotenv()


class MarkoutAnalyzer:
    def __init__(self, api_key: str, api_secret: str):
        # Initialize Binance client and parameters
        self.client = Client(api_key, api_secret)
        self.markout_minutes = 1

    # Existing methods...

    def get_orders(self, symbol: str, start_time: int, end_time: int) -> pd.DataFrame:
        """Fetch all orders for a symbol within a specific time range."""
        orders = []
        current_ts = start_time
        while current_ts < end_time:
            try:
                batch = self.client.futures_get_all_orders(
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
        return df_orders[["orderId", "orderTime"]]

    def get_futures_trades(
        self, symbol: str, chunk_start: str, chunk_end: str
    ) -> pd.DataFrame:
        """Download futures trades for a specific date chunk"""
        start_ts = int(datetime.strptime(chunk_start, "%Y-%m-%d").timestamp() * 1000)
        end_ts = int(
            (datetime.strptime(chunk_end, "%Y-%m-%d") + timedelta(days=1)).timestamp()
            * 1000
        )

        trades = []
        current_ts = start_ts

        while current_ts < end_ts:
            try:
                batch = self.client.futures_account_trades(
                    symbol=symbol, startTime=current_ts, endTime=end_ts, limit=1000
                )

                if not batch:
                    break

                trades.extend(batch)
                current_ts = batch[-1]["time"] + 1
                time.sleep(0.1)

            except Exception as e:
                print(f"Error downloading trades: {e}")
                time.sleep(1)
                break

        if not trades:
            return pd.DataFrame()

        if trades:
            df = pd.DataFrame(trades)
            df["time"] = pd.to_datetime(df["time"], unit="ms")
            df["price"] = df["price"].astype(float)
            df["qty"] = df["qty"].astype(float)
            df["quoteQty"] = df["quoteQty"].astype(float)
            df["commission"] = df["commission"].astype(float)
            df["realizedPnl"] = df["realizedPnl"].astype(float)
            df['kline_time'] = df['time'].dt.floor('1min')


            # Fetch orders and join to get order submission times
            start_ts = int(
                datetime.strptime(chunk_start, "%Y-%m-%d").timestamp() * 1000
            )
            end_ts = int(
                (
                    datetime.strptime(chunk_end, "%Y-%m-%d") + timedelta(days=1)
                ).timestamp()
                * 1000
            )
            df_orders = self.get_orders(symbol, start_ts, end_ts)

            # Merge trades with orders to get order submission times
            df = df.merge(df_orders, on="orderId", how="left")

            # Calculate time to fill
            df["time_to_fill"] = (df["time"] - df["orderTime"]).dt.total_seconds()

            # Convert other fields to numeric as needed...
            return df
        else:
            return pd.DataFrame()

    def get_kline_data(
        self, symbol: str, chunk_start: datetime, chunk_end: datetime
    ) -> pd.DataFrame:
        """Get kline data for a specific date chunk"""
        klines = []
        current_time = chunk_start

        while current_time < chunk_end:
            try:
                batch = self.client.futures_klines(
                    symbol=symbol,
                    interval=Client.KLINE_INTERVAL_1MINUTE,
                    startTime=int(current_time.timestamp() * 1000),
                    endTime=int(
                        min(
                            current_time + timedelta(minutes=1000), chunk_end
                        ).timestamp()
                        * 1000
                    ),
                    limit=1000,
                )

                if not batch:
                    break

                klines.extend(batch)
                current_time += timedelta(minutes=len(batch))
                time.sleep(0.1)

            except Exception as e:
                print(f"Error downloading klines: {e}")
                time.sleep(1)
                break

        if not klines:
            return pd.DataFrame()

        df = pd.DataFrame(
            klines,
            columns=[
                "timestamp",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "close_time",
                "quote_volume",
                "trades_count",
                "taker_buy_volume",
                "taker_buy_quote_volume",
                "ignore",
            ],
        )

        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        df["close"] = df["close"].astype(float)
        df["markout_price"] = df["close"].shift(-1)

        return df[["timestamp", "close", "markout_price"]]

    def calculate_markouts(
        self, trades_df: pd.DataFrame, klines_df: pd.DataFrame
    ) -> pd.DataFrame:
        # Existing method to calculate markouts
        results = pd.merge(
            trades_df,
            klines_df.rename(columns={"timestamp": "kline_time"}),
            on="kline_time",
            how="left",
        )

        # Calculate markout profit and loss in basis points
        results["markout_pnl"] = np.where(
            results["side"] == "BUY",
            (results["markout_price"] - results["price"]) * results["qty"],
            (results["price"] - results["markout_price"]) * results["qty"],
        )
        results["markout_bps"] = results["markout_pnl"] / results["quoteQty"] * 10000

        return results

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

    def analyze_symbol(self, symbol: str, start_date: str, end_date: str) -> Dict:
        """Analyze markouts for a single symbol across multiple date chunks."""
        print(f"\nAnalyzing {symbol}...")

        # Split the date range into chunks
        chunks = self.get_date_chunks(start_date, end_date)
        all_results = []

        for chunk_start, chunk_end in chunks:
            print(f"Processing trades from {chunk_start} to {chunk_end}...")

            # Fetch trades for the symbol within this chunk
            trades = self.get_futures_trades(symbol, chunk_start, chunk_end)
            if trades.empty:
                print(
                    f"No trades found for {symbol} between {chunk_start} and {chunk_end}."
                )
                continue

            # Calculate the start and end time for fetching kline data
            start_time = trades["time"].min().floor("1min")
            end_time = trades["time"].max().ceil("1min") + timedelta(minutes=1)

            # Fetch kline data for this period
            klines = self.get_kline_data(symbol, start_time, end_time)
            if klines.empty:
                print(
                    f"No kline data available for {symbol} between {start_time} and {end_time}."
                )
                continue

            # Calculate markouts by joining trades and kline data
            results = self.calculate_markouts(trades, klines)
            if results.empty:
                print(
                    f"No valid markouts calculated for {symbol} between {chunk_start} and {chunk_end}."
                )
                continue

            all_results.append(results)

        # Combine results from all chunks
        if not all_results:
            print(f"No valid trade data found for {symbol}.")
            return None

        combined_results = pd.concat(all_results, ignore_index=True)

        # Save detailed markout analysis to a CSV file
        output_file = f"markout_analysis_{symbol}_{start_date}_to_{end_date}.csv"
        combined_results.to_csv(output_file, index=False)
        print(f"Saved detailed markout results to {output_file}")

        # Calculate summary statistics
        summary = {
            "symbol": symbol,
            "total_trades": len(combined_results),
            "total_volume": combined_results["quoteQty"].sum(),
            "avg_markout_bps": combined_results["markout_bps"].mean(),
            "median_markout_bps": combined_results["markout_bps"].median(),
            "std_markout_bps": combined_results["markout_bps"].std(),
            "total_pnl": combined_results["markout_pnl"].sum(),
            "win_rate": (combined_results["markout_pnl"] > 0).mean(),
            "median_time_to_fill_pos_markout": combined_results[
                combined_results["markout_pnl"] > 0
            ]["time_to_fill"].median(),
            "median_time_to_fill_neg_markout": combined_results[
                combined_results["markout_pnl"] < 0
            ]["time_to_fill"].median(),
            "avg_time_to_fill_pos_markout": combined_results[
                combined_results["markout_pnl"] > 0
            ]["time_to_fill"].mean(),
            "avg_time_to_fill_neg_markout": combined_results[
                combined_results["markout_pnl"] < 0
            ]["time_to_fill"].mean(),
        }

        print(f"Summary for {symbol} from {start_date} to {end_date}:")
        print(f"Total Trades: {summary['total_trades']}")
        print(f"Total Volume: {summary['total_volume']}")
        print(f"Average Markout (bps): {summary['avg_markout_bps']:.2f}")
        print(f"Median Markout (bps): {summary['median_markout_bps']:.2f}")
        print(f"Markout Std Dev (bps): {summary['std_markout_bps']:.2f}")
        print(f"Total PnL: {summary['total_pnl']:.2f}")
        print(f"Win Rate: {summary['win_rate']:.2%}")
        print(
            f"Median Time to Fill (positive markouts): {summary['median_time_to_fill_pos_markout']:.2f} seconds"
        )
        print(
            f"Median Time to Fill (negative markouts): {summary['median_time_to_fill_neg_markout']:.2f} seconds"
        )
        print(
            f"Average Time to Fill (positive markouts): {summary['avg_time_to_fill_pos_markout']:.2f} seconds"
        )
        print(
            f"Average Time to Fill (negative markouts): {summary['avg_time_to_fill_neg_markout']:.2f} seconds"
        )

        return summary

    def get_symbols_from_trades(self, start_date: str, end_date: str) -> Set[str]:
        """Get all symbols that have trades within the specified date range."""
        symbols = set()
        # Get date chunks for the specified date range
        chunks = self.get_date_chunks(start_date, end_date)

        print("Scanning for traded symbols...")
        for chunk_start, chunk_end in chunks:
            start_ts = int(
                datetime.strptime(chunk_start, "%Y-%m-%d").timestamp() * 1000
            )
            end_ts = int(
                (
                    datetime.strptime(chunk_end, "%Y-%m-%d") + timedelta(days=1)
                ).timestamp()
                * 1000
            )

            current_ts = start_ts
            while current_ts < end_ts:
                try:
                    batch = self.client.futures_account_trades(
                        startTime=current_ts, endTime=end_ts, limit=1000
                    )

                    # Exit if there are no trades in this batch
                    if not batch:
                        break

                    # Collect symbols from trades in this batch
                    for trade in batch:
                        symbols.add(trade["symbol"])

                    # Move to the next batch
                    current_ts = batch[-1]["time"] + 1
                    time.sleep(0.1)

                except Exception as e:
                    print(f"Error while scanning trades: {e}")
                    time.sleep(1)
                    break

        return symbols


def main():
    # Load API credentials
    api_key = os.getenv("BIN_API_KEY_2")
    api_secret = os.getenv("BIN_API_SECRET_2")

    if not api_key or not api_secret:
        raise ValueError(
            "Please set BINANCE_API_KEY and BINANCE_API_SECRET environment variables"
        )

    analyzer = MarkoutAnalyzer(api_key, api_secret)

    # Parameters
    start_date = "2024-11-13"  # Adjust as needed
    end_date = "2024-11-14"  # Adjust as needed

    print(f"Analyzing trades from {start_date} to {end_date} (inclusive)")
    print(f"Data will be processed in 7-day chunks")

    # Get all symbols
    symbols = analyzer.get_symbols_from_trades(start_date, end_date)
    print(f"Found {len(symbols)} symbols with trades: {', '.join(sorted(symbols))}")

    # Analyze each symbol
    all_summaries = []
    for symbol in sorted(symbols):
        summary = analyzer.analyze_symbol(symbol, start_date, end_date)
        if summary:
            all_summaries.append(summary)

    if not all_summaries:
        print("No data found for any symbols")
        return

    # Create combined summary DataFrame
    summary_df = pd.DataFrame(
        [
            {
                "Symbol": s["symbol"],
                "Total Trades": s["total_trades"],
                "Total Volume": s["total_volume"],
                "Avg Markout (bps)": s["avg_markout_bps"],
                "Median Markout (bps)": s["median_markout_bps"],
                "Std Dev (bps)": s["std_markout_bps"],
                "Total PnL": s["total_pnl"],
                "Win Rate": s["win_rate"],
            }
            for s in all_summaries
        ]
    )

    # Sort by total volume
    summary_df = summary_df.sort_values("Total Volume", ascending=False)

    # Save combined summary
    summary_file = f"markout_summary_{start_date}_to_{end_date}.csv"
    summary_df.to_csv(summary_file, index=False)
    print(f"\nSaved combined summary to {summary_file}")

    # Print summary
    print("\nOverall Summary (sorted by volume):")
    print(summary_df.to_string(float_format=lambda x: "{:.2f}".format(x)))


if __name__ == "__main__":
    main()

#
#
#
#
#
#
#
#
