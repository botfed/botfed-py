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


class MarkoutAnalyzer:
    def __init__(
        self, binance_api_key: str, binance_api_secret: str, tardis_api_key: str
    ):
        """Initialize clients and parameters"""
        self.lag_ms_list = [1, 10, 50, 100, 1000]
        self.binance_client = Client(binance_api_key, binance_api_secret)
        self.tardis_api_key = tardis_api_key
        self.markout_periods = [1, 10, 60, 600]  # seconds
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
        return df_orders[["orderId", "orderTime"]]

    def get_all_futures_trades(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        sdate = datetime.strptime(start_date, "%Y-%m-%d")
        edate = datetime.strptime(end_date, "%Y-%m-%d")
        assert edate >= sdate
        date = sdate
        trades = pd.DataFrame()
        while date <= edate:
            trades = pd.concat(
                [trades, self.get_tardis_trades(symbol, date.strftime("%Y-%m-%d"))]
            )
            date += timedelta(days=1)
        return trades

    def merge_quotes_with_multiple_lags(
        self,
        trades_df,
        quotes_df,
    ):
        """
        Merge trades with quotes at multiple lag intervals efficiently.

        Parameters:
        -----------
        trades_df : pd.DataFrame
            DataFrame containing trades with 'time' and 'orderTime' columns.
        quotes_df : pd.DataFrame
            DataFrame containing quotes with 'timestamp' column and quote fields.

        Returns:
        --------
        pd.DataFrame
            Trades merged with quotes at different lags, including shortfalls with respect to both 'time' and 'orderTime'.
        """
        # Ensure DataFrames are sorted
        print("Merging trades with quotes...")
        trades_df = trades_df.sort_values("time")
        quotes_df = quotes_df.reset_index().sort_values("timestamp")

        # Columns to merge from quotes
        quote_cols = ["timestamp", "mid_price", "spread_bps", "bid_price", "ask_price"]

        # Remove rows with null values in 'time' or 'orderTime'
        results = trades_df.dropna(subset=["time", "orderTime"]).copy().reset_index(drop=True)


        # Add lagged merges one by one
        for lag_ms in self.lag_ms_list:
            # Create suffix based on lag
            suffix = f"_{lag_ms}ms" if lag_ms > 0 else ""

            # Add lagged timestamp column
            quotes_df["ts_lagged"] = quotes_df["timestamp"] + pd.Timedelta(
                f"{lag_ms}ms"
            )

            results = results.sort_values("time")

            # Perform asof merge on 'time'
            lag_result_time = pd.merge_asof(
                results,
                quotes_df[quote_cols + ["ts_lagged"]],
                left_on="time",
                right_on="ts_lagged",
                direction="backward",
            ).reset_index(drop=True)

            # Rename columns with lag suffix and add to results
            for col in quote_cols:
                if col != "timestamp":  # Skip timestamp to avoid redundancy
                    results[f"{col}{suffix}_time"] = lag_result_time[col]

            results = results.sort_values("orderTime")
            # Perform asof merge on 'orderTime'
            lag_result_orderTime = pd.merge_asof(
                results,
                quotes_df[quote_cols + ["ts_lagged"]],
                left_on="orderTime",
                right_on="ts_lagged",
                direction="backward",
            ).reset_index(drop=True)

            # Rename columns with lag suffix and add to results
            for col in quote_cols:
                if col != "timestamp":  # Skip timestamp to avoid redundancy
                    results[f"{col}{suffix}_orderTime"] = lag_result_orderTime[col]

            # Calculate shortfall_bps for both 'time' and 'orderTime' based on trade side
            results[f"shortfall_bps{suffix}_time"] = np.where(
                results["taker_side"].str.upper() == "BUY",
                (results["price"] - results[f"mid_price{suffix}_time"])
                / results[f"mid_price{suffix}_time"]
                * 10000,
                (results[f"mid_price{suffix}_time"] - results["price"])
                / results[f"mid_price{suffix}_time"]
                * 10000,
            )

            results[f"shortfall_bps{suffix}_orderTime"] = np.where(
                results["taker_side"].str.upper() == "BUY",
                (results["price"] - results[f"mid_price{suffix}_orderTime"])
                / results[f"mid_price{suffix}_orderTime"]
                * 10000,
                (results[f"mid_price{suffix}_orderTime"] - results["price"])
                / results[f"mid_price{suffix}_orderTime"]
                * 10000,
            )

        return results

    def get_tardis_trades(self, symbol: str, date: str) -> Dict[str, pd.DataFrame]:
        """Download book ticker data from Tardis CSV endpoint"""
        try:
            csv_path = f"tardis_data/binance-futures_trades_{date}_{symbol}.csv.gz"
            if not os.path.exists(csv_path):
                # Download book ticker CSV for the specific date
                datasets.download(
                    exchange="binance-futures",
                    data_types=["trades"],
                    from_date=date,
                    to_date=date,
                    symbols=[symbol],
                    download_dir="tardis_data",
                    api_key=self.tardis_api_key,
                )

            # Load the downloaded CSV
            if not os.path.exists(csv_path):
                print(f"No book ticker data found for {symbol} on {date}")
                return {}

            df_ticks = pd.read_csv(csv_path)
            df_ticks["time"] = pd.to_datetime(df_ticks["timestamp"], unit="us")
            df_ticks["price"] = df_ticks["price"].astype(float)
            df_ticks["qty"] = df_ticks["amount"].astype(float)
            df_ticks["quoteQty"] = df_ticks["qty"] * df_ticks["price"]
            df_ticks["taker_side"] = df_ticks["side"].str.upper()
            df_ticks = df_ticks.rename(columns={"id": "trade_id"})

            return df_ticks[
                [
                    "time",
                    "symbol",
                    "exchange",
                    "price",
                    "qty",
                    "taker_side",
                    "quoteQty",
                    "trade_id",
                ]
            ]

        except Exception as e:
            print(f"Error downloading Tardis data for {symbol} on {date}: {e}")
            return {}

    def get_tardis_quotes(self, symbol: str, date: str) -> Dict[str, pd.DataFrame]:
        """Download book ticker data from Tardis CSV endpoint"""
        try:
            # Load the downloaded CSV
            csv_path = f"tardis_data/binance-futures_book_ticker_{date}_{symbol}.csv.gz"
            if not os.path.exists(csv_path):
                from_dt = datetime.strptime(date, "%Y-%m-%d")
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
                    # .rolling(window=3600 * seconds, min_periods=10)
                    .std()
                ).clip(lower=1e-4)
                resampled[f"ret_std2_{seconds}s"] = (
                    resampled["mid_price"]
                    .pct_change()
                    .rolling(window=3600 * seconds, min_periods=10)
                    # .rolling(window=3600 * seconds, min_periods=10)
                    .std()
                ).clip(lower=1e-4)
                resampled[f"ret_std3_{seconds}s"] = (
                    resampled["mid_price"]
                    .diff().abs()
                    # .rolling(window=100 * seconds, min_periods=10)
                    .rolling(window=3600 * seconds, min_periods=10)
                    .mean()
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

    def get_futures_trades(
        self, symbol: str, chunk_start: str, chunk_end: str
    ) -> pd.DataFrame:
        """Download futures trades for a specific date chunk"""
        start_ts = int(
            datetime.strptime(chunk_start, "%Y-%m-%d")
            .replace(tzinfo=dt.timezone.utc)
            .timestamp()
            * 1000
        )
        end_ts = int(
            (datetime.strptime(chunk_end, "%Y-%m-%d") + timedelta(days=1))
            .replace(tzinfo=dt.timezone.utc)
            .timestamp()
            * 1000
        )

        trades = []
        current_ts = start_ts

        while current_ts < end_ts:
            try:
                batch = self.binance_client.futures_account_trades(
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

        df = pd.DataFrame(trades)
        df["time"] = pd.to_datetime(df["time"], unit="ms")
        df["price"] = df["price"].astype(float)
        df["qty"] = df["qty"].astype(float)
        df["quoteQty"] = df["quoteQty"].astype(float)
        df["commission"] = df["commission"].astype(float)
        df["realizedPnl"] = df["realizedPnl"].astype(float)
        # flip side to be from the perspective of taker
        # we do this so things are consistent with the all trades routine
        df["taker_side"] = np.where(df["side"] == "BUY", "SELL", "BUY")
        # Fetch orders and join to get order submission times
        start_ts = int(
            datetime.strptime(chunk_start, "%Y-%m-%d")
            .replace(tzinfo=timezone.utc)
            .timestamp()
            * 1000
        )
        end_ts = int(
            (
                datetime.strptime(chunk_end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                + timedelta(days=1)
            ).timestamp()
            * 1000
        )
        df_orders = self.get_orders(symbol, start_ts, end_ts)

        # Merge trades with orders to get order submission times
        df = df.merge(df_orders, on="orderId", how="left")

        # Calculate time to fill
        df["time_to_fill_sec"] = (df["time"] - df["orderTime"]).dt.total_seconds()

        return df

    def calculate_execution_quality(
        self,
        trades_df: pd.DataFrame,
        quotes_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """Calculate execution quality metrics relative to midquote"""

        quotes_df = quotes_df.reset_index().sort_values("timestamp")
        trades_df = trades_df.sort_values("time")

        results = self.merge_quotes_with_multiple_lags(trades_df, quotes_df)

        return results

    def calculate_markouts(
        self,
        trades_df: pd.DataFrame,
        quotes_dict: Dict[int, pd.DataFrame],
    ) -> pd.DataFrame:
        """Calculate markouts at multiple time horizons and execution quality"""
        if trades_df.empty or not quotes_dict:
            return pd.DataFrame()

        # First calculate execution quality using tick data
        results = self.calculate_execution_quality(trades_df, quotes_dict["ticks"])

        # Then calculate markouts for each time period
        for seconds in self.markout_periods:
            period_str = f"{seconds}s"
            quotes_df = quotes_dict[seconds]

            # Round trade times to the appropriate interval
            results[f"quote_time_{period_str}"] = results["time"].dt.floor(period_str)

            # Join with quotes for this period
            period_quotes = quotes_df.reset_index().rename(
                columns={
                    "timestamp": f"quote_time_{period_str}",
                    "mid_price": f"markout_price_{period_str}",
                }
            )

            results = pd.merge(
                results,
                period_quotes[
                    [
                        f"quote_time_{period_str}",
                        f"markout_price_{period_str}",
                        f"ret_std_{seconds}s",
                        f"ret_std2_{seconds}s",
                        f"ret_std3_{seconds}s",
                    ]
                ],
                on=f"quote_time_{period_str}",
                how="left",
            )
            # Add lagged merges one by one
            for lag_ms in self.lag_ms_list:
                # Create suffix based on lag
                suffix = f"_{lag_ms}ms" if lag_ms > 0 else ""
                results[f"shortfall_in_std_{period_str}{suffix}_time"] = results[
                    f"shortfall_bps{suffix}_time"
                ] / (1e4 * results[f"ret_std_{period_str}"])
                results[f"shortfall_in_std_{period_str}{suffix}_orderTime"] = results[
                    f"shortfall_bps{suffix}_orderTime"
                ] / (1e4 * results[f"ret_std_{period_str}"])

            # Calculate markout P&L and bps for this period
            # it's from the perspective of the maker so BUY and SELL are effectively flipped
            results[f"markout_pnl_{period_str}"] = np.where(
                results["taker_side"] == "SELL",
                (results[f"markout_price_{period_str}"] - results["price"])
                * results["qty"],
                (results["price"] - results[f"markout_price_{period_str}"])
                * results["qty"],
            )

            results[f"markout_bps_{period_str}"] = (
                results[f"markout_pnl_{period_str}"] / results["quoteQty"] * 10000
            )

        return results

    def make_symbol_df(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        account: bool = True,
    ) -> Dict:
        """Analyze a single symbol across multiple date chunks"""
        print(f"\nAnalyzing {symbol}...")

        chunks = self.get_date_chunks(start_date, end_date)
        all_results = []

        for chunk_start, chunk_end in chunks:
            print(f"Processing chunk {chunk_start} to {chunk_end}...")

            if account:
                trades = self.get_futures_trades(symbol, chunk_start, chunk_end)
            else:
                trades = self.get_all_futures_trades(symbol, chunk_start, chunk_end)
            if trades.empty:
                continue

            current_date = datetime.strptime(chunk_start, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
            end_date_obj = datetime.strptime(chunk_end, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )

            while current_date <= end_date_obj:
                date_str = current_date.strftime("%Y-%m-%d")

                quotes_dict = self.get_tardis_quotes(symbol, date_str)

                if quotes_dict:
                    daily_trades = trades[trades["time"].dt.date == current_date.date()]

                    if not daily_trades.empty:
                        results = self.calculate_markouts(
                            daily_trades,
                            quotes_dict,
                        )
                        if not results.empty:
                            all_results.append(results)

                current_date += timedelta(days=1)

        if not all_results:
            print(f"No valid data found for {symbol}")
            return None

        combined_results = pd.concat(all_results, ignore_index=True)

        return combined_results

    def analyze_symbol(self, symbol: str, start_date: str, end_date: str) -> Dict:
        """Analyze a single symbol across multiple date chunks"""
        print(f"\nAnalyzing {symbol}...")

        chunks = self.get_date_chunks(start_date, end_date)
        all_results = []

        for chunk_start, chunk_end in chunks:
            print(f"Processing chunk {chunk_start} to {chunk_end}...")

            trades = self.get_futures_trades(symbol, chunk_start, chunk_end)
            if trades.empty:
                continue

            current_date = datetime.strptime(chunk_start, "%Y-%m-%d")
            end_date_obj = datetime.strptime(chunk_end, "%Y-%m-%d")

            while current_date <= end_date_obj:
                date_str = current_date.strftime("%Y-%m-%d")

                quotes_dict = self.get_tardis_quotes(symbol, date_str)

                if quotes_dict:
                    daily_trades = trades[trades["time"].dt.date == current_date.date()]

                    if not daily_trades.empty:
                        results = self.calculate_markouts(daily_trades, quotes_dict)
                        if not results.empty:
                            all_results.append(results)

                current_date += timedelta(days=1)

        if not all_results:
            print(f"No valid data found for {symbol}")
            return None

        combined_results = pd.concat(all_results, ignore_index=True)

        output_file = f"markout_analysis_{symbol}_{start_date}_to_{end_date}.csv"
        combined_results.to_csv(output_file, index=False)
        print(f"Saved detailed results to {output_file}")

        # Calculate summary statistics
        summary = {
            "symbol": symbol,
            "total_trades": len(combined_results),
            "total_volume": combined_results["quoteQty"].sum(),
            "avg_spread_bps": combined_results["spread_bps"].mean(),
            "avg_shortfall_bps": combined_results["shortfall_bps"].mean(),
            "avg_shortfall_std_1s": combined_results["shortfall_in_std_1s"].mean(),
            "median_shortfall_bps": combined_results["shortfall_bps"].median(),
            "std_shortfall_bps": combined_results["shortfall_bps"].std(),
        }

        # Add markout statistics for each period
        for seconds in self.markout_periods:
            period_str = f"{seconds}s"
            summary.update(
                {
                    f"avg_markout_bps_{period_str}": combined_results[
                        f"markout_bps_{period_str}"
                    ].mean(),
                    f"median_markout_bps_{period_str}": combined_results[
                        f"markout_bps_{period_str}"
                    ].median(),
                    f"std_markout_bps_{period_str}": combined_results[
                        f"markout_bps_{period_str}"
                    ].std(),
                    f"total_pnl_{period_str}": combined_results[
                        f"markout_pnl_{period_str}"
                    ].sum(),
                    f"win_rate_{period_str}": (
                        combined_results[f"markout_pnl_{period_str}"] > 0
                    ).mean(),
                    f"avg_shortfall_std_{period_str}": combined_results[
                        f"shortfall_in_std_{period_str}"
                    ].mean(),
                    f"median_shortfall_std_{period_str}": combined_results[
                        f"shortfall_in_std_{period_str}"
                    ].median(),
                }
            )

        return summary, combined_results


def calculate_aggregate_stats(all_results: List[pd.DataFrame]) -> Dict:
    """Calculate aggregate statistics across all symbols"""
    if not all_results:
        return None

    # Combine all results
    combined_df = pd.concat(all_results, ignore_index=True)

    # Basic volume statistics
    volume_stats = {
        "total_trades": len(combined_df),
        "total_volume_usd": combined_df["quoteQty"].sum(),
        "avg_trade_size_usd": combined_df["quoteQty"].mean(),
        "median_trade_size_usd": combined_df["quoteQty"].median(),
    }

    # Execution quality statistics (volume-weighted and simple)
    execution_stats = {
        "avg_spread_bps": combined_df["spread_bps"].mean(),
        "vwap_spread_bps": (combined_df["spread_bps"] * combined_df["quoteQty"]).sum()
        / combined_df["quoteQty"].sum(),
        "avg_shortfall_bps": combined_df["shortfall_bps"].mean(),
        "vwap_shortfall_bps": (
            combined_df["shortfall_bps"] * combined_df["quoteQty"]
        ).sum()
        / combined_df["quoteQty"].sum(),
        "median_shortfall_bps": combined_df["shortfall_bps"].median(),
        "std_shortfall_bps": combined_df["shortfall_bps"].std(),
    }

    # Markout statistics for each period
    markout_stats = {}
    for seconds in [1, 10, 60]:
        period_str = f"{seconds}s"
        markout_stats.update(
            {
                f"avg_markout_bps_{period_str}": combined_df[
                    f"markout_bps_{period_str}"
                ].mean(),
                f"vwap_markout_bps_{period_str}": (
                    combined_df[f"markout_bps_{period_str}"] * combined_df["quoteQty"]
                ).sum()
                / combined_df["quoteQty"].sum(),
                f"median_markout_bps_{period_str}": combined_df[
                    f"markout_bps_{period_str}"
                ].median(),
                f"std_markout_bps_{period_str}": combined_df[
                    f"markout_bps_{period_str}"
                ].std(),
                f"total_pnl_{period_str}": combined_df[
                    f"markout_pnl_{period_str}"
                ].sum(),
                f"win_rate_{period_str}": (
                    combined_df[f"markout_pnl_{period_str}"] > 0
                ).mean(),
                f"avg_shortfall_std_{period_str}": combined_df[
                    f"shortfall_in_std_{period_str}"
                ].mean(),
                f"median_shortfall_std_{period_str}": combined_df[
                    f"shortfall_in_std_{period_str}"
                ].median(),
            }
        )

    # # Daily statistics
    # daily_stats = (
    #     combined_df.set_index("time")
    #     .groupby(pd.Grouper(freq="D"))
    #     .agg(
    #         {
    #             "quoteQty": "sum",
    #             "shortfall_bps": [
    #                 "mean",
    #                 "std",
    #                 lambda x: (x * combined_df.loc[x.index, "quoteQty"]).sum()
    #                 / combined_df.loc[x.index, "quoteQty"].sum(),
    #             ],
    #             "markout_bps_1s": [
    #                 "mean",
    #                 "std",
    #                 lambda x: (x * combined_df.loc[x.index, "quoteQty"]).sum()
    #                 / combined_df.loc[x.index, "quoteQty"].sum(),
    #             ],
    #             "markout_bps_10s": [
    #                 "mean",
    #                 "std",
    #                 lambda x: (x * combined_df.loc[x.index, "quoteQty"]).sum()
    #                 / combined_df.loc[x.index, "quoteQty"].sum(),
    #             ],
    #             "markout_bps_60s": [
    #                 "mean",
    #                 "std",
    #                 lambda x: (x * combined_df.loc[x.index, "quoteQty"]).sum()
    #                 / combined_df.loc[x.index, "quoteQty"].sum(),
    #             ],
    #         }
    #     )
    #     .round(2)
    # )

    # daily_stats.columns = [
    #     "volume_usd",
    #     "shortfall_bps_mean",
    #     "shortfall_bps_std",
    #     "shortfall_bps_vwap",
    #     "markout_1s_mean",
    #     "markout_1s_std",
    #     "markout_1s_vwap",
    #     "markout_10s_mean",
    #     "markout_10s_std",
    #     "markout_10s_vwap",
    #     "markout_60s_mean",
    #     "markout_60s_std",
    #     "markout_60s_vwap",
    # ]

    return {
        "volume_stats": volume_stats,
        "execution_stats": execution_stats,
        "markout_stats": markout_stats,
        # "daily_stats": daily_stats,
    }


def main():
    load_dotenv()

    binance_api_key = os.getenv("BIN_API_KEY")
    binance_api_secret = os.getenv("BIN_API_SECRET")
    tardis_api_key = os.getenv("TARDIS_API_KEY")

    if not all([binance_api_key, binance_api_secret, tardis_api_key]):
        raise ValueError(
            "Please set BINANCE_API_KEY, BINANCE_API_SECRET, and TARDIS_API_KEY environment variables"
        )

    analyzer = MarkoutAnalyzer(binance_api_key, binance_api_secret, tardis_api_key)

    start_date = "2024-11-02"  # Adjust as needed
    end_date = "2024-11-02"  # Adjust as needed

    print(f"Analyzing trades from {start_date} to {end_date} (inclusive)")
    print(
        f"Computing markouts at {', '.join(f'{s}s' for s in analyzer.markout_periods)}"
    )
    print("Including execution quality analysis relative to midquote")

    symbols = analyzer.get_symbols_from_trades(start_date, end_date)
    print(f"Found {len(symbols)} symbols with trades: {', '.join(sorted(symbols))}")

    # Store all detailed results for aggregate analysis
    all_detailed_results = []
    all_summaries = []

    for symbol in sorted(symbols):
        summary, detailed_results = analyzer.analyze_symbol(
            symbol, start_date, end_date
        )
        if summary:
            all_summaries.append(summary)
            all_detailed_results.append(detailed_results)

    if not all_summaries:
        print("No data found for any symbols")
        return

    # Calculate aggregate statistics
    aggregate_stats = calculate_aggregate_stats(all_detailed_results)

    # Save aggregate statistics
    aggregate_file = f"markout_aggregate_stats_{start_date}_to_{end_date}.txt"
    with open(aggregate_file, "w") as f:
        f.write(f"Aggregate Statistics ({start_date} to {end_date})\n")
        f.write("=" * 50 + "\n\n")

        f.write("Volume Statistics:\n")
        f.write("-" * 20 + "\n")
        for key, value in aggregate_stats["volume_stats"].items():
            f.write(f"{key}: {value:,.2f}\n")
        f.write("\n")

        f.write("Execution Quality Statistics:\n")
        f.write("-" * 30 + "\n")
        for key, value in aggregate_stats["execution_stats"].items():
            f.write(f"{key}: {value:.2f}\n")
        f.write("\n")

        f.write("Markout Statistics:\n")
        f.write("-" * 20 + "\n")
        for key, value in aggregate_stats["markout_stats"].items():
            f.write(f"{key}: {value:.2f}\n")
        f.write("\n")

        # f.write("Daily Statistics:\n")
        # f.write("-" * 20 + "\n")
        # f.write(aggregate_stats["daily_stats"].to_string())

    print(f"\nSaved aggregate statistics to {aggregate_file}")

    # Create per-symbol summary DataFrame
    summary_data = []
    for s in all_summaries:
        summary_row = {
            "Symbol": s["symbol"],
            "Total Trades": s["total_trades"],
            "Total Volume": s["total_volume"],
            "Avg Spread (bps)": s["avg_spread_bps"],
            "Avg Shortfall (bps)": s["avg_shortfall_bps"],
            "Median Shortfall (bps)": s["median_shortfall_bps"],
            "Std Dev Shortfall (bps)": s["std_shortfall_bps"],
        }

        for seconds in analyzer.markout_periods:
            period_str = f"{seconds}s"
            summary_row.update(
                {
                    f"Avg Markout {period_str} (bps)": s[
                        f"avg_markout_bps_{period_str}"
                    ],
                    f"Median Markout {period_str} (bps)": s[
                        f"median_markout_bps_{period_str}"
                    ],
                    f"Std Dev {period_str} (bps)": s[f"std_markout_bps_{period_str}"],
                    f"Total PnL {period_str}": s[f"total_pnl_{period_str}"],
                    f"Win Rate {period_str}": s[f"win_rate_{period_str}"],
                }
            )

        summary_data.append(summary_row)

    summary_df = pd.DataFrame(summary_data)
    summary_df = summary_df.sort_values("Total Volume", ascending=False)

    summary_file = f"markout_summary_{start_date}_to_{end_date}.csv"
    summary_df.to_csv(summary_file, index=False)
    print(f"Saved per-symbol summary to {summary_file}")

    print("\nPer-Symbol Summary (sorted by volume):")
    print(summary_df.to_string(float_format=lambda x: "{:.2f}".format(x)))

    print("\nAggregate Statistics:")
    print("Volume Statistics:")
    for key, value in aggregate_stats["volume_stats"].items():
        print(f"{key}: {value:,.2f}")

    print("\nExecution Quality Statistics:")
    for key, value in aggregate_stats["execution_stats"].items():
        print(f"{key}: {value:.2f}")

    print("\nMarkout Statistics:")
    for key, value in aggregate_stats["markout_stats"].items():
        print(f"{key}: {value:.2f}")


if __name__ == "__main__":
    main()
