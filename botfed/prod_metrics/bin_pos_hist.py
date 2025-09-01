import os
from copy import deepcopy
import time
import dotenv
import ccxt
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
import argparse

from ..logger import get_logger

logger = get_logger(__name__)


def get_positions(binance, trades):
    trades = trades.reset_index()
    symbols = trades["symbol"].unique()
    account_info = binance.fetch_balance()
    init_positions = {}
    for pos in account_info["info"]["positions"]:
        if pos["symbol"] in symbols:
            init_positions[pos["symbol"]] = float(pos["positionAmt"])

    trades = trades.reset_index()
    (
        trades[["time", "symbol", "qty", "realizedPnl", "commission"]]
        .groupby([pd.Grouper(key="time", freq="1min"), "symbol"])
        .sum()
        .reset_index()
    )
    pos_delta = trades.pivot(index="time", columns="symbol", values="qty").fillna(0)
    positions = pos_delta.cumsum()
    positions.index.name = "time"
    for symbol in symbols:
        positions[symbol] += init_positions[symbol] - positions[symbol].iloc[-1]
    return positions, init_positions


def fetch_all_trades_income(binance, start):
    since_ms = int(start.timestamp() * 1000)
    logger.info("Fetching income")
    income_df = fetch_funding_and_fees(binance, since_ms)
    symbols = fetch_all_traded_symbols_from_income(income_df)

    df = pd.DataFrame()

    for symbol in symbols:
        logger.info(f"Fetching trades for {symbol}")
        trades = fetch_futures_trade_history(binance, symbol, since_ms)
        if trades.empty:
            continue
        df = pd.concat([df, trades])
    return df, income_df


def price_snaps(prices, snaps):
    first_ts = snaps[0]["timestamp"]
    prices = prices[prices.index >= first_ts]

    # Convert snaps to DataFrame with timestamp as index
    snaps_df = pd.DataFrame({k: v for k, v in el.items()} for el in snaps)
    snaps_df["timestamp"] = pd.to_datetime(
        pd.to_numeric(snaps_df["timestamp"]), utc=True
    )
    snaps_df = snaps_df.set_index("timestamp").sort_index()

    # Resample to 1-minute, keeping the last snap per minute and shifting
    snaps_resampled = snaps_df.resample("1min").last().fillna(method="ffill")
    fin_equity = snaps_resampled["equity"].iloc[-1]

    # Reindex to ensure all minutes in prices are present
    all_minutes = pd.date_range(prices.index.min(), prices.index.max(), freq="1min")
    snaps_resampled = snaps_resampled.reindex(all_minutes, method="ffill")
    # If both have DatetimeIndex
    merged = prices.join(snaps_resampled, how="left")
    merged["unrealizedPnl"] = 0.0
    merged["equity"] = 0.0

    for idx, row in merged.iterrows():
        unrealized_pnl = 0
        for s, pos in row["positions"].items():
            price = row[s]
            entry_price = pos["entryPrice"]
            qty = pos["qty"]
            unrealized_pnl += (price - entry_price) * qty

        merged.at[idx, "unrealizedPnl"] = unrealized_pnl
        merged.at[idx, "equity"] = unrealized_pnl + row["cash"]
    merged = merged.dropna()
    equity_delta = fin_equity - merged.iloc[-1]["equity"]
    merged["equity"] += equity_delta
    return merged


def get_hist_snaps(binance, start):
    trades_df, income_df = fetch_all_trades_income(binance, start)

    trades_df["event_type"] = "trade"
    income_df["event_type"] = "income"

    trades_df.reset_index(inplace=True)
    income_df.reset_index(inplace=True)

    combined = pd.concat([trades_df, income_df], ignore_index=True).sort_values(
        "timestamp"
    )
    combined = combined[combined["incomeType"] != "COMMISSION"]

    first_trade = trades_df.iloc[0]
    combined = combined[combined["timestamp"] >= first_trade["timestamp"]]

    account_info = binance.fetch_balance()
    final_equity = float(account_info["info"]["totalMarginBalance"])
    positions = {}
    symbols = trades_df["symbol"].unique()
    for pos in account_info["info"]["positions"]:
        symbol = pos["symbol"]
        if symbol not in symbols:
            continue
        amt = float(pos["positionAmt"])
        qty0 = amt - trades_df[trades_df["symbol"] == symbol]["qty"].sum()
        positions[symbol] = {"qty": qty0}
        first_trade = trades_df[trades_df["symbol"] == symbol].iloc[0]
        realizedPnl, qty, price, fee = (
            first_trade["realizedPnl"],
            first_trade["qty"],
            first_trade["price"],
            first_trade["commission"],
        )
        positions[symbol]["entryPrice"] = price + (realizedPnl) / qty

    snap = {}
    snaps = []
    equity = 0
    for _, row in combined.iterrows():
        ts = row["timestamp"]
        if row["event_type"] == "trade":
            symbol, qty, price, fee = (
                row["symbol"],
                row["qty"],
                row["price"],
                row["commission"],
            )
            equity -= fee
            pos = positions.get(symbol, {"qty": 0, "entryPrice": 0.0})
            qty0 = pos["qty"]
            entryPrice0 = pos["entryPrice"]

            new_qty = qty0 + qty

            # If position direction flips
            if qty0 * new_qty < 0:
                entryPrice = price  # New position, price resets
            # If position was flat, new position takes trade price
            elif qty0 == 0:
                entryPrice = price
            # If increasing same direction
            elif qty0 * qty > 0:
                entryPrice = (entryPrice0 * abs(qty0) + price * abs(qty)) / abs(new_qty)
            elif new_qty == 0:
                entryPrice = 0
            else:
                entryPrice = entryPrice0  # Reducing position, price unchanged

            positions[symbol] = {"qty": new_qty, "entryPrice": entryPrice}
            unrealizedPnl = (price - entryPrice) * new_qty
            unrealized_pnl_delta = unrealizedPnl - (
                snaps[-1]["unrealizedPnl"] if len(snaps) else 0
            )
            equity += row["realizedPnl"] + unrealized_pnl_delta
            snap = {
                "timestamp": ts,
                "equity": equity,
                "unrealizedPnl": unrealizedPnl,
                "positions": deepcopy(positions),
            }
        else:
            equity += row["income"]
            unrealizedPnl = snaps[-1]["unrealizedPnl"] if len(snaps) else 0
            snap = {
                "timestamp": ts,
                "equity": equity,
                "unrealizedPnl": unrealizedPnl,
                "positions": deepcopy(positions),
            }
        snaps.append(snap)
    equity_delta = final_equity - snaps[-1]["equity"]
    for snap in snaps:
        snap["equity"] += equity_delta
        snap["cash"] = snap["equity"] - snap["unrealizedPnl"]
    return snaps, combined


def load_api_keys():
    dotenv.load_dotenv()
    api_key = os.environ.get("BIN_API_KEY")
    api_secret = os.environ.get("BIN_API_SECRET")
    if not api_key or not api_secret:
        raise ValueError("API keys not found in .env file.")
    return api_key, api_secret


def init_binance():
    api_key, api_secret = load_api_keys()
    binance = ccxt.binance(
        {
            "apiKey": api_key,
            "secret": api_secret,
            "enableRateLimit": True,
            "sandbox": False,
            "options": {
                "defaultType": "future",
                "adjustForTimeDifference": True,
            },
        }
    )
    binance.load_markets()
    return binance


def fetch_funding_and_fees(binance, since_ms):
    all_income = []
    cursor = None
    while True:
        params = {"limit": 1000, "timestamp": since_ms}
        params = {"limit": 1000, "timestamp": binance.milliseconds()}
        if cursor:
            params["startTime"] = cursor
        else:
            params["startTime"] = since_ms

        income = binance.fapiPrivateGetIncome(params)
        if not income:
            break
        all_income.extend(income)
        last_time = int(income[-1]["time"])
        if last_time == cursor:
            break
        cursor = last_time + 1
        if last_time > int(time.time() * 1000):
            break
        time.sleep(0.2)
    df = pd.DataFrame(all_income)
    if df.empty:
        return df
    df["time"] = pd.to_datetime(pd.to_numeric(df["time"]), unit="ms", utc=True)
    df["income"] = pd.to_numeric(df["income"])
    df["symbol"] = df["symbol"].astype(str)
    df = df.rename(columns={"time": "timestamp"}).sort_values("timestamp")
    df = df.set_index("timestamp")
    return df


def fetch_all_traded_symbols(binance, since_ms):
    income_df = fetch_funding_and_fees(binance, since_ms)
    return fetch_all_traded_symbols_from_income(income_df)


def fetch_all_traded_symbols_from_income(income_df):
    if income_df.empty:
        return []
    commissions = income_df[income_df["incomeType"] == "COMMISSION"]
    unique_symbols = commissions["symbol"].unique()
    return unique_symbols


def fetch_trade_history(binance, symbol, since_ms):
    """Due to a binance limitation will just fech the last 7 days"""
    all_trades = []
    from_id = "0"
    prev_from_id = "0"
    info_keys = [
        "time",
        "symbol",
        "price",
        "qty",
        "side",
        "realizedPnl",
        "commission",
        "commissionAsset",
    ]
    while True:
        trades = binance.fetch_my_trades(
            symbol, limit=1000, params={"type": "future", "from_id": from_id}
        )
        if not trades:
            break
        from_id = trades[-1]["id"]
        if prev_from_id == from_id:
            break
        trades = [{k: t["info"][k] for k in info_keys} for t in trades]
        all_trades.extend(trades)
        prev_from_id = from_id
    df = pd.DataFrame(all_trades)
    if df.empty:
        return df
    df["time"] = pd.to_datetime(pd.to_numeric(df["time"]), unit="ms", utc=True)
    df["price"] = pd.to_numeric(df["price"])
    df["qty"] = pd.to_numeric(df["qty"])
    df["side"] = df["side"]  # buy or sell
    df["qty"] = np.where(df["side"] == "SELL", -df["qty"], df["qty"])
    return df


def fetch_futures_trade_history(binance, symbol, since_ms):
    all_trades = []
    from_id = None
    info_keys = [
        "time",
        "symbol",
        "price",
        "qty",
        "side",
        "realizedPnl",
        "commission",
        "commissionAsset",
    ]
    prev_from_id = None

    while True:
        # Fetch trades with optional from_id parameter
        params = {"type": "future"}
        if from_id is not None:
            params["from_id"] = from_id

        trades = binance.fetch_my_trades(symbol, limit=1000, params=params)

        if not trades:
            break

        # Filter trades by timestamp
        filtered_trades = []
        for trade in trades:
            trade_time = trade["timestamp"]  # This is in milliseconds
            if trade_time >= since_ms:
                filtered_trades.append(trade)

        if not filtered_trades:
            # If no trades match our time filter, we've gone back far enough
            break

        # Extract info from filtered trades
        trade_data = [{k: t["info"][k] for k in info_keys} for t in filtered_trades]
        all_trades.extend(trade_data)

        # Update from_id for next iteration
        prev_from_id = from_id
        from_id = trades[-1]["id"]
        if prev_from_id == from_id:
            break

        # If we got less than 1000 trades, we've reached the end
        if len(trades) < 1000:
            break

    # Convert to DataFrame
    df = pd.DataFrame(all_trades)
    if df.empty:
        return df

    # Process the data
    df["time"] = pd.to_datetime(pd.to_numeric(df["time"]), unit="ms", utc=True)
    df["price"] = pd.to_numeric(df["price"])
    df["commission"] = pd.to_numeric(df["commission"])
    df["realizedPnl"] = pd.to_numeric(df["realizedPnl"])
    df["qty"] = pd.to_numeric(df["qty"])
    df["side"] = df["side"]  # buy or sell
    df["qty"] = np.where(df["side"] == "SELL", -df["qty"], df["qty"])

    # Final filter to ensure we only have trades after since_ms
    df = df[df["time"] >= pd.to_datetime(pd.to_numeric(since_ms), unit="ms", utc=True)]

    df = df.rename(columns={"time": "timestamp"})

    return df.sort_values("timestamp").set_index("timestamp")


def fetch_ohlcv(binance, symbol, since_ms, end_ms):
    all_bars = []
    while since_ms < end_ms:
        ohlcv = binance.fetch_ohlcv(
            symbol,
            timeframe="1m",
            since=since_ms,
            limit=1_000,
        )
        if not ohlcv:
            break
        all_bars.extend(ohlcv)
        since_ms = ohlcv[-1][0] + 60_000  # Advance by 1 minute
        time.sleep(0.2)
    df = pd.DataFrame(
        all_bars, columns=["time", "open", "high", "low", "close", "volume"]
    )
    df["time"] = pd.to_datetime(pd.to_numeric(df["time"]), unit="ms", utc=True)
    return df


def fetch_prices(binance, symbols, since_ms, end_ms):
    df_all = pd.DataFrame()
    for symbol in symbols:
        df = fetch_ohlcv(binance, symbol, since_ms, end_ms)
        df["symbol"] = symbol
        df = df.set_index(["time", "symbol"])
        df_all = pd.concat([df_all, df])

    df_all = df_all["close"].unstack("symbol")
    return df_all


def cum_pnl_from_income(df):
    # df = df[df['incomeType'] != 'TRANSFER']
    df = (
        df[["time", "symbol", "income"]]
        .groupby([pd.Grouper(key="time", freq="1min"), "symbol"])
        .sum()
        .reset_index()
    )
    df = df.pivot(index="time", columns="symbol", values="income").fillna(0).cumsum()
    return df


def calc_equity_curve(binance, start, end):
    since_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    income_df = fetch_funding_and_fees(binance, since_ms)
    symbols = fetch_all_traded_symbols_from_income(income_df)

    # exclude realized pnl as below we are estimating a live pnl from price movements.
    income = income_df[~income_df["incomeType"].isin(["REALIZED_PNL"])]
    income = cum_pnl_from_income(income)

    account_info = binance.fetch_balance()
    equity = float(account_info["info"]["totalMarginBalance"])
    init_positions = {}
    for pos in account_info["info"]["positions"]:
        if pos["symbol"] in symbols:
            init_positions[pos["symbol"]] = float(pos["positionAmt"])

    df = pd.DataFrame()

    for symbol in symbols:
        trades = fetch_futures_trade_history(binance, symbol, since_ms)
        if trades.empty:
            continue
        df = pd.concat([df, trades])

    df = (
        df[["time", "symbol", "qty", "realizedPnl", "commission"]]
        .groupby([pd.Grouper(key="time", freq="1min"), "symbol"])
        .sum()
        .reset_index()
    )
    pos_delta = df.pivot(index="time", columns="symbol", values="qty").fillna(0)
    positions = pos_delta.cumsum()
    prices = fetch_prices(binance, symbols, since_ms, end_ms)
    # Create a full minute-level datetime index
    full_index = pd.date_range(
        start=prices.index.min(), end=prices.index.max(), freq="1min"
    )
    income = income.reindex(full_index).fillna(method="ffill").fillna(0)

    # Reindex and forward fill positions
    positions = positions.reindex(full_index).fillna(method="ffill").fillna(0)
    positions.index.name = "time"
    for symbol in symbols:
        positions[symbol] += init_positions[symbol] - positions[symbol].iloc[-1]

    price_diffs = prices.diff().fillna(0)
    pnl_deltas = price_diffs * positions.shift(1)
    unrealized_pnl = pnl_deltas.dropna().cumsum()

    equity_curve = income.sum(axis=1) + unrealized_pnl.sum(axis=1)
    equity_curve = equity_curve.astype(float)
    discrepancy = equity - equity_curve.iloc[-1]
    adjustment = np.linspace(0, discrepancy, len(equity_curve))
    equity_curve = equity_curve + adjustment
    return equity_curve, income, unrealized_pnl, positions


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Binance Futures Equity Curve Reconstruction"
    )
    parser.add_argument(
        "--start",
        type=str,
        default=(datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d"),
        help="Start date in YYYY-MM-DD format (default: 30 days ago)",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=datetime.utcnow().strftime("%Y-%m-%d"),
        help="End date in YYYY-MM-DD format (default: today)",
    )
    parser.add_argument(
        "--out", type=str, default="equity_curve.csv", help="Output CSV file"
    )
    args = parser.parse_args()

    binance = init_binance()
    start_dt = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt = datetime.strptime(args.end, "%Y-%m-%d").replace(
        tzinfo=timezone.utc
    ) + timedelta(days=1)

    df = calc_equity_curve(binance, start_dt, end_dt)
    if not df.empty:
        df.to_csv(args.out, index=False)
        print(f"Equity curve saved to {args.out}")
    else:
        print("No data found for the specified period.")
