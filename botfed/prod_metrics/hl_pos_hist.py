import numpy as np
import os
import json
import datetime as dt
from copy import deepcopy
from dataclasses import dataclass
import pandas as pd
from glob import glob
from datetime import datetime, timedelta
import argparse
from typing import Mapping
from ..backfill import local_data as ld
from ..logger import get_logger


logger = get_logger(__name__)


@dataclass
class Position:
    symbol: str
    position_size: float
    entry_price: float
    unrealized_pnl: float
    position_value: float


@dataclass
class AcctSnap:
    total_equity: float
    unrealized_pnl: float
    positions: Mapping[str, Position]
    available_balance: float
    snap_time: dt.datetime


def fetch_acct_info(sdate: dt.datetime, first=True, acct_name="hyper_main") -> AcctSnap:
    if sdate.tzinfo is None:
        raise ValueError("sdate must be timezone-aware (UTC)")

    folder = f"../data/prod/account_snaps/{acct_name}/"
    date_str = sdate.strftime("%Y%m%d")
    pattern = os.path.join(folder, f"snap_{date_str}_*.json")

    matching_files = sorted(glob(pattern))
    if not matching_files:
        raise FileNotFoundError(f"No snapshot files found for date {date_str}")

    def extract_datetime_utc(fname):
        base = os.path.basename(fname)
        yyyymmdd, hhmm = base.replace("snap_", "").replace(".json", "").split("_")
        dt_naive = dt.datetime.strptime(yyyymmdd + hhmm, "%Y%m%d%H%M")
        return dt_naive.replace(tzinfo=dt.timezone.utc)

    # Filter only those files whose timestamp is after sdate
    if first:
        filtered_files = [f for f in matching_files if extract_datetime_utc(f) > sdate]
        filtered_files = [f for f in matching_files]
    else:
        filtered_files = [f for f in matching_files]
    if not filtered_files:
        raise FileNotFoundError(f"No snapshot files found after {sdate.isoformat()}")

    selected_file = filtered_files[0] if first else filtered_files[-1]

    with open(selected_file) as fp:
        acct_info = json.load(fp)

    positions = {
        el["position"]["coin"]: Position(
            **{
                "symbol": el["position"]["coin"],
                "position_size": float(el["position"]["szi"]),
                "entry_price": float(el["position"]["entryPx"]),
                "unrealized_pnl": float(el["position"]["unrealizedPnl"]),
                "position_value": float(el["position"]["positionValue"]),
            }
        )
        for el in acct_info["assetPositions"]
    }
    unrealized_pnl = sum([p.unrealized_pnl for p in positions.values()])
    total_equity = float(acct_info["marginSummary"]["accountValue"])

    acct_snap = AcctSnap(
        total_equity=total_equity,
        positions=positions,
        unrealized_pnl=unrealized_pnl,
        available_balance=total_equity - unrealized_pnl,
        snap_time=acct_info["snap_time"],
    )

    return acct_snap


def fetch_trades(sdate: dt.datetime, edate: dt.datetime, acct_name="hyper_main"):
    tdate = sdate
    df_all = pd.DataFrame()
    while tdate.date() <= edate.date():
        fpath = f'../data/prod/trades/{acct_name}/{tdate.strftime("%Y-%m-%d")}.parquet'
        tdate += dt.timedelta(days=1)
        if not os.path.exists(fpath):
            continue
        df = pd.read_parquet(fpath)
        df = df[df["timestamp"] >= sdate]
        df_all = pd.concat([df, df_all])
    columns = {
        "coin": "symbol",
        "px": "price",
        "sz": "qty",
        "closedPnl": "realizedPnl",
        "side": "side",
        "fee": "fee",
        "timestamp": "timestamp",
    }
    df_all = df_all[columns.keys()].drop_duplicates().sort_values("timestamp")
    df_all = df_all.rename(
        columns=columns,
    )
    df_all["qty"] = df_all["qty"].astype(float)
    df_all["qty"]
    df_all["price"] = df_all["price"].astype(float)
    df_all["realizedPnl"] = df_all["realizedPnl"].astype(float)
    df_all["fee"] = df_all["fee"].astype(float)
    df_all["side"] = np.where(df_all["side"] == "B", "buy", "sell")
    df_all["qty"] = np.where(df_all["side"] == "buy", df_all["qty"], -df_all["qty"])
    return df_all


def fetch_income(sdate: dt.datetime, edate: dt.datetime, acct_name="hyper_main"):
    tdate = sdate
    df_all = pd.DataFrame()
    while tdate.date() <= edate.date():
        fpath = f'../data/prod/income/{acct_name}/{tdate.strftime("%Y-%m-%d")}.parquet'
        tdate += dt.timedelta(days=1)
        if not os.path.exists(fpath):
            continue
        df = pd.read_parquet(fpath)
        df = df[df["timestamp"] >= sdate]
        df_all = pd.concat([df, df_all])
    df_all = df_all.drop_duplicates().sort_values("timestamp")
    df_all = df_all.rename(
        columns={"type": "incomeType", "usdc": "income", "coin": "symbol"}
    )
    df_all["incomeType"] = np.where(
        df_all["incomeType"] == "funding", "FUNDING", df_all["incomeType"]
    )
    df_all["income"] = df_all["income"].astype(float)
    return df_all


def fetch_all_data(sdate: dt.datetime, edate: dt.datetime, acct_name="hyper_main"):
    acct_start = fetch_acct_info(sdate, first=True, acct_name=acct_name)
    acct_end = fetch_acct_info(edate, first=False, acct_name=acct_name)
    acct_snap_time_start = dt.datetime.fromisoformat(acct_start.snap_time).replace(
        tzinfo=dt.timezone.utc
    )
    acct_snap_time_end = dt.datetime.fromisoformat(acct_end.snap_time).replace(
        tzinfo=dt.timezone.utc
    )
    trades = fetch_trades(acct_snap_time_start, edate, acct_name=acct_name)
    income = fetch_income(acct_snap_time_start, edate, acct_name=acct_name)

    def filt(x):
        return (x["timestamp"] >= acct_snap_time_start) & (
            x["timestamp"] <= acct_snap_time_end
        )

    trades = trades[filt(trades)]
    income = income[filt(income)]
    return acct_start, acct_end, trades, income


def get_hist_snaps(sdate, edate, acct_name="hyper_main"):
    acct_start, acct_end, trades_df, income_df = fetch_all_data(
        sdate, edate, acct_name=acct_name
    )

    trades_df["event_type"] = "trade"
    income_df["event_type"] = "income"

    trades_df.reset_index(inplace=True)
    income_df.reset_index(inplace=True)
    trades_df[trades_df["timestamp"] >= acct_start.snap_time]
    income_df[income_df["timestamp"] >= acct_start.snap_time]

    combined = pd.concat([trades_df, income_df], ignore_index=True).sort_values(
        "timestamp"
    )
    symbols = list(combined["symbol"].dropna().unique())

    equity = float(acct_start.total_equity)
    cash = float(acct_start.available_balance)

    positions = deepcopy(acct_start.positions)
    for s in positions:
        if s not in symbols:
            symbols.append(s)

    acct_snap_time_start = dt.datetime.fromisoformat(acct_start.snap_time).replace(
        tzinfo=dt.timezone.utc
    )

    all_symbols = [s for s in symbols if s.strip() != ""]
    if "AERO" not in all_symbols:
        all_symbols.append("AERO")
    price_df = fetch_prices(
        all_symbols,
        acct_snap_time_start.replace(hour=0, minute=0, second=0, microsecond=0),
        edate,
    )

    ts = pd.to_datetime(acct_start.snap_time, utc=True)
    snap = {
        "timestamp": ts,
        "total_equity": equity,
        "cash": cash,
        "transfer": 0,
        "unrealizedPnl": sum([p.unrealized_pnl for p in positions.values()]),
        "positions": deepcopy(positions),
    }
    snaps = [snap]
    for _, row in combined.iterrows():
        snap = {}
        ts = row["timestamp"]
        prices = price_df.loc[ts.floor("T")]
        if row["event_type"] == "trade":
            symbol, qty, price, realized_pnl, fee = (
                row["symbol"],
                row["qty"],
                row["price"],
                row["realizedPnl"],
                row["fee"],
            )
            pos = positions.get(
                symbol,
                Position(
                    symbol=symbol,
                    position_size=0,
                    position_value=0,
                    entry_price=0,
                    unrealized_pnl=0,
                ),
            )
            qty0 = pos.position_size
            entryPrice0 = pos.entry_price

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

            unrealized_profit = (price - entryPrice) * new_qty
            positions[symbol] = Position(
                **{
                    "symbol": symbol,
                    "position_size": new_qty,
                    "entry_price": entryPrice,
                    "unrealized_pnl": unrealized_profit,
                    "position_value": price * new_qty,
                }
            )
            # update other positions with best guess at price
            for s in all_symbols:
                if s == symbol:
                    continue
                pos = positions.get(
                    s,
                    Position(
                        symbol=s,
                        entry_price=0,
                        position_size=0,
                        unrealized_pnl=0,
                        position_value=0,
                    ),
                )
                pos.unrealized_pnl = (prices[s] - pos.entry_price) * pos.position_size

            cash = cash - fee + realized_pnl
            unrealized_pnl = sum([p.unrealized_pnl for p in positions.values()])
            equity = cash + unrealized_pnl
            snap = {
                "timestamp": ts,
                "total_equity": equity,
                "cash": cash,
                "transfer": 0,
                "unrealizedPnl": unrealized_pnl,
                "positions": deepcopy(positions),
            }
        else:
            if row["incomeType"] in ["REALIZED_PNL", "COMMISSION"]:
                continue
            equity += row["income"]
            cash += row["income"]
            unrealizedPnl = snaps[-1]["unrealizedPnl"] if len(snaps) else 0
            if row["incomeType"] == "TRANSFER":
                transfer = row["income"]
            else:
                transfer = 0
            snap = {
                "timestamp": ts,
                "total_equity": equity,
                "cash": cash,
                "transfer": transfer,
                "unrealizedPnl": unrealizedPnl,
                "positions": deepcopy(positions),
            }
        snaps.append(snap)
    ts = pd.to_datetime(edate, utc=True)
    for s in all_symbols:
        position_value = (
            0 if s not in acct_end.positions else acct_end.positions[s].position_value
        )
        position_size = (
            1e-10
            if s not in acct_end.positions
            else acct_end.positions[s].position_size
        )
        entry_price = (
            0 if s not in acct_end.positions else acct_end.positions[s].entry_price
        )
        price = abs(position_value / position_size)
        pos = positions.get(
            s,
            Position(
                symbol=s,
                position_size=0,
                entry_price=0,
                unrealized_pnl=0,
                position_value=0,
            ),
        )
        pos.unrealized_pnl = (price - entry_price) * pos.position_size
    unrealized_pnl = sum([pos.unrealized_pnl for pos in positions.values()])
    equity = cash + unrealized_pnl
    snaps.append(
        {
            "timestamp": ts,
            "total_equity": equity,
            "cash": cash,
            "transfer": 0,
            "unrealizedPnl": unrealized_pnl,
            "positions": deepcopy(positions),
        }
    )
    snaps_df = price_snaps(price_df, snaps)
    snaps_df["pnl"] = (snaps_df["total_equity"].diff() - snaps_df["transfer"]).fillna(0)

    return (
        snaps_df,
        combined,
        acct_start,
        acct_end,
        all_symbols,
        price_df,
        trades_df,
        income_df,
    )


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
    agg_dict = {col: "last" for col in snaps_df.columns if col != "transfer"}
    agg_dict["transfer"] = "sum"

    snaps_resampled = snaps_df.resample("1min").agg(agg_dict).fillna(method="ffill")

    # Reindex to ensure all minutes in prices are present
    all_minutes = pd.date_range(prices.index.min(), prices.index.max(), freq="1min")
    snaps_resampled = snaps_resampled.reindex(all_minutes, method="ffill")
    # If both have DatetimeIndex
    merged = prices.join(snaps_resampled, how="left")

    for idx, row in merged.iterrows():
        unrealized_pnl = 0
        unrealized_pnl_stale = row["unrealizedPnl"]
        for s, pos in row["positions"].items():
            price = row[s]
            entry_price = pos.entry_price
            qty = pos.position_size
            unrealized_pnl += (price - entry_price) * qty
        merged.at[idx, "unrealizedPnl"] = unrealized_pnl
        merged.at[idx, "total_equity"] += unrealized_pnl - unrealized_pnl_stale
    merged = merged.dropna()
    return merged


def fetch_prices(symbols, sdate: dt.datetime, edate: dt.datetime):
    df_all = ld.build_dfs(symbols, sdate, edate, concat=True)
    df_all = df_all.reset_index().set_index(["timestamp", "symbol"])
    df_all = df_all["close"].unstack("symbol")
    # df_all = df_all.rename(columns={s: coin_to_binance_contract(s) for s in df_all.columns})
    return df_all


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
