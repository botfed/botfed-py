import os
import json
import datetime as dt
from copy import deepcopy
import time
import pandas as pd
from glob import glob
from datetime import datetime, timezone, timedelta
import argparse
from ..backfill import local_data as ld


def fetch_acct_info(sdate: dt.datetime, first=True):
    if sdate.tzinfo is None:
        raise ValueError("sdate must be timezone-aware (UTC)")

    folder = "../data/prod/account_snaps/main/"
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

    acct_info["info"]["positions"] = {
        el["symbol"]: el
        for el in acct_info["info"]["positions"]
        if float(el["positionAmt"]) != 0
    }
    acct_info["info"]["assets"] = [
        el for el in acct_info["info"]["assets"] if float(el["walletBalance"]) != 0
    ]

    return acct_info


def fetch_trades(sdate: dt.datetime, edate: dt.datetime):
    tdate = sdate
    df_all = pd.DataFrame()
    while tdate <= edate:
        fpath = f'../data/prod/trades/main/{tdate.strftime("%Y-%m-%d")}.parquet'
        tdate += dt.timedelta(days=1)
        if not os.path.exists(fpath):
            continue
        df = pd.read_parquet(fpath)
        df = df[df["timestamp"] >= sdate]
        df_all = pd.concat([df, df_all])
    df_all = df_all.drop_duplicates().sort_values("timestamp")
    return df_all


def fetch_income(sdate: dt.datetime, edate: dt.datetime):
    tdate = sdate
    df_all = pd.DataFrame()
    while tdate.date() <= edate.date():
        fpath = f'../data/prod/income/main/{tdate.strftime("%Y-%m-%d")}.parquet'
        tdate += dt.timedelta(days=1)
        if not os.path.exists(fpath):
            continue
        df = pd.read_parquet(fpath)
        df = df[df["timestamp"] >= sdate]
        df_all = pd.concat([df, df_all])
    df_all = df_all.drop_duplicates().sort_values("timestamp")
    return df_all


def fetch_all_data(sdate: dt.datetime, edate: dt.datetime):
    acct_start = fetch_acct_info(sdate, first=True)
    acct_end = fetch_acct_info(edate, first=False)
    acct_snap_time_start = dt.datetime.fromisoformat(acct_start["snap_time"]).replace(
        tzinfo=dt.timezone.utc
    )
    acct_snap_time_end = dt.datetime.fromisoformat(acct_end["snap_time"]).replace(
        tzinfo=dt.timezone.utc
    )
    trades = fetch_trades(acct_snap_time_start, edate)
    income = fetch_income(acct_snap_time_start, edate)

    def filt(x):
        return (x["timestamp"] >= acct_snap_time_start) & (
            x["timestamp"] <= acct_snap_time_end
        )

    trades = trades[filt(trades)]
    income = income[filt(income)]
    return acct_start, acct_end, trades, income


def get_hist_snaps(sdate, edate):
    acct_start, acct_end, trades_df, income_df = fetch_all_data(sdate, edate)

    trades_df["event_type"] = "trade"
    income_df["event_type"] = "income"

    trades_df.reset_index(inplace=True)
    income_df.reset_index(inplace=True)
    trades_df[trades_df["timestamp"] >= acct_start["snap_time"]]
    income_df[income_df["timestamp"] >= acct_start["snap_time"]]

    combined = pd.concat([trades_df, income_df], ignore_index=True).sort_values(
        "timestamp"
    )
    symbols = list(combined["symbol"].unique())

    equity = float(acct_start["info"]["totalMarginBalance"])
    cash = float(acct_start["info"]["totalWalletBalance"])
    positions = {
        s: {
            "qty": float(v["positionAmt"]),
            "entryPrice": float(v["entryPrice"]),
            "unrealizedProfit": float(v["unrealizedProfit"]),
        }
        for s, v in acct_start["info"]["positions"].items()
    }
    for s in positions:
        if s not in symbols:
            symbols.append(s)

    acct_snap_time_start = dt.datetime.fromisoformat(acct_start["snap_time"]).replace(
        tzinfo=dt.timezone.utc
    )

    all_symbols = [s for s in symbols if s.strip() != ""]
    price_df = fetch_prices(
        all_symbols,
        acct_snap_time_start.replace(hour=0, minute=0, second=0, microsecond=0),
        edate,
    )

    ts = pd.to_datetime(acct_start["snap_time"], utc=True)
    snap = {
        "timestamp": ts,
        "equity": equity,
        "cash": cash,
        "transfer": 0,
        "unrealizedProfit": sum([p["unrealizedProfit"] for p in positions.values()]),
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
                row["commission"],
            )
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

            unrealized_profit = (price - entryPrice) * new_qty
            positions[symbol] = {
                "qty": new_qty,
                "entryPrice": entryPrice,
                "unrealizedProfit": unrealized_profit,
            }
            # update other positions with best guess at price
            for s in all_symbols:
                if s == symbol:
                    continue
                pos = positions.get(
                    s, {"entryPrice": 0, "qty": 0, "unrealizedProfit": 0}
                )
                pos["unrealizedProfit"] = (prices[s] - pos["entryPrice"]) * pos["qty"]

            cash = cash - fee + realized_pnl
            unrealized_pnl = sum([p["unrealizedProfit"] for p in positions.values()])
            equity = cash + unrealized_pnl
            snap = {
                "timestamp": ts,
                "equity": equity,
                "cash": cash,
                "transfer": 0,
                "unrealizedProfit": unrealized_pnl,
                "positions": deepcopy(positions),
            }
        else:
            if row["incomeType"] in ["REALIZED_PNL", "COMMISSION"]:
                continue
            equity += row["income"]
            cash += row["income"]
            unrealizedPnl = snaps[-1]["unrealizedProfit"] if len(snaps) else 0
            if row["incomeType"] == "TRANSFER":
                transfer = row["income"]
            else:
                transfer = 0
            snap = {
                "timestamp": ts,
                "equity": equity,
                "cash": cash,
                "transfer": transfer,
                "unrealizedProfit": unrealizedPnl,
                "positions": deepcopy(positions),
            }
        snaps.append(snap)
    ts = pd.to_datetime(edate, utc=True)
    for s in all_symbols:
        price = float(
            acct_end["info"]["positions"].get(s, {}).get("notional", 0)
        ) / float(acct_end["info"]["positions"].get(s, {}).get("positionAmt", 1e-10))
        pos = positions[s]
        pos["unrealizedProfit"] = (price - pos["entryPrice"]) * pos["qty"]
    unrealized_pnl = sum([pos["unrealizedProfit"] for pos in positions.values()])
    equity = cash + unrealized_pnl
    snaps.append(
        {
            "timestamp": ts,
            "equity": equity,
            "cash": cash,
            "transfer": 0,
            "unrealizedProfit": unrealized_pnl,
            "positions": deepcopy(positions),
        }
    )
    snaps_df = price_snaps(price_df, snaps)
    snaps_df["pnl"] = (snaps_df["equity"].diff() - snaps_df["transfer"]).fillna(0)

    return snaps_df, combined, acct_start, acct_end, all_symbols, price_df


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
    fin_equity = snaps_resampled["equity"].iloc[-1]

    # Reindex to ensure all minutes in prices are present
    all_minutes = pd.date_range(prices.index.min(), prices.index.max(), freq="1min")
    snaps_resampled = snaps_resampled.reindex(all_minutes, method="ffill")
    # If both have DatetimeIndex
    merged = prices.join(snaps_resampled, how="left")

    for idx, row in merged.iterrows():
        unrealized_pnl = 0
        unrealized_pnl_stale = row["unrealizedProfit"]
        for s, pos in row["positions"].items():
            price = row[s]
            entry_price = pos["entryPrice"]
            qty = pos["qty"]
            unrealized_pnl += (price - entry_price) * qty
        merged.at[idx, "unrealizedProfit"] = unrealized_pnl
        merged.at[idx, "equity"] += unrealized_pnl - unrealized_pnl_stale
    merged = merged.dropna()
    return merged


def fetch_prices(symbols, sdate: dt.datetime, edate: dt.datetime):
    df_all = ld.build_dfs(symbols, sdate, edate, concat=True)
    df_all = df_all.reset_index().set_index(["timestamp", "symbol"])
    df_all = df_all["close"].unstack("symbol")
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
