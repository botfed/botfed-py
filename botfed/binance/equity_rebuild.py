import os
import json
import time
import dotenv
import ccxt
import pandas as pd
from datetime import datetime, timezone, timedelta
import argparse


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
            'sandbox': False,
            "options": {
                "defaultType": "future",
                "adjustForTimeDifference": True,
            },
        }
    )
    binance.load_markets()
    return binance


def fetch_all_traded_symbols(binance, since_ms):
    income_df = fetch_funding_and_fees(binance, since_ms)
    if income_df.empty:
        return []
    commissions = income_df[income_df["incomeType"] == "COMMISSION"]
    unique_symbols = commissions["symbol"].unique()
    return unique_symbols


def fetch_trade_history(binance, symbol, since_ms):
    all_trades = []
    from_id = '0'
    prev_from_id = '0'
    while True:
        trades = binance.fetch_my_trades(symbol, limit=1000, params={'type': 'future', 'from_id': from_id})
        if not trades:
            break
        from_id = trades[-1]['id']
        if prev_from_id == from_id:
            break
        all_trades.extend(trades)
        prev_from_id = from_id
    df = pd.DataFrame(all_trades)
    if df.empty:
        return df
    df["time"] = pd.to_datetime(df["timestamp"], unit="ms")
    df["price"] = pd.to_numeric(df["price"])
    df["qty"] = pd.to_numeric(df["amount"])
    df["side"] = df["side"]  # buy or sell
    return df


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
    df["time"] = pd.to_datetime(df["time"], unit="ms")
    df["income"] = pd.to_numeric(df["income"])
    df["symbol"] = df["symbol"].astype(str)
    return df


def fetch_ohlcv(binance, symbol, since_ms, end_ms):
    all_bars = []
    while since_ms < end_ms:
        ohlcv = binance.fetch_ohlcv(symbol, timeframe="1m", since=since_ms, limit=1000)
        if not ohlcv:
            break
        all_bars.extend(ohlcv)
        since_ms = ohlcv[-1][0] + 60_000  # Advance by 1 minute
        time.sleep(0.2)
    df = pd.DataFrame(
        all_bars, columns=["time", "open", "high", "low", "close", "volume"]
    )
    df["time"] = pd.to_datetime(df["time"], unit="ms")
    return df


def reconstruct_equity(binance, symbols, start, end):
    print("symbols", symbols)
    since_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)

    funding_fees = fetch_funding_and_fees(binance, since_ms)
    all_equity = []

    for symbol in symbols:
        print(f"Processing {symbol}")
        trades = fetch_trade_history(binance, symbol, since_ms)
        if trades.empty:
            continue
        ohlcv = fetch_ohlcv(binance, symbol, since_ms, end_ms)
        if ohlcv.empty:
            continue

        ohlcv.set_index("time", inplace=True)
        position = 0
        cash = 0

        equity_curve = []

        for ts, row in ohlcv.iterrows():
            minute_trades = trades[(trades["time"] <= ts)]
            if not minute_trades.empty:
                for _, trade in minute_trades.iterrows():
                    qty = trade["qty"] if trade["side"] == "BUY" else -trade["qty"]
                    cash -= trade["price"] * qty
                    position += qty
                trades = trades[trades["time"] > ts]

            price = (row["high"] + row["low"]) / 2
            pos_value = position * price
            realized = funding_fees[
                (funding_fees["time"] <= ts)
                & (funding_fees["symbol"] == binance.market_id(symbol))
            ]["income"].sum()
            equity = cash + pos_value + realized

            equity_curve.append(
                {
                    "time": ts,
                    "cash": cash,
                    "position": position,
                    "price": price,
                    "pos_value": pos_value,
                    "realized": realized,
                    "equity": equity,
                }
            )

        df_eq = pd.DataFrame(equity_curve)
        all_equity.append(df_eq)

    if all_equity:
        final_df = pd.concat(all_equity).sort_values("time").reset_index(drop=True)
        return final_df
    else:
        return pd.DataFrame()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Binance Futures Equity Curve Reconstruction"
    )
    parser.add_argument(
        "--start",
        type=str,
        default=(datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%d"),
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

    account_info = binance.fetch_balance()
    print('account', json.dumps(account_info, indent=2))
    symbols = fetch_all_traded_symbols(binance, int(start_dt.timestamp() * 1000))

    df = reconstruct_equity(binance, symbols, start_dt, end_dt)
    if not df.empty:
        df.to_csv(args.out, index=False)
        print(f"Equity curve saved to {args.out}")
    else:
        print("No data found for the specified period.")
