import time
from datetime import datetime, timedelta
import pandas as pd
from binance.client import Client
from dotenv import load_dotenv
import os
import argparse


def get_traded_symbols(client, start_time_ms, end_time_ms):
    """
    Deduces symbols you traded during the period by scanning all your futures trades.
    """
    symbols = set()
    temp_start = start_time_ms

    while True:
        trades = client.futures_account_trades(startTime=temp_start)
        if not trades:
            break
        for trade in trades:
            trade_time = trade['time']
            if trade_time > end_time_ms:
                continue
            symbols.add(trade['symbol'])
        if len(trades) < 500:
            break
        temp_start = trades[-1]['time'] + 1

    return list(symbols)


def compute_binance_fees(api_key, api_secret, start_dt, end_dt):
    """
    Computes total futures trading fees and funding fees for Binance over a period.
    """
    client = Client(api_key, api_secret)

    start_time = int(start_dt.timestamp() * 1000)
    end_time = int(end_dt.timestamp() * 1000)

    symbols = get_traded_symbols(client, start_time, end_time)
    print(f"Detected traded symbols: {symbols}")

    ### Fetch Funding Fees ###
    funding_fees = []
    temp_start = start_time

    while True:
        data = client.futures_income_history(
            incomeType='FUNDING_FEE',
            startTime=temp_start,
            endTime=end_time,
            limit=1000
        )
        if not data:
            break
        funding_fees.extend(data)
        if len(data) < 1000:
            break
        temp_start = data[-1]['time'] + 1

    funding_df = pd.DataFrame(funding_fees)
    if not funding_df.empty:
        funding_df['amount'] = funding_df['income'].astype(float)
        total_funding = funding_df['amount'].sum()
    else:
        total_funding = 0.0

    ### Fetch Trading Fees ###
    total_trading_fees = 0.0

    for symbol in symbols:
        temp_start = start_time
        while True:
            trades = client.futures_account_trades(symbol=symbol, startTime=temp_start)
            if not trades:
                break
            for trade in trades:
                trade_time = trade['time']
                if trade_time > end_time:
                    continue
                total_trading_fees += float(trade['commission'])
            if len(trades) < 500:
                break
            temp_start = trades[-1]['time'] + 1

    return {
        "total_trading_fees": total_trading_fees,
        "total_funding_fees": total_funding
    }


if __name__ == "__main__":
    load_dotenv()

    api_key = os.getenv("BIN_API_KEY")
    api_secret = os.getenv("BIN_API_SECRET")

    if not api_key or not api_secret:
        raise ValueError("Missing BIN_API_KEY or BIN_API_SECRET in .env file")

    parser = argparse.ArgumentParser(description="Compute Binance Futures Trading and Funding Fees")

    now = datetime.now()
    seven_days_ago = now - timedelta(days=7)

    parser.add_argument("--start", type=str, default=seven_days_ago.strftime("%Y-%m-%d %H:%M"),
                        help="Start datetime in 'YYYY-MM-DD HH:MM' format (default: 7 days ago)")
    parser.add_argument("--end", type=str, default=now.strftime("%Y-%m-%d %H:%M"),
                        help="End datetime in 'YYYY-MM-DD HH:MM' format (default: now)")

    args = parser.parse_args()

    try:
        start_dt = datetime.strptime(args.start, "%Y-%m-%d %H:%M")
        end_dt = datetime.strptime(args.end, "%Y-%m-%d %H:%M")
    except ValueError:
        raise ValueError("Start and End dates must be in 'YYYY-MM-DD HH:MM' format")

    if end_dt <= start_dt:
        raise ValueError("End datetime must be after Start datetime")

    results = compute_binance_fees(api_key, api_secret, start_dt, end_dt)

    print(f"\nResults for period {start_dt} to {end_dt}:")
    print(f"Total Trading Fees: {results['total_trading_fees']:.6f} USDT")
    print(f"Total Funding Fees: {results['total_funding_fees']:.6f} USDT")
