import json
import pandas as pd
import time
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from binance.client import Client
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.live import Live
from rich.layout import Layout
import requests

# Load environment variables from .env file
load_dotenv()

# Initialize the Rich console
console = Console()

# Binance API endpoint for book ticker
BINANCE_API_URL = "https://fapi.binance.com/fapi/v1/ticker/bookTicker"

# Initialize Binance client
client = Client(os.getenv("BIN_API_KEY"), os.getenv("BIN_API_SECRET"))


def get_all_midquotes():
    """Fetch midquotes for all symbols in one call."""
    try:
        response = requests.get(BINANCE_API_URL)
        response.raise_for_status()
        data = response.json()
        midquotes = {
            item["symbol"]: (float(item["bidPrice"]) + float(item["askPrice"])) / 2
            for item in data
        }
        return midquotes
    except requests.RequestException as e:
        console.print(f"[red]Error fetching midquotes:[/red] {e}")
        return {}


def fetch_recent_trades():
    """Fetch all trades from the last 24 hours from Binance using pagination."""
    trades = []
    now = datetime.utcnow()
    end_time = now - timedelta(hours=24)  # Define the start of the 24-hour period

    # Get the initial batch of trades
    recent_trades = client.futures_account_trades(
        startTime=int(end_time.timestamp() * 1000), limit=500
    )

    # Process the first batch of trades and filter by the last 24 hours
    for trade in recent_trades:
        trade["time"] = pd.to_datetime(trade["time"], unit="ms")
    recent_trades = [trade for trade in recent_trades if trade["time"] >= end_time]
    trades.extend(recent_trades)

    # Continue fetching trades using pagination
    while recent_trades:
        last_trade_id = trades[-1]["id"]
        recent_trades = client.futures_account_trades(
            limit=500, fromId=last_trade_id + 1
        )
        print(f"fetched {len(recent_trades)} trades, {last_trade_id}")

        # Convert timestamps and filter out trades older than 24 hours
        for trade in recent_trades:
            trade["time"] = pd.to_datetime(trade["time"], unit="ms")
        recent_trades = [trade for trade in recent_trades if trade["time"] >= end_time]

        # Add filtered trades to the list
        trades.extend(recent_trades)

        # Stop if fewer than 500 trades were returned, meaning there are no more trades in the period
        if len(recent_trades) < 500:
            break

    # Convert to DataFrame and calculate notional and realized PnL
    trades_df = pd.DataFrame(trades)
    trades_df["notional"] = trades_df["price"].astype(float) * trades_df["qty"].astype(
        float
    )
    trades_df = trades_df[["time", "symbol", "side", "notional", "realizedPnl"]]

    # Calculate the realized PnL for the last 24 hours
    realized_pnl_24h = trades_df["realizedPnl"].astype(float).sum()

    return trades_df, realized_pnl_24h


def update_display(json_file):
    """Updates the display by reading the JSON file and refreshing account data."""
    try:
        with open(json_file, "r") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        console.print(f"[red]Error reading JSON file:[/red] {e}")
        return None

    try:
        positions_df = pd.DataFrame(data["account"].get("positions", {})).T.sort_index()
        assets_df = pd.DataFrame(data["account"].get("assets", {})).T.reset_index()
        open_orders_df = pd.DataFrame(data.get("open_orders", {})).T

        # Calculate equity, total assets, and unrealized profit
        total_assets = (
            assets_df["wallet_balance"].sum() if "wallet_balance" in assets_df else 0
        )
        unrealized_profit = (
            positions_df["unrealized_profit"].sum()
            if "unrealized_profit" in positions_df
            else 0
        )
        equity = total_assets + unrealized_profit

        notional_value = (
            positions_df["notional"].abs().sum() if "notional" in positions_df else 0
        )
        leverage = notional_value / equity if equity != 0 else float("inf")

        # Fetch recent trades and filter for recent notional calculations
        trades_df, realized_pnl_24h = fetch_recent_trades()
        now = datetime.utcnow()
        last_24_hours_df = trades_df[trades_df["time"] >= now - timedelta(hours=24)]
        last_1_hour_df = trades_df[trades_df["time"] >= now - timedelta(hours=1)]

        # Summary DataFrame with new columns for notional traded in last 24 hours and 1 hour
        summary = pd.DataFrame(
            [
                {
                    "Total Equity": round(equity, 2),
                    "Dollar Exposure": round(positions_df["notional"].sum(), 2),
                    # "Total Assets": round(total_assets, 2),
                    # "Unrealized Profit": round(unrealized_profit, 2),
                    "Leverage": round(leverage, 2),
                    "Ntl Traded Last 24h": round(last_24_hours_df["notional"].sum(), 2),
                    "Number Trades Last 24h": last_24_hours_df.shape[0],
                    "Ntl Traded Last 1h": round(last_1_hour_df["notional"].sum(), 2),
                    "Number Trades Last 1h": last_1_hour_df.shape[0],
                    "PnL Last 24h": round(realized_pnl_24h + unrealized_profit, 2),
                    "PnL bips /ntl Last 24h": round(
                        (realized_pnl_24h + unrealized_profit)
                        / last_24_hours_df["notional"].sum()
                        * 10000,
                        2,
                    ),
                }
            ]
        )

        # Display last 10 trades
        last_10_trades_df = trades_df.sort_values("time", ascending=False).head(10)

        # Helper function to render DataFrame as a rich Table
        def render_dataframe(df, title):
            if df.empty:
                return Panel(f"[yellow]No {title} Data Available[/yellow]", title=title)
            table = Table(title=title)
            for col in df.columns:
                table.add_column(col, justify="right", style="white")
            for _, row in df.iterrows():
                table.add_row(*[str(item) for item in row])
            return table

        # Fetch all midquotes and calculate bips for each open order
        midquotes = get_all_midquotes()
        if not open_orders_df.empty:
            open_orders_df["bips_from_midquote"] = open_orders_df.apply(
                lambda row: calculate_bips_from_midquote(row, midquotes), axis=1
            )
        open_orders_df["ntl"] = (
            open_orders_df["price"].astype(float) * open_orders_df["qty"].astype(float)
        ).round(2)

        open_orders_df = open_orders_df[
            [
                "side",
                "coin",
                "price",
                "ntl",
                "bips_from_midquote",
                # "cloid",
                "oid",
                "status",
            ]
        ].sort_values(["side", "coin"])

        # Render all tables to layout
        layout = Layout()

        # Split into two main columns: Left for Open Orders and Last 10 Trades, Right for Positions and Summary
        layout.split_row(Layout(name="left", ratio=1), Layout(name="right", ratio=1))

        # Split left column into rows for Open Orders and Last 10 Trades
        layout["left"].split_column(
            Layout(render_dataframe(open_orders_df, "Open Orders")),
            Layout(render_dataframe(last_10_trades_df, "Last 10 Trades")),
        )

        # Split right column into rows for Positions and Summary
        layout["right"].split_column(
            Layout(render_dataframe(positions_df, "Positions")),
            Layout(render_dataframe(summary, "Summary")),
        )

        return layout

    except KeyError as e:
        console.print(f"[red]KeyError:[/red] {e}")
        return None


def calculate_bips_from_midquote(order_row, midquotes):
    """Calculate the bips difference from the midquote for an open order."""
    symbol = order_row["coin"]
    price = float(order_row["price"])
    midquote = midquotes.get(symbol)

    if midquote is None:
        return None

    bips = ((price - midquote) / midquote) * 10000  # Calculate bips (basis points)
    return round(bips, 2)


# Main script: Refresh every 30 seconds without using a loop directly
json_file = "../data/prod/bin_account.json"

with Live(console=console, refresh_per_second=2, screen=True) as live:
    while True:
        layout = update_display(json_file)
        if layout is not None:
            live.update(layout)
        time.sleep(10)
