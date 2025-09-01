import os
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
import dotenv
from ..logger import get_logger

dotenv.load_dotenv()
logger = get_logger(__name__)


def fetch_user_fills(address, start_ms):
    resp = requests.post(
        "https://api.hyperliquid.xyz/info",
        json={
            "type": "userFillsByTime",
            "user": address,
            "startTime": int(start_ms),
        },
        headers={"Content-Type": "application/json"},
    )
    resp.raise_for_status()
    return resp.json()


def fetch_user_funding(address):
    resp = requests.post(
        "https://api.hyperliquid.xyz/info",
        json={
            "type": "userFunding",
            "user": address,
        },
        headers={"Content-Type": "application/json"},
    )
    resp.raise_for_status()
    return resp.json()


def snap_hyper_day_trades_and_income(
    address, account_name="hyper_main", outdir_base="../data/prod"
):

    # Directories
    trades_dir = os.path.join(outdir_base, "trades", account_name)
    income_dir = os.path.join(outdir_base, "income", account_name)
    os.makedirs(trades_dir, exist_ok=True)
    os.makedirs(income_dir, exist_ok=True)

    # Time window
    now = datetime.now(timezone.utc)
    start = (now - timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0)
    end = now + timedelta(days=7)
    start_ms = int(start.timestamp() * 1000)
    date_str = now.strftime("%Y-%m-%d")

    logger.info("Fetching user fills...")
    fills = fetch_user_fills(address, start_ms)
    trades_df = pd.DataFrame(fills)
    if not trades_df.empty:
        trades_df["timestamp"] = pd.to_datetime(trades_df["time"], unit="ms", utc=True)
        trades_df = trades_df[
            (trades_df["timestamp"] >= start) & (trades_df["timestamp"] < end)
        ]
        trades_df = trades_df.drop(columns=["time"])
        trades_df.to_parquet(
            os.path.join(trades_dir, f"{date_str}.parquet"), index=False
        )

    logger.info("Fetching funding history...")
    funding = fetch_user_funding(address)
    income_df = pd.DataFrame([{"time": el['time'], **el['delta']} for el in funding])
    if not income_df.empty:
        income_df["timestamp"] = pd.to_datetime(income_df["time"], unit="ms", utc=True)
        income_df = income_df[
            (income_df["timestamp"] >= start) & (income_df["timestamp"] < end)
        ]
        income_df = income_df.drop(columns=["time"])
        income_df.to_parquet(
            os.path.join(income_dir, f"{date_str}.parquet"), index=False
        )


    logger.info(f"Saved Hyperliquid trades and income for {date_str}")


if __name__ == "__main__":
    address = os.environ.get("HEDGER_HYPER_EOA")
    if not address:
        raise ValueError("HEDGER_HYPER_EOA not set in .env")
    snap_hyper_day_trades_and_income(address)
