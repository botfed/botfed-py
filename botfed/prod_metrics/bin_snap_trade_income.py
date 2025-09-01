import os
from datetime import datetime, timedelta, timezone
from .bin_pos_hist import init_binance, fetch_all_trades_income
from ..logger import get_logger

logger = get_logger(__name__)


def snap_prev_day_trades_and_income(
    binance, account_name="main", outdir_base="../data/prod"
):
    # Directories
    trades_dir = os.path.join(outdir_base, "trades", account_name)
    income_dir = os.path.join(outdir_base, "income", account_name)
    os.makedirs(trades_dir, exist_ok=True)
    os.makedirs(income_dir, exist_ok=True)

    # Define previous UTC day range
    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    end = now
    start = (now - timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0)

    # Fetch everything since `start`
    logger.info("Fetching trades_df and income_df")
    trades_df, income_df = fetch_all_trades_income(binance, start)

    trades_df, income_df = trades_df.reset_index(), income_df.reset_index()

    # # Filter to just that day's data
    # trades_df = trades_df[
    #     (trades_df["timestamp"] >= start)
    #     & (trades_df["timestamp"] < start + timedelta(days=1))
    # ]
    # income_df = income_df[
    #     (income_df["timestamp"] >= start)
    #     & (income_df["timestamp"] < start + timedelta(days=1))
    # ]

    # Save files
    date_str = end.strftime("%Y-%m-%d")
    if not trades_df.empty:
        trades_df.to_parquet(
            os.path.join(trades_dir, f"{date_str}.parquet"), index=False
        )

    if not income_df.empty:
        income_df.to_parquet(
            os.path.join(income_dir, f"{date_str}.parquet"), index=False
        )
    print("Snaps saved to", trades_dir, income_dir)


if __name__ == "__main__":
    binance = init_binance()
    snap_prev_day_trades_and_income(binance)
