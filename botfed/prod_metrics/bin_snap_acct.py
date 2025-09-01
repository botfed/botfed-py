import os
import json
from datetime import datetime, timezone
from .bin_pos_hist import init_binance


def snapshot_portfolio(
    binance, account_name="default", outdir_base="../data/prod/account_snaps"
):
    """
    Save a snapshot of the account's portfolio as a JSON file with a timestamped filename.

    Args:
        binance: Initialized Binance exchange object.
        account_name (str): A unique identifier for the account (e.g., "main", "sub1").
        outdir_base (str): Base directory for storing snapshots.
    """
    now = datetime.now(tz=timezone.utc)
    account_info = binance.fetch_balance()
    account_info["snap_time"] = now.isoformat()
    timestamp = now.strftime("%Y%m%d_%H%M")

    outdir = os.path.join(outdir_base, account_name)
    os.makedirs(outdir, exist_ok=True)

    fpath = os.path.join(outdir, f"snap_{timestamp}.json")
    with open(fpath, "w") as fh:
        json.dump(account_info, fh, indent=2)
    print(f"Snapshot saved to {fpath}")


if __name__ == "__main__":
    binance = init_binance()
    snapshot_portfolio(binance, account_name="main")
