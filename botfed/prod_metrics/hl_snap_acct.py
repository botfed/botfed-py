import os
import json
from datetime import datetime, timezone
import requests
import dotenv

dotenv.load_dotenv()


def snapshot_hyper_account(
    address, account_name="default", outdir_base="../data/prod/account_snaps"
):
    """
    Save a snapshot of a Hyperliquid account as a JSON file with a timestamped filename.

    Args:
        account_name (str): A unique identifier for the account (e.g., "main", "vault").
        outdir_base (str): Base directory for storing snapshots.
    """
    # Timestamp
    now = datetime.now(tz=timezone.utc)
    timestamp = now.strftime("%Y%m%d_%H%M")

    # Query account info
    try:
        resp = requests.post(
            "https://api.hyperliquid.xyz/info",
            json={"type": "clearinghouseState", "user": address},
            headers={"Content-Type": "application/json"},
        )
        resp.raise_for_status()
        data = resp.json()
        data["snap_time"] = now.isoformat()
    except Exception as e:
        print(f"Failed to fetch Hyperliquid account state: {e}")
        return

    # Create path and save
    outdir = os.path.join(outdir_base, account_name)
    os.makedirs(outdir, exist_ok=True)
    fpath = os.path.join(outdir, f"snap_{timestamp}.json")
    with open(fpath, "w") as fh:
        json.dump(data, fh, indent=2)
    print(f"Hyperliquid snapshot saved to {fpath}")


if __name__ == "__main__":
    address = os.environ.get("HEDGER_HYPER_EOA")
    if not address:
        raise ValueError("HEDGER_HYPER_EOA not set in environment.")
    snapshot_hyper_account(address, account_name="hyper_main")
