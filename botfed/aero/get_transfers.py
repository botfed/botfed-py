import pandas as pd
import numpy as np
from ..etherscan.get_transfers import get_transfers


def get_funding_transfers(funding_addr: str, manager_addr: str, withdraw_addr: str):
    txs = get_transfers(
        funding_addr
    )  # TODO: get transfers should raise when not fetching / guard against invalid API key
    df = pd.DataFrame(txs)
    df["from"] = df["from"].apply(lambda x: x.lower())
    df["to"] = df["to"].apply(lambda x: x.lower())
    # filter out manager txs
    df = df[(df["to"] != manager_addr.lower()) & (df["from"] != manager_addr.lower())]
    # only transfers to funding withdraw addr survive
    filt = (df["to"] == funding_addr.lower()) | (df["to"] == withdraw_addr.lower())
    df = df[filt]
    df["value"] = np.where(
        df["to"].apply(lambda x: x.lower()) == funding_addr.lower(),
        df["value"],
        -df["value"],
    )
    # divide by usdc decimals
    df["value"] /= 1e6
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    return df


if __name__ == "__main__":
    import os
    import dotenv

    dotenv.load_dotenv()

    funding_addr = os.environ["AERO_MANAGER_GOV"]
    withdraw_addr = os.environ["AERO_WITHDRAW"]
    manager_addr = os.environ["AERO_MANAGER_ADDRESS"]
    df = get_funding_transfers(funding_addr, manager_addr, withdraw_addr)
    print(df)
    print(df["value"].sum())
