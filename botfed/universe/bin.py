import pandas as pd
import json
import datetime as dt
import logging
import requests
from ..backfill.binance_markets import get_bin_uni


OUTDIR = "../data/universe_bin/"


def load_uni(type_="perps", outdir=OUTDIR):
    return pd.read_csv(f"{outdir}/{type_}_latest.csv")


def load_uni_perps_top_N_usdt(N, outdir=OUTDIR):
    perps = load_uni("perps", outdir)
    perps = perps[perps["id"].str.endswith("USDT")]
    perps = perps.sort_values("volume", ascending=False)
    return perps["id"].unique().tolist()[0:N]


def fetch_24hr_spot_stats():
    resp = requests.get("https://api.binance.com/api/v3/ticker/24hr")
    return resp.json()


def fetch_24hr_perp_stats():
    resp = requests.get("https://fapi.binance.com/fapi/v1/ticker/24hr")
    return resp.json()


def save_perps(rows, outdir, perp_stats):
    keys = ["id", "symbol", "taker", "maker", "base", "quote", "fundingIntervalHours"]
    stats = [{key: el[key] for key in keys} for el in rows]
    stats = [el for el in stats if el["id"] in perp_stats]
    for stat in stats:
        stat["volume"] = float(perp_stats[stat["id"]]["volume"])
        stat["quote_volume"] = float(perp_stats[stat["id"]]["quoteVolume"])
        stat["price"] = float(perp_stats[stat["id"]]["lastPrice"])
        stat["notional"] = stat["volume"] * stat["price"]
    date = dt.datetime.now().strftime("%Y%m%d")
    df = pd.DataFrame.from_records(stats)
    fname = f"perps_{date}.csv"
    latest_name = "perps_latest.csv"
    outfile = os.path.join(outdir, fname)
    if not os.path.exists(os.path.dirname(outfile)):
        os.makedirs(os.path.dirname(outfile))
    df.to_csv(outfile, index=True)
    if os.path.exists(os.path.join(outdir, latest_name)):
        os.remove(os.path.join(outdir, latest_name))
    os.symlink(fname, os.path.join(outdir, latest_name))


def save_spot(rows, outdir, spot_stats):
    keys = [
        "id",
        "symbol",
        "taker",
        "maker",
        "base",
        "quote",
    ]
    stats = [{key: el[key] for key in keys} for el in rows]
    for stat in stats:
        stat["volume"] = float(spot_stats[stat["id"]]["volume"])
        stat["quote_volume"] = float(spot_stats[stat["id"]]["quoteVolume"])
        stat["price"] = float(spot_stats[stat["id"]]["lastPrice"])
        stat["notional"] = stat["volume"] * stat["price"]
    date = dt.datetime.now().strftime("%Y%m%d")
    df = pd.DataFrame.from_records(stats)
    fname = f"spot_{date}.csv"
    latest_name = "spot_latest.csv"
    outfile = os.path.join(outdir, fname)
    if not os.path.exists(os.path.dirname(outfile)):
        os.makedirs(os.path.dirname(outfile))
    df.to_csv(outfile, index=True)
    if os.path.exists(os.path.join(outdir, latest_name)):
        os.remove(os.path.join(outdir, latest_name))
    os.symlink(fname, os.path.join(outdir, latest_name))


def get_funding_adj():
    resp = requests.get("https://fapi.binance.com/fapi/v1/fundingInfo")
    return resp.json()


if __name__ == "__main__":
    import os
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--outdir",
        type=str,
        default=OUTDIR,
    )
    parser.add_argument(
        "--minvlm",
        type=int,
        default=int(1),
        help="Minimum volume to consider (1000s USD)",
    )
    parser.add_argument(
        "-d",
        "--debug",
        help="Print lots of debugging statements",
        action="store_const",
        dest="loglevel",
        const=logging.DEBUG,
        default=logging.WARNING,
    )
    parser.add_argument(
        "-v",
        "--verbose",
        help="Be verbose",
        action="store_const",
        dest="loglevel",
        const=logging.INFO,
    )
    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel)

    bin_uni = get_bin_uni()
    adj = get_funding_adj()
    perps = bin_uni["perps"]
    for perp in perps:
        perp["fundingIntervalHours"] = 8
        for ad in adj:
            if perp["id"] == ad["symbol"]:
                perp["fundingIntervalHours"] = ad["fundingIntervalHours"]
                break
    perp_stats = fetch_24hr_perp_stats()
    perp_stats = {stats["symbol"]: stats for stats in perp_stats}
    save_perps(perps, args.outdir, perp_stats)
    spot_stats = fetch_24hr_spot_stats()
    spot_stats = {stats["symbol"]: stats for stats in spot_stats}
    save_spot(bin_uni["spot_all"], args.outdir, spot_stats)


import ccxt


def get_all_spot_usdt():
    # Initialize the Binance exchange
    exchange = ccxt.binance()

    # Fetch all markets
    markets = exchange.load_markets()

    # Define a list of stablecoins
    stablecoins = ["USDT", "BUSD", "USDC", "DAI", "TUSD", "PAX", "GUSD", "UST", "HUSD"]

    # Filter spot pairs quoted in USDT and not a stablecoin itself
    usdt_spot_pairs = [
        symbol
        for symbol in markets
        if markets[symbol]["type"] == "spot"
        and symbol.endswith("/USDT")
        and markets[symbol]["base"] not in stablecoins
    ]

    # Print the results
    for symbol in usdt_spot_pairs:
        print(symbol.split("/")[0])

    # Alternatively, you can store them in a list
    usdt_spot_pairs_list = list(usdt_spot_pairs)
    print(f"Got total coins {len(usdt_spot_pairs_list)}")


def get_all_perps_usdt(outdir):
    # Initialize the Binance futures exchange
    exchange = ccxt.binance(
        {
            "options": {
                "defaultType": "future",
            },
        }
    )

    # Fetch all markets
    markets = exchange.load_markets()

    # print(json.dumps(markets, indent=2))

    # Filter perpetual contracts quoted in USDT
    usdt_perps = [
        (symbol.split(":")[0], item)
        for symbol, item in markets.items()
        if markets[symbol]["info"].get("contractType", None) == "PERPETUAL"
        and symbol.endswith(":USDT")
    ]
    print(f"Got total coins {len(usdt_perps)}")
    keys = ["id", "base", "quote", "created"]
    stats = [
        {key: el[key] for key in keys} | {"symbol": symbol} for symbol, el in usdt_perps
    ]
    date = dt.datetime.now().strftime("%Y%m%d")
    df = pd.DataFrame.from_records(stats)
    df = df.set_index("id")
    fname = f"perps_{date}.csv"
    latest_name = "perps_latest.csv"
    outfile = os.path.join(outdir, fname)
    if not os.path.exists(os.path.dirname(outfile)):
        os.makedirs(os.path.dirname(outfile))
    df.to_csv(outfile, index=True)
    if os.path.exists(os.path.join(outdir, latest_name)):
        os.remove(os.path.join(outdir, latest_name))
    os.symlink(fname, os.path.join(outdir, latest_name))


if __name__ == "__main__":
    # get_all_spot_usdt()
    get_all_perps_usdt(OUTDIR)
