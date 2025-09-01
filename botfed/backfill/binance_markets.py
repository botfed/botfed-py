import logging
import json
import os
import ccxt


def get_bin_uni():
    ex = ccxt.binance()
    markets = ex.fetch_markets()
    markets = [m for m in markets if m["active"] is True]
    perps = [
        m
        for m in markets
        if m["contract"] is True and m["info"]["contractType"] == "PERPETUAL"
    ]
    perps = [m for m in markets if m["id"][-4:] != "PERP"]
    spot_all = [m for m in markets if m["spot"] is True]
    spot_usdt = [m for m in spot_all if m["id"][-4:] == "USDT"]
    print(json.dumps(spot_usdt[231], indent=2))
    print(json.dumps(perps[231], indent=2))
    return {"perps": perps, "spot_usdt": spot_usdt, "spot_all": spot_all}


def get_bin_uni_local(rootdir):
    return json.load(open(os.path.join(rootdir, "binance_uni.json")))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--rootdir", type=str, help="output dir", default=".")
    parser.add_argument(
        "--minmc", type=float, help="minimum market cap (millions)", default=1
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
    data = get_bin_uni()
    outfile = os.path.join(args.rootdir, "binance_uni.json")
    json.dump(data, open(outfile, "w"))
