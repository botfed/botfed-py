import logging
from ..hyperliquid.api import HyperLiquidApi


if __name__ == "__main__":
    import sys
    import os
    import argparse
    import dotenv

    dotenv.load_dotenv()

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--outdir",
        type=str,
        default="../data/universe_hyp/",
    )
    parser.add_argument(
        "--minvlm",
        type=int,
        default=int(500),
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

    api = HyperLiquidApi()
    stats = api.get_market_stats()
    stats = [el for el in stats if float(el["dayNtlVlm"]) > args.minvlm * 1000]
    stats = sorted(stats, key=lambda x: float(x["dayNtlVlm"]), reverse=True)
    for stat in stats:
        print(f"{stat['name']},{round(float(stat['dayNtlVlm']))}")
