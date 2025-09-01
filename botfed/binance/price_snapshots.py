import time
import logging
from concurrent.futures import ThreadPoolExecutor
import numpy as np
from typing import Dict
from ..backfill.binance_ohlcv import fetch_ohlcv_ts
from .feed import BinanceListener
from .universe import binance_contract_to_coin, coin_to_binance_contract


class PriceSnapshots(BinanceListener):
    def __init__(self, obs, max_size=24 * 60 * 60, freq_ms=1000):
        self.obs = obs
        self.last_snapshot = {}
        self.snapshots = {}
        self.max_size = max_size
        self.freq_ms = freq_ms
        self.backfill_dfs = {}
        self.coins = [coin for coin in self.obs]
        self.backfill_coins(self.coins)

    def backfill_coins(self, coins):
        logging.info(f"Price Snapper: Backfilling {len(coins)} coins")
        to_ts = time.time() * 1000

        def func(coin):
            from_ts = int(to_ts - 1000 * 60 * 60)
            try:
                ohlcv = fetch_ohlcv_ts(
                    coin_to_binance_contract(coin).upper().replace("USDT", "/USDT"),
                    from_ts,
                    to_ts,
                    interval_min=1,
                )
            except Exception as e:
                logging.error(f"Error fetching {coin}: {e}")
                return []
            return [
                {
                    "mid_price": el[4],
                    "ts": el[0],
                    "r": np.log(el[4] / (ohlcv[idx - 1][4] if idx > 0 else el[1])),
                }
                for idx, el in enumerate(ohlcv)
            ]

        with ThreadPoolExecutor() as exec:
            bf_coins = []
            for coin in coins:
                if coin in self.backfill_dfs:
                    continue
                bf_coins.append(coin)
            results = exec.map(func, bf_coins)
            self.backfill_dfs.update(
                {bf_coins[idx]: result for idx, result in enumerate(results)}
            )
            self.add_snaps({coin: self.backfill_dfs[coin] for coin in bf_coins})

    def on_timer(self):
        tnow = time.time() * 1000
        for coin in self.obs:
            if self.last_snapshot.get(coin, 0) + self.freq_ms > tnow:
                logging.debug("not yet time")
                continue
            if coin not in self.snapshots:
                self.snapshots[coin] = []
            mid_price = self.obs[coin].mid_price()
            tnow = time.time() * 1000
            if mid_price is None:
                continue
            ret = (
                np.log(mid_price / self.snapshots[coin][-1]["mid_price"])
                if self.snapshots[coin]
                else 0
            )
            self.snapshots[coin].append({"ts": tnow, "mid_price": mid_price, "r": ret})
            if len(self.snapshots[coin]) > self.max_size:
                self.snapshots[coin] = self.snapshots[coin][-self.max_size :]

    def add_snaps(self, snaps):
        for coin in snaps:
            if coin not in self.snapshots:
                self.snapshots[coin] = []
            self.snapshots[coin] = sorted(
                snaps[coin] + self.snapshots.get(coin, []), key=lambda x: x["ts"]
            )

    def resample(self, coin: str, freq_sec: int):
        snaps = self.snapshots[coin]
        if len(snaps) == 0:
            return []
        new_snaps = []
        ts = snaps[-1]["ts"]
        mp = snaps[-1]["mid_price"]
        for el in snaps[::-1]:
            ret = np.log(mp / el["mid_price"])
            if ts - el["ts"] >= freq_sec * 1000:
                new_snaps.append({"ts": ts, "mid_price": mp, "r": ret})
                ts = el["ts"]
                mp = el["mid_price"]
                ret = 0
        new_snaps.append({"ts": ts, "mid_price": mp, "r": ret})
        return new_snaps[::-1]

    def resample_all(self, freq_sec: int):
        return {coin: self.resample(coin, freq_sec) for coin in self.snapshots}
