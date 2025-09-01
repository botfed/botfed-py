from typing import Dict, List
import pandas as pd
import numpy as np
import datetime as dt
from ..core import time
from ..backfill import local_data as ld
from ..statarb import mr
from ..binance.universe import coin_to_binance_contract, binance_contract_to_coin


class KLineStore:

    def __init__(self, N=10000, freq_ms=10 * 1000, exch="bin", hedge_coin="ETH"):
        self.exch = exch
        self.data = {}
        self.freq_ms = freq_ms
        self.last_fire = {}
        self.listeners = []
        self.betas = {}
        self.sigmas = {}
        self.hedge_coin = hedge_coin
        self.N = N

    def add_listener(self, listener):
        self.listeners.append(listener)

    def get_dfs(self):
        return {coin: pd.DataFrame.from_records(self.data[coin]) for coin in self.data}

    def load_historical(self, coins, lookback_days=1):
        edate = dt.datetime.fromtimestamp(time.time(), dt.timezone.utc) - dt.timedelta(
            days=1
        )
        sdate = edate - dt.timedelta(days=lookback_days)
        dfs = ld.build_dfs([coin_to_binance_contract(c) for c in coins], sdate, edate)
        dfs = {binance_contract_to_coin(t): df for t, df in dfs.items()}
        for coin in dfs:
            df = dfs[coin]
            records = df.to_dict(orient="records")
            self.data[coin] = [
                {
                    "T": rec["open_time"],
                    "o": rec["open"],
                    "h": rec["high"],
                    "l": rec["low"],
                    "c": rec["close"],
                    "v": rec["volume"],
                }
                for rec in records
            ]

    def add_coin(self, coin):
        self.data[coin] = []

    def remove_coin(self, coin):
        self.data.pop(coin, None)

    def on_event(self, event):
        if event["e"] == "kline" and event["data"]["exch"] == self.exch:
            symbol = event["data"]["coin"]
            if self.last_fire.get(symbol, 0) + self.freq_ms >= event["timestamp"]:
                return
            if symbol not in self.data:
                self.data[symbol] = []
            data = event["data"]
            data['T'] = event["timestamp"]
            data["o"] = float(data["open"])
            data["c"] = float(data["close"])
            data["h"] = float(data["high"])
            data["l"] = float(data["low"])
            data["v"] = float(data["volume"])
            data["r_oc"] = np.log(1 + (data["c"] - data["o"]) / data["o"])
            self.data[symbol].append(data)
            if len(self.data[symbol]) > self.N:
                self.data[symbol].pop(0)
            self.last_fire[symbol] = data["T"]
            for listener in self.listeners:
                listener.on_kline(symbol, data)

    def compute_stats(self):
        dfs = self.get_dfs()
        dfs_new = {}
        for coin in dfs:
            df = dfs[coin]
            if df.empty or len(df) < 10:
                print(f"Skipping stats for {coin}")
                continue
            df["r"] = np.log(1 + df["c"].pct_change())
            df["sig"] = df["r"].rolling(24 * 60).std()
            self.sigmas[coin] = df["sig"].iloc[-1]
            df = df.dropna()
            dfs_new[coin] = df
        self.betas = mr.compute_betas(dfs_new, sources=[self.hedge_coin])
        for coin in dfs:
            if coin not in self.betas:
                self.betas[coin] = {self.hedge_coin: 1}
