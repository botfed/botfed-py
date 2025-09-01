from . import time
import numpy as np
from typing import Dict
from abc import abstractmethod, abstractproperty


class OrderBookBase:
    def __init__(self, coin: str):
        self.coin = coin
        self.book_data = {"bids": [], "asks": [], "time": 0}
        self.listeners = []
        self.last_bbo_snap = 0
        self.bbo_snaps = []
        self.snap_freq_ms = 1000
        self.max_snaps = 1e4
        self.last_update_ms_loc: float = 0

    def add_listener(self, listener):
        self.listeners.append(listener)

    def on_book_update(self, book_msg: Dict) -> None:
        tnow = time.time() * 1000
        self._on_book_update(book_msg)
        self.last_update_ms_loc = tnow
        if self.last_bbo_snap + self.snap_freq_ms <= tnow:
            self.snap_bbo()
            self.last_bbo_snap = tnow
        for listener in self.listeners:
            listener.on_book_update(self.coin, book_msg)

    def on_l2_update(self, depth_msg: Dict) -> None:
        res = self._on_l2_update(depth_msg)
        if res is True:
            for listener in self.listeners:
                listener.on_l2_update(self.coin, depth_msg)

    def snap_bbo(self):
        self.bbo_snaps.append(self.bbo())
        if len(self.bbo_snaps) > self.max_snaps:
            self.bbo_snaps.pop(0)

    @abstractmethod
    def _on_book_update(self, book_msg: Dict) -> None:
        """Book updates"""
        pass

    def bbo(self):
        try:
            best_bid = self.book_data["bids"][0]
            best_bid["px"] = float(best_bid["px"])
            best_bid["sz"] = float(best_bid["sz"])
            best_ask = self.book_data["asks"][0]
            best_ask["px"] = float(best_ask["px"])
            best_ask["sz"] = float(best_ask["sz"])
            return best_bid, best_ask
        except IndexError:
            return None, None

    def last_update_ms(self):
        return self.book_data["time"]

    def mid_price(self):
        best_bid, best_ask = self.bbo()
        if best_bid is None or best_ask is None:
            return None
        return (float(best_bid["px"]) + float(best_ask["px"])) / 2

    def spread(self):
        # spread is in basis points
        best_bid, best_ask = self.bbo()
        if best_bid is None or best_ask is None:
            return None
        return (float(best_ask["px"]) - float(best_bid["px"])) / self.mid_price() * 1e4

    def ret(self, n=1):
        if self.last_bbo_snap + 500 > time.time() * 1000:
            n += 1
        # default two snaps ago or between one and two seconds ...
        if len(self.bbo_snaps) < n:
            return 0
        bbo = self.bbo_snaps[-n]

        return np.log(self.mid_price() / ((bbo[0]["px"] + bbo[1]["px"]) / 2))

    def ofi(self, n=2):
        if len(self.bbo_snaps) < n:
            return 0
        lbbo = self.bbo_snaps[-n]
        cbbo = self.bbo()
        ofi = (
            cbbo[0]["sz"] * (cbbo[0]["px"] > lbbo[0]["px"])
            - lbbo[0]["sz"] * (cbbo[0]["px"] < lbbo[0]["px"])
            - cbbo[1]["sz"] * (cbbo[1]["px"] < lbbo[1]["px"])
            + lbbo[1]["sz"] * (cbbo[1]["px"] > lbbo[1]["px"])
        )
        return ofi

    def spread_mean(self, n=60):
        # spread is in basis points
        bbos = self.bbo_snaps[-n:]
        spreads = sorted(
            [
                2 * 1e4 * (bbo[1]["px"] - bbo[0]["px"]) / (bbo[1]["px"] + bbo[0]["px"])
                for bbo in bbos
            ]
        )
        # err on the side of a larger spread by cutting the bottom 50%
        return np.mean(spreads[len(spreads) // 2 :])

    def ob_imbalance(self, pct=0.01):
        bid_notional, ask_notional = self.liquidity(pct=pct)
        return bid_notional / ask_notional - 1.0

    def obi_ntl(self, pct=0.01):
        bid_notional, ask_notional = self.liquidity(pct=pct)
        return np.log(bid_notional / ask_notional)

    def liq_ntl(self, pct=0.01):
        bids = self.book_data["bids"]
        asks = self.book_data["asks"]
        mid_price = self.mid_price()
        if mid_price is None:
            return None, None
        bid_cut = bids[0]["px"] * (1 - pct)
        bid_notional = sum(
            [float(x["px"]) * float(x["sz"]) for x in bids if float(x["px"]) >= bid_cut]
        )
        ask_cut = asks[0]["px"] * (1 + pct)
        ask_notional = sum(
            [float(x["px"]) * float(x["sz"]) for x in asks if float(x["px"]) <= ask_cut]
        )
        return bid_notional + ask_notional

    def liquidity(self, pct=0.01):
        bids = self.book_data["bids"]
        asks = self.book_data["asks"]
        mid_price = self.mid_price()
        if mid_price is None:
            return None, None
        bid_cut = bids[0]["px"] * (1 - pct)
        bid_notional = sum(
            [float(x["px"]) * float(x["sz"]) for x in bids if float(x["px"]) >= bid_cut]
        )
        ask_cut = asks[0]["px"] * (1 + pct)
        ask_notional = sum(
            [float(x["px"]) * float(x["sz"]) for x in asks if float(x["px"]) <= ask_cut]
        )
        return bid_notional, ask_notional

    def poll(self):
        """Polling loop"""
        pass

    @abstractproperty
    def exchange(self):
        pass
