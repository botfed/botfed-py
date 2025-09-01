import logging
from typing import Dict
from threading import Thread
import requests

from ..core.order_book import OrderBookBase
from .feed import BinanceListener
from .universe import binance_contract_to_coin, coin_to_binance_contract


class OrderBook(OrderBookBase, BinanceListener):
    def __init__(self, coin):
        OrderBookBase.__init__(self, coin)
        self.ticker = coin_to_binance_contract(self.coin).upper()
        self.last_snapshot = None
        self.last_u_id = None
        self.bids = {}
        self.asks = {}
        self.last_event_time_bin = 0
        self.queue = []

    def on_book_ticker(self, ticker: str, bbo: {}):
        if binance_contract_to_coin(ticker) == self.coin:
            self.on_book_update(bbo)

    def _on_book_update(self, book_msg: Dict) -> None:
        """On book update"""
        self.book_data = {
            "bids": [
                {
                    "px": float(book_msg["best_bid"]),
                    "sz": float(book_msg["best_bid_qty"]),
                }
            ],
            "asks": [
                {
                    "px": float(book_msg["best_ask"]),
                    "sz": float(book_msg["best_ask_qty"]),
                }
            ],
            "time": book_msg["time"] * 1000,
        }

    def fetch_snapshot(self):
        def func():
            url = f"https://fapi.binance.com/fapi/v1/depth?symbol={self.ticker}&limit=1000"
            res = requests.get(url)
            snap = res.json()
            try:
                snap["lastUpdateId"]
            except KeyError:
                return
            self.last_snapshot = snap
            self.bids = {}
            self.asks = {}
            for bid in self.last_snapshot["bids"]:
                if float(bid[1]) > 0:
                    self.bids[bid[0]] = bid[1]
            for ask in self.last_snapshot["asks"]:
                if float(ask[1]) > 0:
                    self.asks[ask[0]] = ask[1]
            logging.info("Snapshot received for %s", self.ticker)
            self._close_l2_update()

        thread = Thread(target=func)
        thread.start()

    def _on_l2_update(self, event_data) -> bool:
        if event_data["s"].upper() != self.ticker.upper():
            return
        last = self.last_update_ms()
        self.queue.append(event_data)
        if self.last_snapshot is None:
            self.fetch_snapshot()
            return
        for item in self.queue:
            self._process_item(item)
        self.queue = []
        self._close_l2_update()
        return self.last_update_ms() > last

    def _close_l2_update(self):
        self.book_data["bids"] = sorted(
            [{"px": float(px), "sz": float(qty)} for px, qty in self.bids.items()],
            key=lambda x: x["px"],
            reverse=True,
        )
        self.book_data["asks"] = sorted(
            [{"px": float(px), "sz": float(qty)} for px, qty in self.asks.items()],
            key=lambda x: x["px"],
        )
        self.book_data["time"] = self.last_event_time_bin

    def _process_item(self, event_data):
        if not self.last_u_id:
            if (
                event_data["U"] > self.last_snapshot["lastUpdateId"]
                or event_data["u"] < self.last_snapshot["lastUpdateId"]
            ):
                return
        elif event_data["u"] <= self.last_u_id:
            logging.info("Orderbook stale again")
            self.last_snapshot = None
            return
        for bid in event_data["b"]:
            if float(bid[1]) == 0 and bid[0] in self.bids:
                self.bids.pop(bid[0])
            elif float(bid[1]) > 0:
                self.bids[bid[0]] = bid[1]
        for ask in event_data["a"]:
            if float(ask[1]) == 0 and ask[0] in self.asks:
                self.asks.pop(ask[0])
            elif float(ask[1]) > 0:
                self.asks[ask[0]] = ask[1]
        self.last_u_id = event_data["u"]
        self.last_event_time_bin = float(event_data["E"])
        return True

    @property
    def exchange(self):
        return "binance"
