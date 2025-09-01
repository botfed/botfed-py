import logging
import time
from abc import abstractmethod
from typing import List
import websocket
import json
import threading
from ..core.feed import Feed
from .universe import binance_contract_to_coin
from ..core.websocket_mngr import WebsocketManager
import traceback
import ssl
import os
from ..core.ssl_context import context


#########
PREDEFINED_TICKERS = ["ethusdt", "btcusdt"]
########


class BinanceListener:
    """Dydx listener"""

    @abstractmethod
    def on_book_ticker(self, ticker: str, bbo: {}):
        """Listens to the book ticker feed"""

    @abstractmethod
    def on_kline(self, ticker: str, kline: {}):
        """Listens to the kline feed"""


class BinanceFeed(Feed, WebsocketManager):
    """Binance Feed"""

    def __init__(
        self,
        tickers: List[str],
    ):
        Feed.__init__(self)
        self.tickers = tickers
        self.websocket_url = "wss://fstream.binance.com/stream?streams=" + "/".join(
            [f"{symbol}@bookTicker" for symbol in self.tickers]
        )
        WebsocketManager.__init__(self, self.websocket_url, timeout_threshold=30)

    def on_message(self, ws, message):
        """On message received from websocket"""
        data = json.loads(message)
        if "stream" in data:
            stream_info = data["stream"]
            event_data = data["data"]
            event_data["ts_recv"] = time.time() * 1000
            if event_data["e"] == "bookTicker":
                try:
                    self.handle_book_ticker(stream_info, event_data)
                except Exception as e:
                    logging.error("Error handling book ticker: %s" % e)

    def handle_book_ticker(self, stream_info, event_data):
        symbol = event_data["s"]
        best_bid = float(event_data["b"])
        best_ask = float(event_data["a"])
        best_bid_qty = float(event_data["B"])
        best_ask_qty = float(event_data["A"])
        bbo = {
            "ticker": symbol,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "best_bid_qty": best_bid_qty,
            "best_ask_qty": best_ask_qty,
            "time": event_data["E"] / 1000,
            "ts_recv": event_data["ts_recv"],
        }
        for listener in self.listeners:
            listener.on_book_ticker(symbol, bbo)


class GenericTickerFeed(Feed, WebsocketManager):
    """Binance Feed"""

    def __init__(
        self,
        tickers: List[str],
        stream_type: str,
        stream_params: str = "",
    ):
        Feed.__init__(self)
        assert stream_type in ["bookTicker", "markPrice", "forceOrder", "aggTrade", "depth"]
        self.tickers = tickers
        self.stream_type = stream_type
        self.stream_params = stream_params
        self.websocket_url = "wss://fstream.binance.com/stream"
        WebsocketManager.__init__(self, self.websocket_url, timeout_threshold=30)

    def on_open(self, ws):
        """On open connection"""
        streams = [
            f"{symbol.lower()}@{self.stream_type}{self.stream_params}"
            for symbol in self.tickers
        ]
        ws.send(json.dumps({"method": "SUBSCRIBE", "params": streams, "id": 1}))

    def on_message(self, ws, message):
        """On message received from websocket"""
        data = json.loads(message)
        if "stream" in data:
            event_data = data["data"]
            event_data["ts_recv"] = time.time() * 1000
            for listener in self.listeners:
                listener(event_data)


class BinanceKLineFeed(Feed, WebsocketManager):
    """Binance Feed"""

    def __init__(
        self,
        tickers: List[str],
        interval: str = "1m",
    ):
        self.tickers = tickers
        self.websocket_url = "wss://fstream.binance.com/stream?streams=" + "/".join(
            [f"{symbol}@kline_{interval}" for symbol in self.tickers]
        )
        self.ws = websocket.WebSocketApp(
            self.websocket_url,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )
        self.ws.on_open = self.on_open
        self.thread = threading.Thread(target=self.ws.run_forever)
        self.thread.start()
        self.listeners: List[BinanceListener] = []

    def add_listener(self, listener: BinanceListener):
        """Add listener"""
        self.listeners.append(listener)
        pass

    # def on_open(self, ws):
    #     """On open connection"""
    #     streams = "/".join([f"{symbol}@bookTicker" for symbol in self.tickers])
    #     ws.send(
    #         json.dumps({"method": "SUBSCRIBE", "params": streams.split("/"), "id": 1})
    #     )

    def on_message(self, ws, message):
        """On message received from websocket"""
        data = json.loads(message)
        if "stream" in data:
            stream_info = data["stream"]
            event_data = data["data"]
            if event_data["e"] == "kline":
                try:
                    self.handle_kline(stream_info, event_data)
                except Exception as e:
                    logging.error("Error handling kline %s " % e)
                    traceback.print_exc()

    def handle_kline(self, stream_info, event_data):
        symbol = binance_contract_to_coin(event_data["s"])
        kline = event_data["k"]
        for listener in self.listeners:
            listener.on_kline(symbol, kline)


class BinanceFRFeed(Feed, WebsocketManager):

    def __init__(
        self,
        tickers: List[str],
    ):
        self.tickers = tickers
        self.websocket_url = "wss://fstream.binance.com/stream?streams=" + "/".join(
            [f"{symbol}@markPrice@1s" for symbol in tickers]
        )
        self.ws = websocket.WebSocketApp(
            self.websocket_url,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )
        self.ws.on_open = self.on_open
        self.thread = threading.Thread(target=self.ws.run_forever)
        self.thread.start()
        self.listeners: List[BinanceListener] = []

    def add_listener(self, listener: BinanceListener):
        """Add listener"""
        self.listeners.append(listener)

    def on_message(self, ws, message):
        """On message received from websocket"""
        data = json.loads(message)
        if "stream" in data:
            event_data = data["data"]
            event_data["ts_recv"] = time.time() * 1000
            for listener in self.listeners:
                listener.on_mark_price(event_data)


class BinanceLiquidationFeed(Feed, WebsocketManager):

    def __init__(
        self,
    ):
        self.listeners: List[BinanceListener] = []
        self.websocket_url = "wss://fstream.binance.com/stream?streams=!forceOrder@arr"
        WebsocketManager.__init__(self, self.websocket_url, timeout_threshold=60, warn_threshold=30)

    def add_listener(self, listener: BinanceListener):
        """Add listener"""
        self.listeners.append(listener)

    def on_message(self, ws, message):
        """On message received from websocket"""
        data = json.loads(message)
        if "stream" in data:
            event_data = data["data"]
            event_data["ts_recv"] = time.time() * 1000
            if event_data["e"] == "forceOrder":
                for listener in self.listeners:
                    listener(event_data)
