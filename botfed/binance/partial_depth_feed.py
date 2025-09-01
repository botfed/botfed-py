import time
from typing import List
import logging
import json
from ..core.feed import Feed
from ..core.websocket_mngr import WebsocketManager


class PartialDepthFeed(Feed, WebsocketManager):
    """Binance Feed"""

    def __init__(self, tickers: List[str], depth=20, update_speed=100, exit_event=None):
        assert depth in [5, 10, 20]
        assert update_speed in [100, 250, 500]
        self.exit_event = exit_event
        self.update_speed = update_speed
        self.depth = depth
        self.tickers = tickers
        self.websocket_url = "wss://fstream.binance.com/stream"
        self.listeners = []
        WebsocketManager.__init__(self, self.websocket_url)

    def add_listener(self, listener):
        """Add listener"""
        self.listeners.append(listener)

    def run_ticks(self):
        pass

    def on_open(self, ws):
        """On open connection"""
        streams = [
            f"{symbol.lower()}@depth{self.depth}@{self.update_speed}ms"
            for symbol in self.tickers
        ]
        ws.send(json.dumps({"method": "SUBSCRIBE", "params": streams, "id": 1}))

    def on_message(self, ws, message):
        """On message received from websocket"""
        data = json.loads(message)
        if "stream" in data:
            event_data = data["data"]
            event_data["ts_recv"] = time.time() * 1000
            if event_data["e"] == "depthUpdate":
                for listener in self.listeners:
                    listener(event_data)
