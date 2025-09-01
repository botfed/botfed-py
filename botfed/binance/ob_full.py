import logging
from typing import List
import json
from ..core.feed import Feed
from ..core.event_loop import EventLoop
from .order_book import OrderBook
from ..core.websocket_mngr import WebsocketManager


class DepthFeed(Feed, WebsocketManager):
    """Binance Feed"""

    def __init__(self, tickers: List[str]):
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
        streams = "/".join([f"{symbol}@depth" for symbol in self.tickers])
        ws.send(
            json.dumps({"method": "SUBSCRIBE", "params": streams.split("/"), "id": 1})
        )

    def on_message(self, ws, message):
        """On message received from websocket"""
        data = json.loads(message)
        if "stream" in data:
            stream_info = data["stream"]
            event_data = data["data"]
            if event_data["e"] == "depthUpdate":
                try:
                    self.handle_depth_update(event_data)
                except Exception as e:
                    logging.error("Error handling book ticker:", e)

    def handle_depth_update(self, event_data):
        for listener in self.listeners:
            listener.on_l2_update(event_data)


if __name__ == "__main__":
    import sys
    import dotenv

    dotenv.load_dotenv()
    logging.basicConfig(level=logging.INFO)

    try:
        event_loop = EventLoop()
        feed = DepthFeed(["ethusdt", "btcusdt"])
        ob = OrderBook("ETH")
        feed.add_listener(ob)
        event_loop.add_feed(feed)
        event_loop.run()
    except KeyboardInterrupt:
        print("\nExiting on ctrl-c")
        sys.exit(0)
