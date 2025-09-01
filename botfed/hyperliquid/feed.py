import json
import traceback
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from .info import Info
from hyperliquid.utils.types import (
    L2BookMsg,
    Trade,
)

from ..core.feed import Feed
from ..core.websocket_mngr import WebsocketManager
import websocket


class HLTxFeed(Feed, WebsocketManager):
    """Unfortunately, the HyperLiquid API does not provide a way to get all transactions, only the first five per block."""

    def __init__(self):
        self.ws_url = "wss://api.hyperliquid.xyz/ws"
        # self.ws_url = "wss://api-ui.hyperliquid.xyz/ws"
        self.ws = websocket.WebSocketApp(
            self.ws_url,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open,
        )
        self.thread = threading.Thread(target=self.ws.run_forever)
        Feed.__init__(self)

    def start(self):
        self.thread.start()

    def on_open(self, ws):
        subscription_message = json.dumps(
            {"method": "subscribe", "subscription": {"type": "explorerTxs"}}
        )

        # Send the subscription message
        self.ws.send(subscription_message)

    def on_message(self, ws, msg):
        msg["ts_recv"] = time.time() * 1000
        for listener in self.listeners:
            listener.on_all_txs(msg)


class HLStream(Feed):
    pass


class HLPoller(Feed):
    def __init__(self):
        self.info = Info(skip_ws=True)
        self.tx_feed = HLTxFeed()
        Feed.__init__(self)
        self.listeners = {"all_mark_price": [], "trades": [], "l2_book": []}
        self.thread_mark_price = None
        uni = self.info.post("/info", {"type": "metaAndAssetCtxs"})[0]["universe"]
        self.all_coins = [coin["name"] for coin in uni]

    def poll_l2_book(self, coins, freq_ms=50):
        print(f"Subscribing to {len(self.all_coins)} coins l2book")
        threading.Thread(target=self._poll_l2_book, args=(coins, freq_ms)).start()

    def _poll_l2_book(self, coins, freq_ms=50):
        """Turns out to be more granular than the subscription method"""

        def func(coin):
            try:
                data = self.info.l2_snapshot(coin)
                data["ts_recv"] = time.time() * 1000
                self.on_book_update(data)
            except Exception as e:
                print(f"Error: {e}")
                return

        while True:
            if len(coins) == 1:
                func(coins[0])
            else:
                with ThreadPoolExecutor(max_workers=10) as exec:
                    for coin in coins:
                        exec.submit(func, coin)
            time.sleep(freq_ms / 1000)

    def subscribe_all_mark_price(self, listener, poll_freq_sec=1):
        if not self.thread_mark_price:
            self.thread_mark_price = threading.Thread(
                target=self._poll_all_mark_price, args=(poll_freq_sec,)
            )
            self.thread_mark_price.start()
        if listener is not None and listener not in self.listeners["all_mark_price"]:
            self.listeners["all_mark_price"].append(listener)

    def _poll_all_mark_price(self, poll_freq_sec):
        while True:
            try:
                data = self.info.post("/info", {"type": "metaAndAssetCtxs"})
                data = {"data": data}
                data["ts_recv"] = time.time() * 1000
                for listener in self.listeners["all_mark_price"]:
                    listener(data)
                time.sleep(poll_freq_sec)
            except Exception as e:
                traceback.print_exc()
                print(f"Error: {e}")

    def on_book_update(self, msg: L2BookMsg) -> None:
        msg["ts_recv"] = time.time() * 1000
        for listener in self.listeners["l2_book"]:
            listener(msg)


class HLFeed(Feed):
    def __init__(self):
        self.info = Info(skip_ws=False)
        self.tx_feed = HLTxFeed()
        Feed.__init__(self)
        self.listeners = {
            "all_mark_price": [],
            "trades": [],
            "l2_book": [],
            "all_mids": [],
        }
        self.thread_mark_price = None
        uni = self.info.post("/info", {"type": "metaAndAssetCtxs"})[0]["universe"]
        self.all_coins = [coin["name"] for coin in uni]

    def subscribe_all_mark_price(self, listener, poll_freq_sec=1):
        if not self.thread_mark_price:
            self.thread_mark_price = threading.Thread(
                target=self._poll_all_mark_price, args=(poll_freq_sec,)
            )
            self.thread_mark_price.start()
        if listener is not None and listener not in self.listeners["all_mark_price"]:
            self.listeners["all_mark_price"].append(listener)

    def _poll_all_mark_price(self, poll_freq_sec):
        while True:
            data = self.info.post_ws({"type": "metaAndAssetCtxs"})
            data = {"data": data}
            data["ts_recv"] = time.time() * 1000
            for listener in self.listeners["all_mark_price"]:
                listener(data)
            time.sleep(poll_freq_sec)

    def subscribe_l2_book(self, coins=[], listener=None):
        """Unfortunately this one was found to be very slow, at around 600ms per update,
        where as blocks are produced every 300ms or so."""
        if not coins:
            coins = self.all_coins
        print(f"Subscribing to {len(coins)} coins l2book")
        subs = [
            ({"type": "l2Book", "coin": coin}, self.on_book_update) for coin in coins
        ]
        self.info.bulk_subscribe(subs)
        if listener and listener not in self.listeners["l2_book"]:
            self.listeners["l2_book"].append(listener)

    def subscribe_trades(self, coins=[], listener=None):
        """This one gets updated after each block, but doesn't contain bbo information directly."""
        if not coins:
            coins = self.all_coins
        print(f"Subscribing to {len(coins)} coins trades")
        subs = [({"type": "trades", "coin": coin}, self.on_trade) for coin in coins]
        self.info.bulk_subscribe(subs)
        if listener and listener not in self.listeners["trades"]:
            self.listeners["trades"].append(listener)

    def subscribe_all_txs(self):
        self.tx_feed.add_listener(self)
        self.tx_feed.start()

    def subscribe_all_mids(self, listener=None):
        """Unfortunately this one was found to be even slower than the l2 book feed, at about 1 second per update."""
        self.info.subscribe({"type": "allMids"}, self.on_all_mids)
        if listener:
            self.listeners["all_mids"].append(listener)

    def on_book_update(self, msg: L2BookMsg) -> None:
        msg["ts_recv"] = time.time() * 1000
        for listener in self.listeners["l2_book"]:
            listener(msg)

    def on_trade(self, msg: Trade) -> None:
        msg["ts_recv"] = time.time() * 1000
        for listener in self.listeners["trades"]:
            listener(msg)

    def on_all_mids(self, msg) -> None:
        msg["ts_recv"] = time.time() * 1000
        for listener in self.listeners["all_mids"]:
            listener(msg)

    def on_all_txs(self, msg) -> None:
        msg["ts_recv"] = time.time() * 1000
        for listener in self.listeners:
            listener.on_all_txs(msg)
