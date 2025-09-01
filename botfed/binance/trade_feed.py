import time
import csv
import logging
from typing import List
import json
from ..core.feed import Feed
from ..core.event_loop import EventLoop
from ..core.websocket_mngr import WebsocketManager
from .universe import binance_contract_to_coin


class TradeFeed(Feed, WebsocketManager):
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
        streams = [f"{symbol.lower()}@aggTrade" for symbol in self.tickers]
        ws.send(json.dumps({"method": "SUBSCRIBE", "params": streams, "id": 1}))

    def on_message(self, ws, message):
        """On message received from websocket"""
        msg = json.loads(message)
        if "stream" in msg:
            data = msg["data"]
            data["ts_recv"] = time.time() * 1000
            if data["e"] == "aggTrade":
                self.handle_agg_trade(data)

    def handle_agg_trade(self, event_data):
        for listener in self.listeners:
            listener.on_agg_trade(event_data)


class TradeStore:

    def __init__(self):
        self.trades = {}
        self.listeners = []

    def parse_trade_data(self, data):
        ts_loc = time.time() * 1e3
        coin = binance_contract_to_coin(data["s"])
        ts = data["E"]
        ntl = self.calc_trade_ntl(data)
        return {
            "ts_loc": ts_loc,
            "ts_rem": ts,
            "symbol": data["s"],
            "ntl": ntl,
            "price": data["p"],
            "qty": data["q"],
            "is_buy": not data["m"],
        }

    def add_listener(self, listener):
        self.listeners.append(listener)

    def on_agg_trade(self, event_data):
        coin = binance_contract_to_coin(event_data["s"])
        if coin not in self.trades:
            self.trades[coin] = []
        self.trades[coin].append(event_data)
        if len(self.trades[coin]) > 1000:
            self.trades[coin].pop(0)
        data = self.parse_trade_data(event_data)
        for listener in self.listeners:
            listener.on_agg_trade(data)

    def calc_trade_ntl(self, trade):
        return float(trade["q"]) * (-1 if trade["m"] else 1) * float(trade["p"])

    def get_net_flow(self, coin, since_ms):
        if coin not in self.trades:
            return 0
        net = 0
        ts = self.trades[coin][-1]["E"]
        idx = 0
        while ts > since_ms and idx < len(self.trades[coin]):
            trade = self.trades[coin][-idx]
            net = net + self.calc_trade_ntl(trade)
            idx += 1
            ts = self.trades[coin][-idx]["E"]
        return net


class TradeWriter(TradeStore):

    def __init__(self, outfile):
        self.outfile = outfile
        self.header_written = False
        open(self.outfile, "w").close()
        TradeStore.__init__(self)

    def on_agg_trade(self, event_data):
        rec = self.parse_trade_data(event_data)
        with open(self.outfile, "a") as f:
            writer = csv.DictWriter(f, fieldnames=rec.keys())
            if not self.header_written:
                writer.writeheader()
                self.header_written = True
            writer.writerow(rec)


class TradePrinter(TradeStore):

    def __init__(self, coin):
        self.coin = coin
        TradeStore.__init__(self)

    def on_agg_trade(self, event_data):
        ntl = self.calc_trade_ntl(event_data)
        coin = binance_contract_to_coin(event_data["s"])
        if self.coin and coin != self.coin:
            return
        if abs(ntl) > 5e4:
            print(
                f'Large trade: {ntl} {event_data["s"]} {event_data["p"]} {event_data["q"]} {round(self.calc_trade_ntl(event_data))}'
            )
            tnow = time.time() * 1e3
            print(
                f"Net order flow {coin}: {round(self.get_net_flow(coin, tnow - 60 * 1e3))}"
            )


if __name__ == "__main__":
    import sys
    import argparse
    from ..examples.hyperliquid.tickers import tickers

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--outfile", type=str, help="output file", default="./bin_trades.csv"
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    try:
        event_loop = EventLoop()
        feed = TradeFeed(tickers)
        tw = TradeWriter(args.outfile)
        feed.add_listener(tw)
        event_loop.add_feed(feed)
        event_loop.run()
    except KeyboardInterrupt:
        print("\nExiting on ctrl-c")
        sys.exit(0)
