import time

class FastBBO:

    def __init__(self, ticker_converter):
        self.ticker_converter = ticker_converter
        self.bbo = {}
        self.last_bbo = {}
        self.listeners_price = []
        self.listeners_any = []

    def add_listener_any(self, listener):
        self.listeners_any.append(listener)

    def add_listener_price(self, listener):
        self.listeners_price.append(listener)

    def get_bbo(self, ticker):
        return self.bbo.get(ticker)

    def get_last_bbo(self, ticker):
        return self.last_bbo.get(ticker)

    def last_update_ms(self, ticker):
        return (self.get_bbo(ticker) or {}).get("ts_recv", 0)

    def spread_bps(self, ticker):
        bbo = self.get_bbo(ticker)
        if bbo:
            return (bbo["a"] - bbo["b"]) / self.mid_price(ticker) * 1e4
        return None

    def mid_price(self, ticker):
        bbo = self.get_bbo(ticker)
        if bbo:
            return (bbo["b"] + bbo["a"]) / 2
        return None

    def _parse_msg(self, data):
        return self.ticker_converter(data["s"]), {
            "b": float(data["b"]),
            "a": float(data["a"]),
            "bq": float(data["B"]),
            "aq": float(data["A"]),
            "ts_recv": float(data["ts_recv"]),
            "ts_feed_put": float(data["ts_feed_put"]),
            "exch_ts": float(data["T"]),
            "exch_seq": data.get("u"),
        }

    def on_book_update(self, data):
        symbol, bbo = self._parse_msg(data)
        self.last_bbo[symbol] = self.get_bbo(symbol)
        self.bbo[symbol] = bbo
        tnow = time.time() * 1000
        if bbo and tnow - bbo["ts_recv"] > 100:
            print(f"Stale data {symbol} {tnow - bbo['ts_recv']}")   
        if self.listeners_price:
            if self.get_last_bbo(symbol) and (
                self.last_bbo[symbol]["a"] != self.bbo[symbol]["a"]
                or self.last_bbo[symbol]["b"] != self.bbo[symbol]["b"]
            ):
                for listener in self.listeners_price:
                    listener(symbol)
        if self.listeners_any:
            for listener in self.listeners_any:
                listener(symbol)
