import time
from .universe import binance_contract_to_coin


class LiqStore:

    def __init__(self):
        self.store = {}

    def on_liquidations(self, data):
        ts_loc = time.time() * 1000
        notional = float(data["o"]["q"]) * float(data["o"]["p"])
        is_buy = data["o"]["S"].upper() == "BUY"
        coin = binance_contract_to_coin(data["o"]["s"])

        if coin not in self.store:
            self.store[coin] = []

        self.store[coin].append(
            {"ts_loc": ts_loc, "notional": notional, "is_buy": is_buy}
        )

    def get_net_liquidations(self, coin, since=0):
        if coin not in self.store:
            return {"notional": 0, "is_buy": True, "ts_loc": 0, "num": 0}

        liqs = [x for x in self.store[coin] if x["ts_loc"] > since]
        ntl = sum(x["notional"] * (1 if x["is_buy"] else -1) for x in liqs)
        return {
            "notional": abs(ntl),
            "is_buy": ntl > 0,
            "ts_loc": time.time() * 1000,
            "num": len(liqs),
        }
