from typing import Mapping, Dict
from ..core.order_book import OrderBookBase
from .oms import HLOpenOrder
from .info import Info
from hyperliquid.utils.types import Cloid


def local_order_to_hl_order(order) -> HLOpenOrder:
    return HLOpenOrder(
        coin=order["coin"],
        sz=float(order["qty"]),
        side="B" if order["side"] == "buy" else "A",
        px=order["price"],
        limitPx=order["price"],
        cloid=Cloid.from_int(order.get("cloid")).to_raw() if order.get("cloid") else None,
        oid=order.get("oid"),
    )


class SimExchange:

    def __init__(self, obs: Mapping[str, OrderBookBase], cfg: Dict):
        self.info = Info(skip_ws=True)
        self.obs = obs
        self.mock_bids = []
        self.mock_asks = []
        self.listeners_order_resp = []
        self.listeners_user_events = []
        self.oid = 0
        self.acct_bal = cfg.get("acct_bal", 1e4)
        self.init_bal = self.acct_bal
        self.maker_fee = cfg.get("maker_fee", 1e-4)
        self.positions = {}
        self.vlm_traded = 0
        self.total_fees = 0

    def pnl(self):
        return self.acct_bal - self.init_bal + self.unrealized_pnl()

    def add_listener_order_resp(self, listener):
        self.listeners_order_resp.append(listener)

    def add_listener_user_events(self, listener):
        self.listeners_user_events.append(listener)

    def _base_resp(self):
        return {"res": {"status": "ok", "orders": []}}

    def on_order(self, msg):
        resp = None
        if msg["type"] == "modify":
            resp = self.on_modify(msg, key="cloid")
        elif msg["type"] == "bulk":
            resp = self.on_bulk_orders(msg)
        elif msg["type"] == "cancel":
            resp = self.on_cancel(msg)
        elif msg["type"] == "cancel_by_oid":
            resp = self.on_cancel(msg, key="oid")
        else:
            raise ValueError(f"Invalid order type {msg['type']}")
        self.send_resp(resp)

    def send_resp(self, resp):
        for listener in self.listeners_order_resp:
            listener(resp)

    def on_cancel(self, msg, key="cloid") -> Dict:
        resp = self._base_resp()
        resp_orders = resp["res"]["orders"]
        orders = msg["orders"]
        ids = [order[key] for order in orders]
        for order in self.mock_bids:
            if order[key] in ids:
                self.mock_bids.remove(order)
                resp_orders.append(
                    {"cancelled": {"oid": order["oid"], "cloid": order["cloid"]}}
                )
        for order in self.mock_asks:
            if order[key] in ids:
                self.mock_asks.remove(order)
                resp_orders.append(
                    {"cancelled": {"oid": order["oid"], "cloid": order["cloid"]}}
                )

    def on_bulk_orders(self, msg) -> Dict:
        resp = self._base_resp()
        resp_orders = resp["res"]["orders"]
        for order in msg["orders"]:
            self.oid += 1
            order["oid"] = self.oid
            resp_orders.append({"resting": {"oid": self.oid, "cloid": order["cloid"]}})
            if order["side"] == "buy":
                self.mock_bids.append(order)
            elif order["side"] == "sell":
                self.mock_asks.append(order)
            else:
                raise ValueError(f"Invalid side {order['side']}")
        return resp

    def on_modify(self, msg, key="cloid") -> Dict:
        orders = msg["orders"]
        bids = [order for order in orders if order["side"] == "buy"]
        asks = [order for order in orders if order["side"] == "sell"]
        resp = self._base_resp()
        resp_orders = resp["res"]["orders"]
        for bid in bids:
            for order in self.mock_bids:
                if order[key] == bid[key]:
                    self.oid += 1
                    order["price"] = bid["price"]
                    order["qty"] = bid["qty"]
                    order["oid"] = self.oid
                    resp_orders.append(
                        {"resting": {"oid": self.oid, "cloid": order["cloid"]}}
                    )
        for ask in asks:
            for order in self.mock_asks:
                if order[key] == ask[key]:
                    self.oid += 1
                    order["price"] = ask["price"]
                    order["qty"] = ask["qty"]
                    order["oid"] = self.oid
                    resp_orders.append(
                        {"resting": {"oid": self.oid, "cloid": order["cloid"]}}
                    )

    def on_trade(self, trade):
        coin = trade["coin"]
        px = float(trade["px"])
        qty = float(trade["sz"])
        is_buy = trade["side"] == "A"
        if is_buy:
            fills = self.handle_buy(coin, px, qty)
        else:
            fills = self.handle_sell(coin, px, qty)
        if not fills:
            return
        self.on_fills(fills)

    def handle_buy(self, coin, px, qty):
        fills = []
        for ask in self.mock_asks:
            if px < ask["price"] or coin != ask["coin"]:
                continue
            fill = self.make_fill(ask, min(qty, ask["qty"]))
            fills.append(fill)
            qty = max(0, ask["qty"] - qty)
            if qty == 0:
                break
        return fills

    def handle_sell(self, coin, px, qty):
        fills = []
        for bid in self.mock_bids:
            if px > bid["price"] or coin != bid["coin"]:
                continue
            fill = self.make_fill(bid, min(qty, bid["qty"]))
            fills.append(fill)
            qty = max(0, bid["qty"] - qty)
            if qty == 0:
                break
        return fills

    def make_fill(self, mock_order, qty):
        mock_order["sz"] = max(0, mock_order["qty"] - qty)
        self.oid += 1
        mock_order["oid"] = self.oid
        fill = {
            "sz": qty,
            "px": mock_order["price"],
            "coin": mock_order["coin"],
            "oid": self.oid,
            "cloid": Cloid.from_int(mock_order["cloid"]).to_raw(),
            "side": "B" if mock_order["side"] == "buy" else "A",
        }
        return fill

    def get_open_orders(self):
        return [
            local_order_to_hl_order(order) for order in self.mock_bids + self.mock_asks
        ]

    def on_fills(self, fills):
        realized_pnl = 0
        fees = sum([fill["sz"] * fill["px"] * self.maker_fee for fill in fills])
        for fill in fills:
            coin = fill["coin"]
            if coin not in self.positions:
                self.positions[coin] = {"sz": 0, "entryPx": 0}
            qty = abs(fill["sz"])
            pos = self.positions[coin]
            new_sz = pos["sz"] + qty * (1 if fill["side"] == "B" else -1)
            if pos["sz"] > 0 and fill["side"] == "B":
                pos["entryPx"] = (pos["sz"] * pos["entryPx"] + qty * fill["px"]) / (
                    new_sz
                )
            elif pos["sz"] > 0 and fill["side"] == "A":
                realized_pnl += min(abs(pos['sz']), qty) * (fill["px"] - pos["entryPx"])
                if qty > abs(pos['sz']):
                    pos['entryPx'] = fill['px']
            elif pos["sz"] < 0 and fill["side"] == "A":
                pos["entryPx"] = (
                    abs(pos["sz"]) * pos["entryPx"] + qty * fill["px"]
                ) / abs(new_sz)
            elif pos["sz"] < 0 and fill["side"] == "B":
                realized_pnl += min(qty, abs(pos['sz'])) * (pos["entryPx"] - fill["px"])
                if qty > abs(pos['sz']):
                    pos['entryPx'] = fill['px']
            elif pos['sz'] == 0:
                pos['entryPx'] = fill['px']
            pos["sz"] = new_sz
            self.update_orders_on_fill(fill)

        self.acct_bal += realized_pnl - fees
        self.total_fees += fees
        self.update_vlm_traded(fills)

        resp = {"data": {"fills": fills}}
        for listener in self.listeners_user_events:
            listener(resp)

    def update_vlm_traded(self, fills):
        for fill in fills:
            self.vlm_traded += abs(fill["sz"] * fill["px"])

    def update_orders_on_fill(self, fill):
        if fill['side'] == 'A':
            for order in self.mock_asks:
                if order['oid'] == fill['oid']:
                    order['qty'] -= fill['sz']
                if order['qty'] <= 0:
                    self.mock_asks.remove(order)
        elif fill['side'] == 'B':
            for order in self.mock_bids:
                if order['oid'] == fill['oid']:
                    order['qty'] -= fill['sz']
                if order['qty'] <= 0:
                    self.mock_bids.remove(order)

    def unrealized_pnl(self):
        pnl = 0
        for coin, pos in self.positions.items():
            if pos["sz"] > 0:
                pnl += pos["sz"] * (self.obs[coin].mid_price() - pos["entryPx"])
            if pos["sz"] < 0:
                pnl += abs(pos["sz"]) * (pos["entryPx"] - self.obs[coin].mid_price())
        return pnl

    def user_state(self):
        state = {
            "marginSummary": {
                "totalNtlPos": 0,
                "accountValue": self.acct_bal + self.unrealized_pnl(),
            },
            "assetPositions": [
                {
                    "position": {
                        "coin": coin,
                        "szi": pos["sz"],  # can be negative
                        "entryPx": pos["entryPx"],  # 'entryPx': '0.0',
                        "positionValue": pos["sz"] * self.obs[coin].mid_price(),
                    }
                }
                for coin, pos in self.positions.items()
            ],
        }
        state["marginSummary"]["totalNtlPos"] = sum(
            [abs(el["position"]["positionValue"]) for el in state["assetPositions"]]
        )
        return state

    def get_spot_positions(self):
        return {"balances": []}

    def meta(self):
        return self.info.meta()
