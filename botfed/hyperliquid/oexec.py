import logging
import traceback
from hyperliquid.utils.types import Cloid
from .hl_interface import setup


class HyperExec:
    tif_alo = {"limit": {"tif": "Alo"}}
    tif_ioc = {"limit": {"tif": "Ioc"}}
    market_order = {"limit": {"tif": "Ioc"}}

    def __init__(
        self,
        eoa: str,
        secret: str,
        ghost_mode=False,
    ):
        self.address, self.info, self.exchange = setup(eoa, secret, skip_ws=True)
        print(f"Hyper exec acting on behalf of {self.address}")
        self.meta = self.info.meta()
        self.ghost_mode = ghost_mode

    def bulk_modify_orders(self, orders):
        orders = [
            {
                "order": {
                    "coin": el["coin"],
                    "is_buy": el["side"] == "buy",
                    "sz": el["qty"],
                    "limit_px": el["price"],
                    "order_type": self.tif_alo,
                    "reduce_only": el.get("reduce_only", False),
                    "cloid": Cloid.from_int(el.get("cloid")),
                },
                "oid": el["oid"],
            }
            for el in orders
        ]
        if self.ghost_mode:
            return {"ghost_mode": True}
        return self.exchange.bulk_modify_orders_new(orders)

    def cancel_orders(self, orders):
        cancels = [
            {"coin": el["coin"], "cloid": Cloid.from_int(el.get("cloid"))}
            for el in orders
        ]
        if self.ghost_mode:
            return {"ghost_mode": True}
        return self.exchange.bulk_cancel_by_cloid(cancels)

    def process_order(self, order):
        if order["type"] == "cancel":
            return self.cancel_orders(order["orders"])
        elif order["type"] == "market":
            return self.market_open(order["order"])
        elif order["type"] == "modify":
            return self.bulk_modify_orders(order["orders"])
        elif order["type"] == "market_close":
            return self.exchange.market_close(order["coin"])
        elif order["type"] == "bulk":
            result = self.submit_bulk_order(order["orders"])
        else:
            raise ValueError(f"Unknown order type: {order['type']}")
        # coins = [el["coin"] for el in order["orders"]]
        # all_mids = self.info.all_mids()
        # result["prices_post"] = {coin: all_mids[coin] for coin in coins}
        return result

    def market_open(self, order):
        try:
            return self.exchange.market_open(
                order["coin"],
                order["side"] == "buy",
                order["qty"],
                px=order["price"],
                slippage=max(0.0001, order.get("slippage", 0)),
                cloid=Cloid.from_int(order["cloid"]) if "cloid" in order else None,
            )
        except Exception as e:
            return {"error": e}

    def submit_bulk_order(self, orders):
        ors = []
        for order in orders:
            data = {
                "coin": order["coin"],
                "is_buy": order["side"] == "buy",
                "sz": order["qty"],
                "cloid": Cloid.from_int(order.get("cloid")),
                "limit_px": order["price"],
                "order_type": self.tif_alo,
                "reduce_only": order.get("reduce_only", False),
            }
            ors.append(data)
        if self.ghost_mode:
            return {"ghost_mode": True}
        try:
            return self.exchange.bulk_orders(ors)
        except Exception as e:
            logging.error(e)
            traceback.print_exc()
            return {}
