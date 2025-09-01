import logging
import traceback
import os
import inspect

# Define getargspec in terms of getfullargspec if not available
if not hasattr(inspect, "getargspec"):

    def getargspec(func):
        import warnings

        warnings.warn(
            "inspect.getargspec() is deprecated, use inspect.signature() or inspect.getfullargspec()",
            DeprecationWarning,
            stacklevel=2,
        )
        return inspect.getfullargspec(func)

    inspect.getargspec = getargspec
import eth_account
from hyperliquid.utils.types import Cloid
from eth_account.signers.local import LocalAccount
from .info import Info
from .exchange_async import Exchange


def setup(base_url=None, skip_ws=True):
    account: LocalAccount = eth_account.Account.from_key(os.environ["HYPER_SECRET"])
    print("Running with API account address: %s" % account.address)
    address = os.environ["HYPER_ADDRESS"]
    if address == "":
        address = account.address
    logging.debug(f"Running with account address: {address}")
    if address != account.address:
        logging.debug("Running with agent address: %s" % account.address)
    info = Info(base_url, skip_ws)
    exchange = Exchange(account, base_url, account_address=address)
    return address, info, exchange


class HyperExecAsync:
    tif_alo = {"limit": {"tif": "Alo"}}
    market_order = {"limit": {"tif": "Ioc"}}

    def __init__(
        self,
        ghost_mode=False,
    ):
        self.address, self.info, self.exchange = setup(skip_ws=True)
        self.meta = self.info.meta()
        self.ghost_mode = ghost_mode
        print(f"Hyper exec acting on behalf of {self.address}")

    async def process_order(self, order):
        if order["type"] == "cancel":
            return await self.cancel_orders(order["orders"])
        elif order["type"] == "modify":
            return await self.bulk_modify_orders(order["orders"])
        elif order["type"] == "market_close":
            return await self.exchange.market_close(order["coin"])
        elif order["type"] == "bulk":
            return await self.submit_bulk_order(order["orders"])
        else:
            raise ValueError(f"Unknown order type: {order['type']}")

    async def cancel_orders(self, orders):
        cancels = [
            {"coin": el["coin"], "cloid": Cloid.from_int(el.get("cloid"))}
            for el in orders
        ]
        if self.ghost_mode:
            return {"ghost_mode": True}
        return await self.exchange.bulk_cancel_by_cloid(cancels)

    async def submit_bulk_order(self, orders):
        ors = []
        for order in orders:
            is_buy = order["side"] == "buy"
            data = {
                "coin": order["coin"],
                "is_buy": is_buy,
                "sz": order["qty"],
                "cloid": Cloid.from_int(order.get("cloid")),
            }
            if order["type"] == "market":
                slippage = max(0.0001, order.get("slippage_bps", 1) / 1e4)
                px = self.exchange._slippage_price(
                    order["coin"], is_buy, slippage, px=order["price"]
                )
                data.update(
                    {
                        "limit_px": px,
                        "order_type": self.market_order,
                        "reduce_only": False,
                    }
                )
            else:
                data.update(
                    {
                        "limit_px": order["price"],
                        "order_type": self.tif_alo,
                        "reduce_only": order.get("reduce_only", False),
                    }
                )
            ors.append(data)
        if self.ghost_mode:
            return {"status": "ghost_mode"}
        try:
            return await self.exchange.bulk_orders(ors)
        except Exception as e:
            logging.error(e)
            traceback.print_exc()
            return {}
