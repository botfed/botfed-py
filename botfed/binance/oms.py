import os
import requests
import math
import json
import time
import uuid
import threading
import queue
from typing import Mapping, List
from ..core.timer_feed import TimerListener
from ..core.oms import OMS, LocalOrder, OrderStatus
from ..core.fast_bbo import FastBBO
from ..tradeserver.client import TradeClient
from ..logger import get_logger
from .universe import coin_to_binance_contract

logger = get_logger(__name__)


def get_exchange_info():
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()
    return data


def get_precision(symbol, data):

    for s in data["symbols"]:
        if s["symbol"] == symbol:
            for f in s["filters"]:
                if f["filterType"] == "LOT_SIZE":
                    step_size = float(f["stepSize"])
                    precision = abs(int(round(math.log10(step_size))))
                    return precision

    raise ValueError(f"Symbol {symbol} not found.")


def bin_status_to_local(status):
    if status == "NEW":
        return OrderStatus.ACTIVE
    elif status == "PARTIALLY_FILLED":
        return OrderStatus.PARTIALLY_FILLED
    elif status == "FILLED":
        return OrderStatus.FILLED
    elif status == "CANCELED":
        return OrderStatus.CANCELED
    elif status == "REJECTED":
        return OrderStatus.REJECTED
    elif status == "EXPIRED":
        return OrderStatus.EXPIRED
    else:
        logger.warning(f"Unknown status: {status}")
        return status


def bin_to_local_order(data) -> LocalOrder:
    if "s" in data:
        return LocalOrder(
            {
                "coin": data["s"],
                "qty": float(data["q"]),
                "side": "buy" if data["S"] == "BUY" else "sell",
                "price": float(data["p"]),
                "oid": data["i"],
                "cloid": data["c"],
                "status": bin_status_to_local(data["X"]),
                "ts_trigger": time.time(),
                "exch_update_time": data["T"],
                "type": "limit",
            }
        )
    elif "symbol" in data:
        return LocalOrder(
            {
                "coin": data["symbol"],
                "qty": float(data["origQty"]),
                "side": "buy" if data["side"] == "BUY" else "sell",
                "price": float(data["price"]),
                "oid": data["orderId"],
                "cloid": data["clientOrderId"],
                "status": bin_status_to_local(data["status"]),
                "ts_trigger": time.time(),
                "exch_update_time": data["updateTime"],
                "type": "limit",
            }
        )
    else:
        raise ValueError(f"Unknown order data: {data}")


class BinOMS(OMS, TimerListener):
    BASE_URL = "https://fapi.binance.com"

    def __init__(
        self,
        tc: TradeClient,
        stop_event,
        state_file="../data/prod/bin_account.json",
    ):
        self.tc = tc
        self.tc.add_listener(self.from_trade_server)
        self.positions = {}
        self.symbol_info = self.get_exchange_info()
        OMS.__init__(self)
        self.listeners = []
        self.orders_cloid: Mapping[str, LocalOrder] = {}
        self.removed_orders = []
        self.state_file = state_file
        self.queue = queue.Queue()
        self.stop_event = stop_event
        self.thread = threading.Thread(target=self.record_state)
        self.thread.start()
        self.exchange_info = get_exchange_info()

    def next_cloid(self):
        return str(uuid.uuid4()).replace("-", "")[:22]

    def from_trade_server(self, data):
        # logger.info(f"BinOMS: From trade server: {data}")
        if "resp" in data and data["resp"] == "dropped":
            for order in data["order"]["orders"]:
                del self.orders_cloid[order["cloid"]]

        # if data.get("error", None) is not None:
        #     for order in data["orders"]:
        #         if order["cloid"] in self.orders_cloid:
        #             self.orders_cloid[order["cloid"]]["status"] = OrderStatus.REJECTED

    def add_listener(self, listener):
        self.listeners.append(listener)

    def get_position(self, coin: str):
        symbol = coin_to_binance_contract(coin)
        if self.account['ready']:
            return self.account['positions'].get(symbol, {})
        else:
            raise Exception("Account not ready")

    def on_user_event(self, data):
        if data["type"] == "bin_account_info":
            self.account["positions"] = {
                pos["symbol"]: {
                    "symbol": pos["symbol"],
                    "qty": float(pos["positionAmt"]),
                    "entry_price": float(pos["entryPrice"]),
                    "notional": float(pos["notional"]),
                    "unrealized_profit": float(pos["unrealizedProfit"]),
                }
                for pos in data["data"]["positions"]
            }
            self.account["assets"] = {
                el["asset"]: {
                    "wallet_balance": float(el["walletBalance"]),
                    "available_balance": float(el["availableBalance"]),
                }
                for el in data["data"]["assets"]
            }
            self.account['ready'] = True
        elif data["type"] == "bin_user_event":
            if data["data"]["e"] == "ORDER_TRADE_UPDATE":
                self.on_bin_order_update(data["data"])
        elif data["type"] == "bin_open_orders":
            created_at = {
                cloid: order.get("created_at")
                for cloid, order in self.orders_cloid.items()
            }
            self.orders_cloid = {}
            for el in data["data"]:
                cloid = el["clientOrderId"]
                self.orders_cloid[cloid] = bin_to_local_order(el)
                if cloid in created_at:
                    self.orders_cloid[cloid]["created_at"] = created_at[cloid]

    def on_bin_order_update(self, data):
        cloid = data["o"]["c"]
        created_at = self.orders_cloid.get(cloid, {}).get("created_at", 0)
        self.orders_cloid[cloid] = bin_to_local_order(data["o"])
        self.orders_cloid[cloid]["created_at"] = created_at
        if data["o"]["X"] in ["FILLED", "CANCELED"]:
            del self.orders_cloid[cloid]

    @property
    def ready(self):
        return self.account.equity() != 0

    def get_exchange_info(self):
        """Fetch exchange info and extract precision rules for all symbols."""
        url = f"{self.BASE_URL}/fapi/v1/exchangeInfo"
        response = requests.get(url)
        response.raise_for_status()  # Ensure request succeeded
        exchange_info = response.json()
        symbol_info = {}
        for symbol in exchange_info["symbols"]:
            symbol_info[symbol["symbol"]] = {
                "pricePrecision": symbol["pricePrecision"],
                "quantityPrecision": symbol["quantityPrecision"],
                "minPrice": float(symbol["filters"][0]["minPrice"]),
                "tickSize": float(symbol["filters"][0]["tickSize"]),
                "minQty": float(symbol["filters"][2]["minQty"]),
                "stepSize": float(symbol["filters"][2]["stepSize"]),
            }
        return symbol_info

    def valid_price(self, symbol: str, price: float):
        """Ensure the price conforms to the tick size of the symbol."""
        tick_size = self.symbol_info[symbol]["tickSize"]
        valid_price = math.floor(price / tick_size) * tick_size
        return float(f"{valid_price:.8f}")  # Ensure precision for API

    def valid_qty(self, symbol: int, qty: float):
        """Ensure the quantity conforms to the step size of the symbol."""
        step_size = self.symbol_info[symbol]["stepSize"]
        min_qty = self.symbol_info[symbol]["minQty"]
        # Ensure the quantity is above the minimum quantity
        if qty < min_qty:
            return 0
        valid_qty = math.floor(qty / step_size) * step_size
        return float(f"{valid_qty:.8f}")  # Ensure precision for API

    def get_open_orders(
        self,
        coin,
        statuses=[
            OrderStatus.PENDING_NEW,
            OrderStatus.ACTIVE,
            OrderStatus.PARTIALLY_FILLED,
        ],
    ) -> List[LocalOrder]:
        return [
            el
            for el in self.orders_cloid.values()
            if el["coin"] == coin and el.get("status") in statuses
        ]

    def submit_orders(self, orders: List[LocalOrder], extra={}):
        for order in orders:
            order["cloid"] = self.next_cloid()
            self.orders_cloid[order["cloid"]] = {**order}
            self.orders_cloid[order["cloid"]]["status"] = OrderStatus.PENDING_NEW
        i = 0
        while i < len(orders):
            self.tc.submit(
                {
                    "type": "bulk",
                    "exchange": "bin",
                    "ts_oms_send": time.time() * 1000,
                    "orders": orders[i : i + 5],
                    **extra,
                }
            )
            i += 5

    def _submit_market_order(
        self, symbol, qty, side, price=None, slippage_bps=1, extra={}
    ):
        precision = get_precision(symbol, self.exchange_info)
        self.tc.submit(
            {
                "type": "market",
                "exchange": "bin",
                "ts_oms_send": time.time() * 1000,
                "orders": [
                    {
                        "coin": symbol,
                        "qty": round(qty, precision),
                        "side": side,
                    }
                ],
            }
        )

    def cancel_orders(self, orders: List[LocalOrder], extra={}):
        if len(orders) == 0:
            return
        for order in orders:
            self.tc.submit(
                {
                    "type": "cancel",
                    "exchange": "bin",
                    "orders": [order],
                    "ts_oms_send": time.time() * 1000,
                    **extra,
                },
            )
        for order in orders:
            if order["cloid"] in self.orders_cloid:
                self.orders_cloid[order["cloid"]]["status"] = OrderStatus.PENDING_CANCEL

    def record_state(self, sleep_ms=1000):
        if not self.state_file:
            return
        # get state file base dir if not none and create if not exists
        if not os.path.exists(os.path.dirname(self.state_file)):
            os.makedirs(os.path.dirname(self.state_file))
        while not self.stop_event.is_set():
            time.sleep(sleep_ms / 1000)
            with open(self.state_file, "w") as f:
                f.write(
                    json.dumps(
                        {
                            "account": self.account,
                            "positions": self.account.get("positions", {}),
                            "open_orders": self.orders_cloid or {},
                        },
                        indent=2,
                    )
                )
        logger.info("OMS: record state stopped.")

    def __del__(self):
        if self.thread:
            self.thread.join()
        logger.info("BinOMS deleted.")
