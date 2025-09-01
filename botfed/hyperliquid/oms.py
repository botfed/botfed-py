import logging
import requests
import time
from typing import Mapping, TypedDict, List
from hyperliquid.utils.types import (
    UserEventsMsg,
)
from ..core.timer_feed import TimerListener
from ..tradeserver.client import TradeClient
from ..core.oms import OMS, LocalOrder, OrderStatus
from ..core.fast_bbo import FastBBO
from ..logger import get_logger

from .hl_interface import HLInterface

from hyperliquid.utils.types import Cloid

logger = get_logger(__name__)


def symbol_to_hl_contract(coin):
    if coin.upper() in ["WETH"]:
        coin = "ETH"
    elif coin.upper() in ["CBBTC", "WBTC", "TBTC"]:
        coin = "BTC"
    elif coin.upper() in ["USUI"]:
        coin = "SUI"
    elif coin.upper() in ["PEPE", "BONK", "SHIB"]:
        coin = "k" + coin.upper()
    return coin


class HLOpenOrder(TypedDict):
    coin: str
    sz: float
    side: str  # 'A' or 'B'
    px: float
    limitPx: float
    cloid: int
    oid: str
    tif: str


def hl_order_to_local_order(order: HLOpenOrder) -> LocalOrder:
    return LocalOrder(
        coin=order["coin"],
        qty=float(order["sz"]),
        side="buy" if order["side"] == "B" else "sell",
        price=float(order["limitPx"]),
        cloid=int(order.get("cloid"), 0) if order.get("cloid") else None,
        oid=order.get("oid"),
    )


def local_order_to_hl_order(order) -> HLOpenOrder:
    return HLOpenOrder(
        coin=order["coin"],
        sz=float(order["qty"]),
        side="B" if order["side"] == "buy" else "A",
        px=order["price"],
        limitPx=order["price"],
        cloid=(
            Cloid.from_int(order.get("cloid")).to_raw() if order.get("cloid") else None
        ),
        oid=order.get("oid"),
        tif="Ioc" if order["type"] == "MARKET" else "Alo",
    )


class HyperOMS(OMS, TimerListener):

    def __init__(
        self,
        tc: TradeClient,
        trade_log_file="./trade.log",
    ):
        self.tc = tc
        self.meta = self.fetch_meta()
        self.universe = {el["name"]: el for el in self.meta["universe"]}
        self.positions = {}
        self.open_orders = []
        self.user_state = {}
        self.trade_log_file = trade_log_file
        OMS.__init__(self)
        self.trade_log = []
        self.spot_positions = {}
        self.listeners = []
        self.orders_cloid: Mapping[int, LocalOrder] = {}
        self.curr_orders = []
        self.cloid = max(list(self.orders_cloid.keys()) + [1])
        self.removed_orders = []
        self._ready = False
        self._last_user_event = 0

    def fetch_meta(self):
        resp = requests.post(
            "https://api.hyperliquid.xyz/info",
            json={"type": "meta"},
            headers={"Content-Type": "application/json"},
        )
        return resp.json()

    def is_tradeable(self, coin):
        return symbol_to_hl_contract(coin) in self.universe

    def from_trade_server(self, data):
        resp = data["resp"]
        orders = data.get("orders")
        if orders is None:
            return
        if "ghost_mode" in resp:
            return
        elif "status" not in resp:
            logging.error(f"Error from trade server: {resp}")
            return
        elif resp["status"] != "ok":
            logging.error(f"Error from trade server: {resp}")
            return
        elif data["type"] == "cancel":
            for idx, order in enumerate(orders):
                status = resp["response"]["data"]["statuses"][idx]
                if "status" == "success":
                    if order["cloid"] in self.orders_cloid:
                        self.remove_cloid(order["cloid"])
            return
        for idx, order in enumerate(orders):
            status = resp["response"]["data"]["statuses"][idx]
            if "error" in status:
                logging.info(status["error"])
                if "Cannot modify" in status["error"]:
                    for listener in self.listeners:
                        listener.on_modify_error(order)
            elif "resting" in status:
                if order["cloid"] in self.orders_cloid:
                    cloid = int(status["resting"]["cloid"], 0)
                    assert cloid == order["cloid"]
                    new_oid = status["resting"]["oid"]
                    old_oid = self.orders_cloid[cloid].get("oid")
                    self.orders_cloid[cloid]["oid"] = (
                        new_oid if old_oid is None or new_oid > old_oid else old_oid
                    )
                    self.orders_cloid[cloid]["status"] = OrderStatus.ACTIVE

    def add_listener(self, listener):
        self.listeners.append(listener)

    def on_user_event(self, data):
        self.on_user_data(data["data"])
        self._ready = True
        self._last_user_event = time.time()

    def get_open_orders(
        self, coin, statuses=[OrderStatus.PENDING_NEW, OrderStatus.ACTIVE]
    ) -> List[LocalOrder]:
        return [
            el
            for el in self.orders_cloid.values()
            if el["coin"] == coin and el.get("status") in statuses
        ]

    def sz_decimals(self, coin):
        for el in self.meta["universe"]:
            if el["name"] == coin:
                return el["szDecimals"]

    def equity(self):
        if self.user_state:
            return float(self.user_state["marginSummary"]["accountValue"])
        else:
            return 0

    def is_stale(self):
        return self._last_user_event < time.time() - 60

    @property
    def ready(self):
        if not self._ready:
            logger.debug("OMS not initialized")
            return False
        if self.is_stale():
            logger.debug("OMS is stale")
            return False
        return True

    def get_position(self, symbol: str):
        coin = symbol_to_hl_contract(symbol)
        return OMS.get_position(self, coin)

    def total_pos_size(self):
        if self.user_state:
            return float(self.user_state["marginSummary"]["totalNtlPos"])
        else:
            return 0

    def leverage(self):
        return self.total_pos_size() / (0.0001 + self.equity())

    def position_delta(self):
        if not self.user_state:
            return None
        delta = 0
        for _, position in self.positions.items():
            pos_value = abs(float(position["positionValue"]))
            size = float(position["szi"])
            delta += pos_value * (1 if size > 0 else -1)
        return delta

    def valid_qty(self, coin, qty):
        decimals = self.sz_decimals(coin)
        return round(qty, decimals)

    def valid_price(self, _, price):
        return round(float(f"{price:.5g}"), 6)

    def on_user_data(self, user_data):
        self.user_state = user_data
        self.account["positions"] = {}
        for position in self.user_state["assetPositions"]:
            pos = position["position"]
            coin = position["position"]["coin"]
            self.account["positions"][coin] = {
                "symbol": coin,
                "qty": float(pos["szi"]),
                "entry_price": float(pos["entryPx"]),
                "unrealized_profit": float(pos["unrealizedPnl"]),
                "notional": float(pos["positionValue"]),
            }

    def on_orders(self, orders):
        self.orders_cloid = {}
        self.curr_orders = orders
        for order in orders:
            if "cloid" in order:
                self.orders_cloid[int(order["cloid"], 0)] = hl_order_to_local_order(
                    order
                )
                self.orders_cloid[int(order["cloid"], 0)]["status"] = OrderStatus.ACTIVE

    def spot_coin(self, coin):
        return coin.split("/")[0]

    def on_user_events(self, user_events: UserEventsMsg) -> None:
        user_events_data = user_events["data"]
        fills = user_events_data.get("fills", [])
        if not fills:
            return
        for fill in fills:
            cloid = int(fill["cloid"], 0)
            order = self.orders_cloid.get(cloid)
            if order is None:
                continue
            order["oid"] = fill["oid"]
            order["filled"] = True
            if float(fill["sz"]) >= float(order["qty"]):
                self.remove_cloid(cloid)
            else:
                order["qty"] = float(order["qty"]) - float(fill["sz"])
            coin = self.spot_coin(fill["coin"])
            if coin in self.spot_positions:
                self.spot_positions[coin] = float(self.spot_positions[coin]) + float(
                    fill["sz"]
                ) * (1 if fill["side"] == "B" else -1)
            elif coin in self.positions:
                self.positions[coin]["szi"] = float(
                    self.positions[coin]["szi"]
                ) + float(fill["sz"]) * (1 if fill["side"] == "B" else -1)
        for listener in self.listeners:
            listener.on_fills(fills)

    def remove_cloid(self, cloid):
        if cloid in self.orders_cloid:
            logging.debug(f"Removing cloid {cloid} from orders_cloid")
            self.removed_orders.append(self.orders_cloid[cloid])
            del self.orders_cloid[cloid]

    def submit_modify(self, args):
        orders = [
            LocalOrder(
                dict(
                    {
                        "coin": arg[0],
                        "qty": arg[1],
                        "side": arg[2],
                        "price": arg[3],
                        "micro_price": arg[4],
                        "type": "limit",
                        "spread": arg[5],
                        "cloid": arg[6]["cloid"],
                        "oid": arg[6].get("oid"),
                    },
                    **arg[7],
                )
            )
            for arg in args
        ]
        for arg in args:
            self.orders_cloid[arg[6]["cloid"]] = arg[6]
            self.orders_cloid[arg[6]["cloid"]]["oid"] = None

        self.tc.submit(
            {
                "type": "modify",
                "exchange": "hl",
                "orders": orders,
            }
        )

    def submit_market_order(
        self, coin: str, qty: float, side: str, price: float, max_slippage=0.01
    ):
        assert side in ["buy", "sell"]
        coin = symbol_to_hl_contract(coin)
        if coin not in self.universe:
            logger.error(f"{coin} not found in hl universe, skipping market order")
            return
        ts = time.time() * 1000
        slippage_factor = (1 + max_slippage) if side == "buy" else (1 - max_slippage)
        self.tc.submit(
            {
                "type": "market",
                "exchange": "hl",
                "ts_oms_send": ts,
                "ts_trigger": ts,
                "order": {
                    "coin": coin,
                    "qty": self.valid_qty(coin, qty),
                    "side": side,
                    "price": (
                        self.valid_price(coin, price * slippage_factor)
                        if price
                        else None
                    ),
                    "slippage": max_slippage,
                },
            }
        )

    def submit_orders(self, orders: List[LocalOrder], extra={}):
        for idx, order in enumerate(orders):
            order["cloid"] = self.cloid + idx
            self.orders_cloid[order["cloid"]] = {**order}
            self.orders_cloid[order["cloid"]]["status"] = OrderStatus.PENDING_NEW
        self.cloid += len(orders)
        self.tc.submit(
            {
                "type": "bulk",
                "exchange": "hl",
                "ts_oms_send": time.time() * 1000,
                "orders": orders,
                **extra,
            }
        )

    def cancel_all(self, coin):
        self.cancel_orders(self.get_open_orders(coin))

    def cancel_orders(self, orders: List[LocalOrder], extra={}):
        if len(orders) == 0:
            return
        self.tc.submit(
            {
                "type": "cancel",
                "exchange": "hl",
                "orders": orders,
                "ts_oms_send": time.time() * 1000,
                **extra,
            },
        )
        for order in orders:
            if order["cloid"] in self.orders_cloid:
                self.orders_cloid[order["cloid"]]["status"] = OrderStatus.PENDING_CANCEL
