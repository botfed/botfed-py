from typing import List
import datetime as dt
import functools
import requests

from ..statarb.sim_account import (
    AccountBase,
    Position,
    MarketOrder,
    OrderFill,
    LimitOrder,
)
from ..binance.universe import binance_contract_to_coin
from ..logger import get_logger
from ..statarb.universe import hl_symbol_to_uni_symbol, uni_symbol_to_coin
from ..core import time
from .hl_interface import setup


def with_retry(max_retries=3, delay=1.0, backoff=2.0):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            current_delay = delay
            while True:
                try:
                    return func(*args, **kwargs)
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 429:
                        if retries < max_retries:
                            time.sleep(current_delay)
                            retries += 1
                            current_delay *= backoff
                        else:
                            raise
                    else:
                        raise

        return wrapper

    return decorator


logger = get_logger(__name__)


def hl_coin_to_uni_symbol(coin: str):
    s = coin
    if s[1] == "k":
        s = "1000" + s[1:]
    s += "_USDT"
    return s


def hl_order_to_limit_order(o: {}) -> LimitOrder:
    return LimitOrder(
        symbol=hl_coin_to_uni_symbol(o["coin"]),
        size=float(o["sz"]),
        orig_size=float(o["origSz"]),
        side="BUY" if o["side"] == "B" else "SELL",
        price=float(o["limitPx"]),
        oid=o["oid"],
        created_at=dt.datetime.fromtimestamp(o["timestamp"] / 1e3, tz=dt.timezone.utc),
    )


class HLOrderUtil:

    def __init__(self, info):
        self.info = info
        self.meta = self.info.meta()

    def sz_decimals(self, coin):
        for el in self.meta["universe"]:
            if el["name"] == coin:
                return el["szDecimals"]

    def valid_qty(self, coin, qty):
        decimals = self.sz_decimals(coin)
        return round(qty, decimals)

    def valid_price(self, _, price):
        return round(float(f"{price:.5g}"), 6)


class HyperStatArbAccount(AccountBase):
    tif_alo = {"limit": {"tif": "Alo"}}
    tif_gtc = {"limit": {"tif": "Gtc"}}
    tif_ioc = {"limit": {"tif": "Ioc"}}

    def __init__(self, alerter=None, ghost_mode=False):
        self.address, self.info, self.exchange = setup()
        self._equity: float = 0
        self.hl_util = HLOrderUtil(self.info)
        self.alerter = alerter
        self.last_update = time.time() * 1000
        self.ghost_mode = ghost_mode
        self.open_orders: List[LimitOrder] = []
        AccountBase.__init__(self)

    def equity(self) -> float:
        return self._equity

    def min_order_ntl(self, symbol):
        return 11

    @with_retry()
    def fetch_user_state(self):
        return self.info.user_state(self.address)

    @with_retry()
    def fetch_open_orders(self):
        return self.info.open_orders(self.address)

    def update(self) -> None:
        user_state = self.fetch_user_state()
        open_orders = self.fetch_open_orders()
        self.positions = {}
        self._equity = float(user_state["marginSummary"]["accountValue"])
        self._total_ntl_pos = float(user_state["marginSummary"]["totalNtlPos"])
        self.balance = self._equity - self._total_ntl_pos
        self.limit_orders = {o["oid"]: hl_order_to_limit_order(o) for o in open_orders}
        for item in user_state["assetPositions"]:
            position = item["position"]
            symbol = hl_symbol_to_uni_symbol(position["coin"])
            pos = Position(symbol=symbol)
            pos.size = float(position["szi"])
            pos.entry_price = float(position["entryPx"])
            self.positions[symbol] = pos
            logger.debug(f"Account update position {pos}")
        fills = self.info.user_fills(self.address)
        fills = [fill for fill in fills if fill["time"] > self.last_update]
        for fill in fills:
            msg = f"Alert: {'BUY' if fill['side'].upper()[0] == 'B' else 'SELL'} ${float(fill['sz']) * float(fill['px']):.2f} of {fill['coin']} @ ${fill['px']}"
            logger.debug(msg)
            if self.alerter:
                self.alerter.send_alert(msg)
        self.last_update = time.time() * 1e3

        self.equity_curve.append(
            {
                "timestamp": self.last_update,
                "equity": self.equity(),
                "borrow_cost": float(self.total_borrow_cost),
                "tx_cost": float(self.total_tx_cost),
                "leverage": self.leverage(),
                "net_notional": self.net_notional() / self.equity(),
            }
        )

        logger.debug(
            f"Account update: Equity=${self._equity:.2f}, NtlPosSize=${self._total_ntl_pos:.2f}, Balance=${self.balance:.2f}"
        )

    def cancel_all_orders(self, symbol: str, side: str = None):
        symbol = uni_symbol_to_coin(symbol)
        orders = self.info.open_orders(self.address)
        hl_side = "B" if side == "BUY" else ("S" if side == "SELL" else None)
        to_cancel = [
            {"coin": symbol, "oid": order["oid"]}
            for order in orders
            if order["coin"] == symbol
            and (order["side"] == hl_side if hl_side else True)
        ]
        if to_cancel:
            logger.info(f"Canceling orders for {symbol} {side}")
            self.exchange.bulk_cancel(to_cancel)

    def place_limit_order(self, order: LimitOrder):
        symbol = uni_symbol_to_coin(order.symbol)
        self.print_limit_order(order)
        data = {
            "coin": symbol,
            "is_buy": order.side.lower() == "buy",
            "sz": self.hl_util.valid_qty(symbol, order.size),
            "limit_px": self.hl_util.valid_price(symbol, order.price),
            "order_type": self.tif_gtc,
            "reduce_only": False,
        }
        logger.debug(data)
        resp = self.exchange.bulk_orders([data])
        logger.debug(resp)
        return resp

    def place_bulk_market_order(
        self, orders: List[MarketOrder], slippage=0.01
    ) -> List[OrderFill]:
        hl_orders = []
        symbols = [order.symbol for order in orders]
        for order in orders:
            logger.info(
                f"{dt.datetime.utcfromtimestamp(time.time_ms()/1e3)} {order.side} {order.symbol} {order.size:.2f} / {order.price*order.size:.2f} @ {order.price:.2f} MKT"
            )
            symbol = binance_contract_to_coin(order.symbol)
            limit_px = (
                order.price * (1 + slippage)
                if order.side == "BUY"
                else order.price * (1 - slippage)
            )
            hl_orders.append(
                {
                    "coin": symbol,
                    "is_buy": order.side.lower() == "buy",
                    "sz": self.hl_util.valid_qty(symbol, order.size),
                    "limit_px": self.hl_util.valid_price(symbol, limit_px),
                    "order_type": self.tif_ioc,
                    "reduce_only": False,
                }
            )
        if not self.ghost_mode:
            resp = self.exchange.bulk_orders(hl_orders)
            logger.info(resp)
            if resp["status"] == "err":
                return []
            fills: List[OrderFill] = []
            for idx, item in enumerate(resp["response"]["data"]["statuses"]):
                if "filled" in item:
                    f = item["filled"]
                    fills.append(
                        OrderFill(
                            symbol=symbols[idx],
                            size=float(f["totalSz"]),
                            price=float(f["avgPx"]),
                            created_at=dt.datetime.now(dt.timezone.utc),
                        )
                    )
            return fills
        else:
            return []
