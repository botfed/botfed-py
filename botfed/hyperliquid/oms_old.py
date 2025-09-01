import logging
import threading
from typing import Mapping
import json
from concurrent.futures import ThreadPoolExecutor
import os
import time
import eth_account
from eth_account.signers.local import LocalAccount

from hyperliquid.utils.signing import get_timestamp_ms
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info
from hyperliquid.utils.types import (
    UserEventsMsg,
)
from ..core.timer_feed import TimerListener
from ..core.oms import OMS
from ..core.order_book import OrderBookBase


def setup(base_url=None, skip_ws=False):
    config = os.environ
    account: LocalAccount = eth_account.Account.from_key(config["HYPER_SECRET"])
    address = config["HYPER_ADDRESS"]
    if address == "":
        address = account.address
    logging.info(f"Running with account address: {address}")
    if address != account.address:
        logging.info("Running with agent address: %s" % account.address)
    info = Info(base_url, skip_ws)
    user_state = info.user_state(address)
    margin_summary = user_state["marginSummary"]
    if float(margin_summary["accountValue"]) == 0:
        logging.info(
            "Not running the example because the provided account has no equity."
        )
        url = info.base_url.split(".", 1)[1]
        error_string = f"No accountValue:\nIf you think this is a mistake, make sure that {address} has a balance on {url}.\nIf address shown is your API wallet address, update the config to specify the address of your account, not the address of the API wallet."
        raise Exception(error_string)
    exchange = Exchange(account, base_url, account_address=address)
    return address, info, exchange


class HyperOMS(OMS, TimerListener):
    tif_alo = {"limit": {"tif": "Alo"}}
    market_order = {"limit": {"tif": "Ioc"}}

    def __init__(
        self,
        obs: Mapping[str, OrderBookBase],
        ghost_mode=False,
        trade_log_file="./trade.log",
    ):
        self.obs = obs
        self.address, self.info, self.exchange = setup()
        self.meta = self.info.meta()
        self.info.subscribe(
            {"type": "userEvents", "user": self.address}, self.on_user_events
        )
        self.positions = {}
        self.pending_orders = []
        self.user_state = {}
        self.ghost_mode = ghost_mode
        self.trade_log_file = trade_log_file
        OMS.__init__(self)
        self.poll()
        self.trade_log = []

    def sz_decimals(self, coin):
        for el in self.meta["universe"]:
            if el["name"] == coin:
                return el["szDecimals"]

    def on_user_events(self, user_events: UserEventsMsg) -> None:
        user_events_data = user_events["data"]
        return user_events_data

    def process_order(self, order):
        self.submit_bulk_order(order["orders"])

    def submit_bulk_order(self, orders):
        ors = []
        for order in orders:
            ors.append(
                {
                    "coin": order["coin"],
                    "is_buy": order["side"] == "buy",
                    "sz": order["qty"],
                    "limit_px": order["price"],
                    "order_type": (
                        self.market_order if order["type"] == "market" else self.tif_alo
                    ),
                    "reduce_only": order.get("reduce_only", False),
                }
            )
        return self.exchange.bulk_orders(ors)

    def submit_limit_order(self, coin, qty, side, price, reduce_only=False):
        is_buy = side == "buy"
        order_request = {
            "coin": coin,
            "is_buy": is_buy,
            "sz": qty,
            "limit_px": price,
            "order_type": {"limit": {"tif": "Alo"}},
            "reduce_only": reduce_only,
        }
        result = self.exchange.bulk_orders([order_request])
        logging.info("Order result: %s" % result)

    def submit_limit_order_bulk(self, orders):
        ors = []
        for order in orders:
            ors.append(
                {
                    "coin": order[0],
                    "is_buy": order[1] == "buy",
                    "sz": order[2],
                    "limit_px": order[3],
                    "order_type": {"limit": {"tif": "Alo"}},
                    "reduce_only": order[4],
                }
            )

        result = self.exchange.bulk_orders(ors)
        logging.info("Order result: %s" % result)

    def _submit_market_order(
        self, coin, qty, side, price=None, slippage_bps=100, extra={}
    ):
        if not self.ready:
            logging.info("Not ready")
            return
        is_buy = side == "buy"
        sz_decimals = self.sz_decimals(coin)
        qty = round(qty, sz_decimals)
        logging.info(f"Submitting market order for {qty} {coin} side {side}")
        if self.ghost_mode:
            return

        def func():
            utc_now = time.time()
            try:
                order_result = self.exchange.market_open(
                    coin,
                    is_buy,
                    qty,
                    px=price,
                    slippage=max(0.0001, slippage_bps / 1e4),
                )
                ts_reply = time.time()
            except Exception as e:
                ts_reply = time.time()
                logging.error(f"Error submitting order {e}")
                self.log_trade(
                    {
                        "ts_submit": utc_now,
                        "coin": coin,
                        "exchange": "hyperliquid",
                        "target_price": price,
                        "slippage_tolerance_bps": slippage_bps,
                        "mid_price": self.obs[coin].mid_price(),
                        "spread": self.obs[coin].spread(),
                        "side": side,
                        "qty": qty,
                        "status": "error",
                        "error": str(e),
                        "extra": extra,
                        "ts_reply": ts_reply,
                        "submit_latency_ms": (ts_reply - utc_now) * 1000,
                        "midprice_post": self.obs[coin].mid_price(),
                    }
                )
                return
            if order_result["status"] == "ok":
                for status in order_result["response"]["data"]["statuses"]:
                    try:
                        filled = status["filled"]
                        if coin not in self.positions:
                            self.positions[coin] = {"szi": 0}
                        self.positions[coin]["szi"] += float(filled["totalSz"]) * (
                            1 if is_buy else -1
                        )
                        self.positions[coin]["positionValue"] = abs(
                            float(filled["avgPx"]) * self.positions[coin]["szi"]
                        )
                        logging.info(
                            f'Order #{filled["oid"]} filled {filled["totalSz"]} @{filled["avgPx"]}, new position size: {self.positions[coin]["szi"]}, position value: {self.positions[coin]["positionValue"]}'
                        )
                        sign = 1 if is_buy else -1
                        slippage_mid_bps = (
                            1e4
                            * sign
                            * (float(filled["avgPx"]) - self.obs[coin].mid_price())
                            / self.obs[coin].mid_price()
                        )
                        slppage_target_bps = (
                            (
                                1e4
                                * sign
                                * (float(filled["avgPx"]) - price)
                                / self.obs[coin].mid_price()
                            )
                            if price
                            else None
                        )

                        self.log_trade(
                            {
                                "ts_submit": utc_now,
                                "coin": coin,
                                "exchange": "hyperliquid",
                                "target_price": price,
                                "slippage_tolerance_bps": slippage_bps,
                                "slippage_mid_bps": slippage_mid_bps,
                                "slippage_target_bps": slppage_target_bps,
                                "mid_price": self.obs[coin].mid_price(),
                                "spread": self.obs[coin].spread(),
                                "filled": filled,
                                "expected_slippage_bps": self.obs[coin].spread() / 2,
                                "side": side,
                                "qty": qty,
                                "status": "ok",
                                "extra": extra,
                                "ts_reply": ts_reply,
                                "submit_latency_ms": (ts_reply - utc_now) * 1000,
                                "midprice_post": self.obs[coin].mid_price(),
                            }
                        )
                    except KeyError:
                        logging.error(f"Error...  {status}")
                        self.log_trade(
                            {
                                "ts_submit": utc_now,
                                "coin": coin,
                                "exchange": "hyperliquid",
                                "target_price": price,
                                "mid_price": self.obs[coin].mid_price(),
                                "spread": self.obs[coin].spread(),
                                "side": side,
                                "qty": qty,
                                "status": "error",
                                "error": status,
                                "extra": extra,
                                "ts_reply": ts_reply,
                                "submit_latency_ms": (ts_reply - utc_now) * 1000,
                                "midprice_post": self.obs[coin].mid_price(),
                            }
                        )

        func()
        # threading.Thread(target=func).start()

    def log_trade(self, log_item):
        with open(self.trade_log_file, "a") as f:
            f.write(json.dumps(log_item) + "\n")

    def equity(self):
        if self.user_state:
            return float(self.user_state["marginSummary"]["accountValue"])
        else:
            return 0

    @property
    def ready(self):
        return self.user_state != {}

    def position_size(self, coin: str):
        return float(self.positions.get(coin, {"szi": 0})["szi"])

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
            pos_value = float(position["positionValue"])
            size = float(position["szi"])
            delta += pos_value * (1 if size > 0 else -1)
        return delta

    def all_open_orders(self):
        return self.info.open_orders(self.address)

    def bulk_cancel(self, orders):
        self.exchange.bulk_cancel(orders)

    def poll(self):
        logging.debug("OMS polling ...")
        self.user_state = self.info.user_state(self.address)
        self.positions = {}
        for position in self.user_state["assetPositions"]:
            coin = position["position"]["coin"]
            self.positions[coin] = position["position"]
            self.positions[coin]["szi"] = float(self.positions[coin]["szi"])
            self.positions[coin]["price"] = abs(
                float(self.positions[coin]["positionValue"])
                / self.positions[coin]["szi"]
            )
            logging.debug(f"Position {coin}: {self.positions[coin]['szi']}")
        return {coin: val for coin, val in self.positions.items()}

    def on_timer(self):
        self.poll()
