import os
from ..core import eth_account
from ..core.feed import Feed
from eth_account.signers.local import LocalAccount
from typing import Any

# from .exchange import Exchange
# from .info import Info
from ..tradeserver.client import TradeClient

from ..logger import get_logger

from hyperliquid.exchange import Exchange
from hyperliquid.info import Info

logger = get_logger(__name__)


def setup(address: str, secret: str, base_url=None, skip_ws=True):
    if not secret:
        secret = os.environ["HYPER_SECRET"]
    if not address:
        address = os.environ["HYPER_ADDRESS"]
    account: LocalAccount = eth_account.Account.from_key(secret)
    info = Info(base_url, skip_ws)
    exchange = Exchange(account, base_url, account_address=address)
    return address, info, exchange


class HLInterface(Feed):
    def __init__(
        self, tc: TradeClient, user_feed: Feed, eoa: str = None, secret: str = None
    ):
        self.tc = tc
        self.user_feed = user_feed
        self.address, self.info, self.exchange = setup(eoa, secret, skip_ws=True)

    def subscribe(self, msg, callback):
        return self.info.subscribe(msg, callback)

    def subscribe_tc(self, callback):
        self.tc.add_listener(callback)

    def add_user_listener(self, callback):
        self.user_feed.add_user_listener(callback)

    def add_orders_listener(self, callback):
        self.user_feed.add_orders_listener(callback)

    def add_order_resp_listener(self, listener):
        self.tc.add_listener(listener)

    def meta(self):
        return self.info.meta()

    def get_spot_positions(self):
        return self.info.post(
            "/info", {"type": "spotClearinghouseState", "user": self.address}
        )

    def post(self, url_path: str, payload: Any = None) -> Any:
        return self.info.post(url_path, payload)

    def open_orders(self):
        return self.info.open_orders(self.address)

    def user_state(self):
        return self.info.user_state(self.address)

    def submit_modify(self, data):
        return self.tc.submit(data)

    def submit_orders(self, data):
        return self.tc.submit(data)

    def cancel_orders(self, data):
        return self.tc.submit(data)

    def run_ticks(self):
        self.user_feed.run_ticks()
