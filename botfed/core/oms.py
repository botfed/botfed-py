from abc import abstractmethod
import copy
import time
from typing import TypedDict, Mapping, List, Dict


class Position(TypedDict):
    symbol: str
    qty: float
    entry_price: float
    unrealized_profit: float
    notional: float


class AssetItem(TypedDict):
    asset: str
    wallet_balance: float
    available_balance: float


class AccountState(TypedDict):
    assets: List[AssetItem] = []
    positions: Mapping[str, Position] = {}
    ready: bool = False

    def equity(self):
        cash = sum(
            [
                el["wallet_balance"]
                for el in self["assets"]
                if el["asset"] in ["USDT", "USDC", "DAI"]
            ]
        )
        unrealized_profit = [pos["unrealized_profit"] for pos in self["positions"]]
        return cash + sum(unrealized_profit)


class OrderStatus:
    ACTIVE = "active"
    PENDING_NEW = "pending_new"
    CANCELED = "canceled"
    PENDING_CANCEL = "pending_cancel"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    REJECTED = "rejected"
    EXPIRED = "expired"


class LocalOrder(TypedDict):
    coin: str
    qty: float
    side: str  # buy or sell
    price: float
    type: str
    oid: int
    cloid: int
    status: OrderStatus
    ts_trigger: float
    micro_price: float
    spread: float
    created_at: float
    updated_at: float
    # track last tick from exch:
    ts_recv: float
    exch_ts: int
    exch_seq: int
    # extra info for debugging:
    extra: Dict


class OMS:

    def __init__(self):
        self._last_trade_time: Mapping[str, float] = {}
        self.account = AccountState()

    @abstractmethod
    def open_orders(self, coin):
        pass

    @abstractmethod
    def position_delta(self):
        pass

    def get_positions(self) -> Mapping[str, Position]:
        return copy.deepcopy(self.account.get("positions", {}))

    def get_position(self, symbol: str) -> Position:
        return copy.deepcopy(self.account.get("positions", {}).get(symbol, {}))

    def position_qty(self, symbol: str) -> float:
        return self.account.get("positions", {}).get(symbol, {}).get("qty", 0)

    def equity(self) -> float:
        return self.account.equity()

    @abstractmethod
    def process_order(self, order: {}):
        pass

    @abstractmethod
    def total_pos_size(self):
        pass

    @abstractmethod
    def valid_qty(self, coin, qty):
        pass

    @abstractmethod
    def valid_price(self, coin, price):
        pass

    @property
    def ready(self):
        return False

    def last_trade_time(self, coin) -> int:
        return 1000 * (time.time() - self._last_trade_time.get(coin, 0))

    def submit_market_order(
        self, coin, qty, side, price=None, slippage_bps=1, extra={}
    ):
        assert qty > 0, "qty must be positive"
        assert side in ["sell", "buy"]
        assert slippage_bps >= 0
        self._update_last_trade_time(coin)
        self._submit_market_order(
            coin, qty, side, price=price, slippage_bps=slippage_bps, extra=extra
        )

    @abstractmethod
    def _submit_market_order(
        self, coin, qty, side, price=None, slippage_bps=1, extra={}
    ):
        pass

    def _update_last_trade_time(self, coin):
        self._last_trade_time[coin] = time.time()
