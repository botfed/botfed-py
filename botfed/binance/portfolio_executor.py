"""

This strat that places quotes within a range
but keeps them for a minimum time, thus hoping to capture a mean reversion effect.

Research shows the replacement strat should get more hits and should have positive markouts.
"""

import requests
from ..core.oms import OMS
from ..logger import get_logger
from .universe import coin_to_binance_contract


logger = get_logger(__name__)


def normalize_symbol(coin):
    if coin.upper() in ["WETH"]:
        return "ETH"
    elif coin.upper() in ["CBBTC", "WBTC", "TBTC"]:
        return "BTC"
    elif coin.upper() in ["USUI"]:
        return "SUI"
    else:
        return coin


class PortfolioExecutor:
    BASE_URL = "https://fapi.binance.com"

    def __init__(
        self,
        oms: OMS,
        min_order_usd: float = 25,
        max_order_pct: float = 0.05,
    ):
        self.oms = oms
        self.min_order_usd = min_order_usd
        self.max_order_pct = max_order_pct

    def execute(self, target_portfolio, min_pct=None):
        """
        Takes target_portfolio: dict of {symbol: target_position}
        Fetches current positions
        Calculates difference
        Executes orders to align portfolio
        """
        if not self.oms.ready:
            logger.info("Skipping execution, oms not ready.")
            return
        orders = []
        for coin, target_qty in target_portfolio.items():
            try:
                qty = self.oms.get_position(coin).get("qty", 0)
                logger.debug(f"Current {coin} qty={qty}")
            except Exception as e:
                logger.debug(f"Could not get position for {coin}, {e}")
                continue
            qty_delta = target_qty - qty
            price = self.get_price(coin)
            if not price:
                logger.debug(f"{coin} price is None, skipping")
                continue
            elif min_pct and abs(qty_delta) < abs(min_pct * target_qty):
                logger.debug(f"Skipping {coin}: delta {qty_delta:.4f} below threshold {min_pct * target_qty:.4f}")
                continue
            if abs(qty_delta * price) > self.min_order_usd:
                equity = self.oms.equity()
                target_ntl = abs(qty_delta * price)
                max_ntl = min(self.max_order_pct * equity, target_ntl)
                qty_delta = qty_delta * max_ntl / target_ntl
                orders.append((coin, qty_delta))
        logger.info(f"Submitting orders: {orders}")
        for coin, qty in orders:
            self.oms.submit_market_order(
                coin, abs(qty), "sell" if qty < 0 else "buy", None
            )

    def normalize_symbol(self, coin):
        return normalize_symbol(coin)

    def is_tradeable(self, coin):
        return self.oms.is_tradeable(coin)

    def get_price(self, coin: str):
        """
        Fetches the mid price from the order book (best bid + best ask) / 2
        symbol: e.g., 'BTCUSDT'
        """
        try:
            symbol = coin_to_binance_contract(coin)

            url = f"{self.BASE_URL}/fapi/v1/depth"
            params = {"symbol": symbol, "limit": 5}

            resp = requests.get(url, params=params)
            data = resp.json()

            best_bid = float(data["bids"][0][0])
            best_ask = float(data["asks"][0][0])
            mid_price = (best_bid + best_ask) / 2

            return mid_price
        except Exception as e:
            return None
