import concurrent
from typing import Dict
import pandas as pd
from web3 import Web3
from ..core.timer_feed import TimerListener
from ..core.exchange_states import ExchangeState
from .api import HyperLiquidApi


class HyperLiquidState(TimerListener, ExchangeState):
    """Attempts to maintain correct exchange state"""

    def __init__(self, w3: Web3):
        self.markets: Dict = {}
        self.api = HyperLiquidApi(w3)
        self.funding_rates = {}
        self.fr_df: pd.DataFrame = pd.DataFrame({})
        self.market_stats = []

    def update_market_stats(self):
        market_stats = self.api.get_market_stats()
        if market_stats:
            self.market_stats = market_stats

    def update_rates(self):
        market_configs = self.api.market_configs
        self.update_market_stats()
        res = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            res = [
                executor.submit(
                    self.api.get_last_funding_rate, market_config[0].split("-")[0]
                )
                for market_config in market_configs
            ]
            concurrent.futures.wait(res)
        for idx, market_config in enumerate(market_configs):
            fr = res[idx].result()
            if fr is None:
                continue
            record = {
                "ticker": market_config[0],
                "coin": fr["coin"],
                "m_idx": idx,
                "fr": float(fr["fundingRate"]),
                "fr_ann": float(fr["fundingRate"]) * 365 * 24 * 100,
                "pr": float(fr["premium"]),
                "fv": self.api.get_funding_rate_velocity(idx),
                "oi": float(self.market_stats[idx]["openInterest"]) if self.market_stats else 0,
                "time": fr["time"],
            }
            self.funding_rates[record["ticker"]] = record

    def on_timer(self):
        self.update_rates()
        if len(self.funding_rates) == 0:
            print("Fundings rates not found")
            return
        self.fr_df = pd.DataFrame.from_records(
            [el for _, el in self.funding_rates.items()]
        ).sort_values(by="fr", ascending=False, key=abs)

    @property
    def name(self):
        return "hyperliquid"

    @property
    def has_orderbook(self):
        return True
