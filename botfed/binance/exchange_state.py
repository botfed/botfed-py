from typing import List
import datetime as dt
import pandas as pd
import requests
from .utils import get_binance_open_interest


class BinanceState:
    """Binance state"""

    def __init__(self, tickers: List[str] = []):
        self.current_data = {}
        self.funding_rates = {}
        self.mark_price_snaps = {}
        self.intervals = {}
        self.oi = {}
        # for ticker in tickers:
        #     self.intervals[ticker.upper()] = self.get_fr_interval(ticker.upper())
        # print(self.intervals)

    def on_mark_price(self, data):
        coin = data["s"].replace("USDT", "-USD")
        ticker = data["s"]
        if coin not in self.mark_price_snaps:
            self.mark_price_snaps[coin] = []
        self.mark_price_snaps[coin].append(data)
        if len(self.mark_price_snaps[coin]) > 60 * 60 * 8:
            self.mark_price_snaps[coin] = self.mark_price_snaps[coin][-100:]
        funding_rate = float(data["r"])
        # Normaliza la tasa de financiamiento según el intervalo
        if coin not in self.intervals:
            self.intervals[ticker] = self.get_fr_interval(ticker)
        if coin not in self.oi:
            self.oi[ticker] = get_binance_open_interest(ticker)
        intervalo = self.intervals.get(ticker, 8)
        # Usa un valor predeterminado si es necesario
        normalized_funding_rate = funding_rate / intervalo
        self.funding_rates[coin] = {
            "ticker": coin,
            "fr": normalized_funding_rate,
            "oi": self.oi[ticker],
        }

    # obtención de intervalo de financiamiento para cada símbolo
    def get_fr_interval(self, symbol):
        url = "https://fapi.binance.com/fapi/v1/fundingInfo"
        params = {"symbol": symbol}
        response = requests.get(url, params=params)
        data = response.json()
        if data and len(data) > 0:
            return data[-1]["fundingIntervalHours"]
        return 8

    @property
    def fr_df(self):
        return pd.DataFrame.from_records([el for _, el in self.funding_rates.items()])

    @property
    def name(self):
        return "binance"

    @property
    def has_orderbook(self):
        return True
