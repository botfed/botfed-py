import logging
from abc import ABC, abstractmethod
from typing import List
from .universe import binance_contract_to_coin, coin_to_binance_contract


class BinDSHandler:
    """Binance Feed"""

    def __init__(self, obs, trade_store):
        self.obs = obs
        self.trade_store = trade_store
        self.liq_listeners = []

    def add_liq_listener(self, listener):
        self.liq_listeners.append(listener)

    def on_book_ticker(self, msg):
        coin = binance_contract_to_coin(msg["ticker"])
        if coin in self.obs:
            self.obs[coin].on_book_ticker(msg['ticker'], msg)

    def on_agg_trade(self, msg):
        self.trade_store.on_agg_trade(msg)

    def on_liquidation(self, msg):
        for listener in self.liq_listeners:
            listener.on_liquidation(msg)
