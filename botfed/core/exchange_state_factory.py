import os
from typing import List, Dict, Mapping
from web3 import Web3
from .exchange_states import ExchangeState
from ..core.timer_feed import TimerFeed
from ..core.event_loop import EventLoop
from ..rabbitx.exchange_state import RabbitXState
from ..hyperliquid.exchange_state import HyperLiquidState
from ..binance.exchange_state import BinanceState


class ExchangeStatesFactory:

    @classmethod
    def build_exchange_states(
        cls, tickers: List[str], exchange_names: List[str], event_loop: EventLoop
    ) -> Mapping[str, ExchangeState]:
        states: Mapping[str, ExchangeState] = {}
        for exchange in exchange_names:
            if exchange == "rabbitx":
                states[exchange] = cls.build_rabbitx_state(event_loop)
            elif exchange == "bfx":
                states[exchange] = cls.build_bfx_state(event_loop)
            elif exchange == "hyperliquid":
                states[exchange] = cls.build_hyperliquid_state(event_loop)
            elif exchange == "dydx":
                states[exchange] = cls.build_dydx_state(event_loop)
            elif exchange == "binance":
                states[exchange] = cls.build_bin_state(tickers, event_loop)
            else:
                raise Exception("Uknown exchange")
        return states

    @classmethod
    def build_rabbitx_state(cls, event_loop: EventLoop) -> ExchangeState:
        strat = RabbitXState()
        feed = TimerFeed(freq_ms=60 * 1000)
        feed.add_listener(strat)
        event_loop.add_feed(feed)
        return strat

    @classmethod
    def build_bfx_state(cls, event_loop: EventLoop) -> ExchangeState:
        from ..bfx.exchange_state import BFXState
        strat = BFXState()
        feed = TimerFeed(freq_ms=60 * 1000)
        feed.add_listener(strat)
        event_loop.add_feed(feed)
        return strat

    @classmethod
    def build_hyperliquid_state(cls, event_loop: EventLoop) -> ExchangeState:
        WS_URL = os.getenv("WS_URL")
        assert WS_URL, "WS_URL must be set in .env file."
        w3 = Web3(Web3.WebsocketProvider(WS_URL))
        strat = HyperLiquidState(w3)
        # default every ten seconds:
        feed = TimerFeed(freq_ms=10 * 1000)
        feed.add_listener(strat)
        event_loop.add_feed(feed)
        return strat

    @classmethod
    def build_dydx_state(cls, event_loop: EventLoop) -> ExchangeState:
        from ..dydx.feed import DyDXFeed
        from ..dydx.exchange_state import DyDxState

        strat = DyDxState()
        # default every ten seconds:
        feed = DyDXFeed()
        feed.add_listener(strat)
        event_loop.add_feed(feed)
        return strat

    @classmethod
    def build_bin_state(cls, tickers: List[str], event_loop: EventLoop) -> ExchangeState:
        from ..binance.feed import BinanceFRFeed
        from ..dydx.exchange_state import DyDxState

        strat = BinanceState(tickers=tickers)
        # default every ten seconds:
        feed = BinanceFRFeed(tickers)
        feed.add_listener(strat)
        event_loop.add_feed(feed)
        return strat
