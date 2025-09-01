import logging
import traceback
from typing import List
from .feed import Feed
from . import time


TEN_THOUSAND = int(1e4)

class TimerListener:
    """Timer listener class"""

    def on_timer(self):
        pass


class TimerFeed(Feed):
    """Timer class"""

    def __init__(self, freq_ms: int = TEN_THOUSAND):
        self.listeners: List[TimerListener] = []
        self.last_fire: int = -1
        self.freq_ms = freq_ms

    def add_listener(self, listener: TimerListener):
        """Add listener"""
        self.listeners.append(listener)

    def run_ticks(self):
        """
        Run for a bit then release control
        """
        utc_ms = int(round(time.time() * 1000))
        if utc_ms >= self.last_fire + self.freq_ms:
            self.last_fire = utc_ms
            self._dispatch()

    def _dispatch(self):
        for listener in self.listeners:
            try:
                listener()
            except Exception as e:
                logging.error(e)
                traceback.print_exc()
                continue
