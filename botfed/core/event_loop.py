"""
Main event loop class
"""

from . import time
import traceback
from .feed import Feed
from ..logger import get_logger

logger = get_logger(__name__)


class EventLoop:
    """Main program event loop"""

    def __init__(self, sleep_time=0, etime: int = None):
        self.feeds: [Feed] = []
        self.sleep_time = sleep_time
        self.etime = etime

    def add_feed(self, feed: Feed):
        """Add feed"""
        self.feeds.append(feed)

    def run(self, stop_event=None):
        """run indefinitely"""
        while True:
            time.sleep(self.sleep_time)
            if stop_event is not None and stop_event.is_set():
                break
            for feed in self.feeds:
                try:
                    feed.run_ticks()
                except Exception as e:
                    logger.error(f"Error in feed: {e}")
                    traceback.print_exc()
            self.feeds = [
                feed
                for feed in self.feeds
                if not hasattr(feed, "done") or not feed.done
            ]
            if len(self.feeds) == 0:
                break
            if self.etime and self.etime <= time.time():
                break

        for feed in self.feeds:
            feed.close()
        logger.info("EventLoop: All feeds closed")
