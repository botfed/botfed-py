from abc import abstractmethod
from queue import Queue


class Feed:
    """Abstract feed base class"""

    def __init__(self):
        self.listeners = []

    def add_listener(self, listener):
        """Add listener"""
        self.listeners.append(listener)

    def run_ticks(self):
        """Run feed ticks for a bit then release control"""
        pass

    def close(self):
        """Cleanup and close"""
        pass

