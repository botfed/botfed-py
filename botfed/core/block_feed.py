from web3 import Web3
from .feed import Feed
from .block_listener import BlockListener


class BlockFeed(Feed):

    def __init__(self, w3: Web3):
        self.w3 = w3
        self.listeners: [BlockListener] = []

    def add_listener(self, listener: BlockListener):
        """ Add listener"""
        self.listeners.append(listener)

    def run_ticks(self):
        """
        Run for a bit then release control
        """
        block_filter = self.w3.eth.filter("latest")
        for event in block_filter.get_new_entries():
            block = self.w3.eth.get_block(event)
            self._dispatch(block)

    def _dispatch(self, event: {}):
        for listener in self.listeners:
            listener.on_block(event)
