import json
from ..core.event_loop import EventLoop
from ..core.order_book import OrderBookBase
from .feed import HLTxFeed

""" THis one never worked as thee explorerTxs feed only yields 5 txs from each block making a reconstruction of the book impossible. """


def coin_to_asset_id(coin):
    return 1


class OBDepth(OrderBookBase):
    def __init__(self, coin):
        self.block = 0
        self.asset_id = coin_to_asset_id(coin)
        self.bids = []
        self.asks = []
        self.watch_addr = "0xCc31595b3955B15e38b17D2E237A986F5E1f618D".lower()
        super().__init__(coin)

    def on_all_txs(self, msg):
        msg = json.loads(msg)
        if "channel" in msg or len(msg) == 0:
            return
        print(len(msg), msg[-1]["block"])
        for tx in msg:
            if tx["user"].lower() == self.watch_addr:
                print(tx)
            action = tx["action"]
            if action["type"] == "cancelByCloid":
                self.handle_cancel_by_cloid(tx)
            elif action["type"] == "cancel":
                self.handle_cancel(tx)
            elif action["type"] == "order":
                # {'time': 1715374876006, 'user': '0x31ca8395cf837de08b24da3f660e77761dfb974b', 'action': {'type': 'order', 'orders': [{'a': 2, 'b': False, 'p': '8.6817', 's': '891.35', 'r': False, 't': {'limit': {'tif': 'Alo'}}}], 'grouping': 'na'}, 'block': 148396515, 'ha
                self.handle_order(tx)
            elif action["type"] == "SetGlobalAction":
                pass
            elif action["type"] == "ValidatorSignWithdrawalAction":
                pass
            elif action["type"] == "VoteEthDepositAction":
                pass
            elif action["type"] == "VoteEthFinalizedWithdrawalAction":
                pass
            elif action["type"] == "batchModify":
                # {'time': 1715374950902, 'user': '0x0cefeb5cb11a8ab051bb8489f1bdc25e7fcb8dab', 'action': {'type': 'batchModify', 'modifies': [{'oid': 22152122050, 'order': {'a': 103, 'b': True, 'p': '0.62506', 's': '24', 'r': False, 't': {'limit': {'tif': 'Alo'}}}}]}, 'block': 148396725, 'hash': '0x8f8cbbddf993166c8f4c0408d85ab50104006a064ebe4f1f9e741858b5769744'}
                pass
            else:
                print(tx)

    def handle_cancel_by_cloid(self, tx):
        user = tx["user"]
        cancels = [
            user + el["cloid"]
            for el in tx["action"]["cancels"]
            if el["asset"] == self.asset_id
        ]
        self.bids = [el for el in self.bids if user + el["c"] not in cancels]
        self.asks = [el for el in self.asks if user + el["c"] not in cancels]

    def handle_cancel(self, tx):
        cancels = [el for el in tx["action"]["cancels"] if el["a"] == self.asset_id]
        self.bids = [
            el for el in self.bids if el["oid"] not in [c["o"] for c in cancels]
        ]
        self.asks = [
            el for el in self.asks if el["oid"] not in [c["o"] for c in cancels]
        ]

    def handle_order(self, tx):
        orders = tx["action"]["orders"]
        bids = [ord for ord in orders if ord["a"] == self.asset_id and ord["b"] is True]
        asks = [
            ord for ord in orders if ord["a"] == self.asset_id and ord["b"] is False
        ]


if __name__ == "__main__":
    event_loop = EventLoop()
    ob = OBDepth("BTC")
    feed = HLTxFeed()
    feed.add_listener(ob)
    feed.start()
    event_loop.add_feed(feed)
    event_loop.run()
