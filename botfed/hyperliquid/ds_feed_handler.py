

class HLDSHandler:
    """Binance Feed"""

    def __init__(self, obs):
        self.obs = obs
        self.trade_listeners = []

    def add_trade_listener(self, listener):
        self.trade_listeners.append(listener)

    def on_book_update(self, msg):
        if msg['data']["coin"] in self.obs:
            self.obs[msg['data']["coin"]].on_book_update(msg)

    def on_trade(self, msg):
        for data in msg['data']:
            for listener in self.trade_listeners:
                listener(data)
