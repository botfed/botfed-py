from hyperliquid.utils.types import (
    L2BookMsg,
)

from ..core.order_book import OrderBookBase


class OrderBook(OrderBookBase):
    def __init__(self, coin):
        super().__init__(coin)

    def _on_book_update(self, book_msg: L2BookMsg) -> None:
        book_data = book_msg["data"]
        if book_data["coin"] != self.coin:
            return
        self.book_data = {
            "bids": book_data["levels"][0],
            "asks": book_data["levels"][1],
            "time": book_data["time"],
        }

    def on_trade(self, msg):
        print(msg)

    @property
    def exchange(self):
        return "hyperliquid"
