from typing import Any
from .sim_exchange import SimExchange
from .meta import meta


class HLInterfaceSim:
    def __init__(self, sim_exch: SimExchange):
        self.exch = sim_exch

    def subscribe(self, msg, callback):
        return

    def subscribe_tc(self, callback):
        return

    def add_user_listener(self, callback):
        return self.exch.add_user_listener(callback)

    def add_orders_listener(self, callback):
        return self.exch.add_orders_listener(callback)

    def add_order_resp_listener(self, listener):
        return self.exch.add_listener_order_resp(listener)

    def add_user_events_listener(self, callback):
        return self.exch.add_listener_user_events(callback)

    def meta(self):
        return meta

    def get_spot_positions(self):
        return self.exch.get_spot_positions()

    def open_orders(self):
        return self.exch.get_open_orders()

    def user_state(self):
        return self.exch.user_state()

    def submit_modify(self, data):
        return self.exch.on_modify(data)

    def submit_orders(self, data):
        return self.exch.on_bulk_orders(data)

    def cancel_orders(self, data):
        return self.exch.on_cancel(data)


    def print_orders(self):
        print("Orders: ")
        for order in self.exch.get_open_orders():
            print(f"{order['oid']}: {order['sz']} {order['side']} {order['limitPx']}")

    def print_positions(self):
        for coin, pos in self.exch.positions.items():
            print(
                f"{coin}: {pos['sz']}, {pos['sz'] * self.exch.obs[coin].mid_price()} {pos['entryPx']}, {self.exch.obs[coin].mid_price()}"
            )

    def print_pnl(self):
        print(
            f"Acct Bal: {self.exch.acct_bal}, unrealized_pnl: {self.exch.unrealized_pnl()}"
        )
        print(
            f"PNL {self.exch.pnl()}, Vlm Traded: {self.exch.vlm_traded} Bips: {self.exch.pnl() / (1 + self.exch.vlm_traded) * 1e4}"
        )
        print(f"Total fees {self.exch.total_fees}")

    def on_timer(self):
        self.print_positions()
        # self.print_orders()
        self.print_pnl()
