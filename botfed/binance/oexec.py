from typing import List
import os
import dotenv


from binance.client import Client
from binance.enums import SIDE_SELL, SIDE_BUY, ORDER_TYPE_MARKET

from ..logger import get_logger

from ..core.oexec import OrderResp


logger = get_logger(__name__)

dotenv.load_dotenv()

BIN_API_KEY = os.getenv("BIN_API_KEY")
BIN_API_SECRET = os.getenv("BIN_API_SECRET")


class BinExec:

    def __init__(
        self,
        ghost_mode=False,
    ):
        self.client = Client(BIN_API_KEY, BIN_API_SECRET, testnet=False)
        self.ghost_mode = ghost_mode

    def process_order(self, data) -> List[OrderResp]:
        if data["type"] == "cancel":
            return self.cancel_orders(data["orders"])
        elif data["type"] == "bulk":
            result = self.submit_bulk_order(data["orders"])
        elif data["type"] == "market":
            result = self.submit_market_order(data["orders"])
        else:
            raise ValueError(f"Unknown order type: {data['type']}")
        return result

    def cancel_orders(self, orders) -> List[OrderResp]:
        if self.ghost_mode:
            return {"ghost_mode": True}
        symbols = list(set([el["coin"] for el in orders]))
        result = []
        for symbol in symbols:
            cancels = [el for el in orders if el["coin"] == symbol and el.get("cloid")]
            for el in cancels:
                try:
                    resp = self.client.futures_cancel_order(
                        symbol=symbol, origClientOrderId=el["cloid"]
                    )
                    if "clientOrderId" not in resp:
                        resp = {
                            "status": "UNKOWN",
                            "cloid": el["cloid"],
                            "oid": el.get("oid"),
                            "updateTime": el.get("exch_update_time"),
                            "error_msg": resp,
                        }
                        result.append(
                            OrderResp(
                                cloid=resp["clientOrderId"],
                                status=resp["status"],
                                oid=resp["orderId"],
                                update_time=resp["updateTime"],
                                error_msg=resp.get("error_msg"),
                            )
                        )
                    else:
                        result.append(
                            OrderResp(
                                cloid=resp["clientOrderId"],
                                status=resp["status"],
                                oid=resp["orderId"],
                                update_time=resp["updateTime"],
                            )
                        )

                except Exception as e:
                    logger.error(f"Error cancelling order: {e}")
                    result.append(
                        OrderResp(
                            cloid=el["cloid"],
                            oid=el.get("oid"),
                            status="ERROR",
                            error_msg=str(e),
                        )
                    )
        return result

    def submit_market_order(self, orders) -> List[OrderResp]:
        result = []
        for order in orders:
            resp = self.client.futures_create_order(
                symbol=order["coin"],
                side=SIDE_BUY if order["side"] == "buy" else SIDE_SELL,
                type=ORDER_TYPE_MARKET,
                quantity=order["qty"],
            )
            result.append(
                OrderResp(
                    oid=resp["orderId"],
                    cloid=resp["clientOrderId"],
                    status=resp["status"],
                    update_time=resp["updateTime"],
                )
            )
        return result

    def submit_bulk_order(self, orders) -> List[OrderResp]:
        ors = []
        for order in orders:
            data = {
                "symbol": order["coin"],
                "side": order["side"].upper(),
                "quantity": str(order["qty"]),
                "newClientOrderId": order.get("cloid"),
                "price": str(order["price"]),
                "type": "LIMIT",
                "timeInForce": "GTX",
                # "recvWindow": 1000,
            }
            ors.append(data)
        if self.ghost_mode:
            return {"ghost_mode": True}
        result = []
        resp = self.client.futures_place_batch_order(batchOrders=ors)
        for el in resp:
            result.append(
                OrderResp(
                    oid=el["orderId"],
                    cloid=el["clientOrderId"],
                    status=el["status"],
                    update_time=el["updateTime"],
                )
            )
        return result
