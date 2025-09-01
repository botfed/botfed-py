import pandas as pd
import requests
import json
import os
from urllib.parse import urlencode
from dotenv import load_dotenv
import time
import hmac
import hashlib


class BinanceTradeLogMerger:
    def __init__(self, api_key, api_secret, trade_log_path):
        # Load API keys from .env file
        load_dotenv()
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = "https://fapi.binance.com"
        self.headers = {"X-MBX-APIKEY": self.api_key}

        # Load and parse the trade log data
        self.trade_log_path = trade_log_path
        self.trade_data = self._load_trade_log()

    def _load_trade_log(self):
        """Load trade log from file and convert nested JSON into a structured DataFrame."""
        with open(self.trade_log_path, "r") as file:
            data = []
            for line in file:
                entry = json.loads(line)
                for idx, order in enumerate(entry["orders"]):
                    flat_data = {
                        "resp_status": (
                            entry["resp"][idx]["status"] if entry["resp"] else None
                        ),
                        "update_time": (
                            entry["resp"][idx].get("update_time") if entry["resp"] else None
                        ),
                        "ts_submit": entry["ts_submit"],
                        "ts_reply": entry["ts_reply"],
                        "type": entry["type"],
                        "exchange": entry["exchange"],
                        "error_msg": entry['resp'][idx].get("error_msg", None),
                        **{
                            k: order[k] for k in order if k != "extra"
                        },  # Flatten the order details
                        **order.get("extra", {}),  # Add extra fields if present,
                    }
                    if flat_data.get("oid") is None:
                        for resp in entry["resp"]:
                            if "cloid" in resp:
                                if resp["cloid"] == flat_data["cloid"]:
                                    flat_data["oid"] = resp["oid"]
                                    flat_data["update_time"] = resp["update_time"]
                                    break
                    data.append(flat_data)
            df = pd.DataFrame(data)
            df["exch_ts"] = df["exch_ts"].astype(int)
            df["exch_seq"] = df["exch_seq"].astype(int)
            return df

    def _create_signature(self, params):
        """Create HMAC SHA256 signature for Binance API."""
        query_string = urlencode(params)
        return hmac.new(
            self.api_secret.encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    def _fetch_orders(self, symbol, start_order_id, end_order_id):
        """Fetch orders from Binance API within the specified orderId range for a symbol."""
        endpoint = f"{self.base_url}/fapi/v1/allOrders"
        params = {
            "symbol": symbol,
            "orderId": int(start_order_id),
            # "endTime": int(end_order_id),
            "timestamp": int(time.time() * 1000),
        }
        params["signature"] = self._create_signature(params)

        response = requests.get(endpoint, headers=self.headers, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to fetch orders for symbol {symbol}: {response.json()}")
            return []

    def merge_orders_with_trades(self):
        """Download orders per symbol, merge with trade data, and return merged DataFrame."""
        all_orders = []

        # Process each coin (symbol) in the trade log
        for coin in self.trade_data["coin"].unique():
            symbol_trades = self.trade_data[self.trade_data["coin"] == coin]
            start_order_id = symbol_trades["oid"].min()
            end_order_id = symbol_trades["oid"].max()

            # Fetch orders within the specified orderId range
            symbol_orders = self._fetch_orders(coin, start_order_id, end_order_id)
            all_orders.extend(symbol_orders)

        # Convert orders to DataFrame and merge with trade data
        df_orders = pd.DataFrame(all_orders)
        df_orders["orderId"] = df_orders["orderId"].astype(int)
        df_merged = pd.merge(
            self.trade_data.add_prefix("log_"),
            df_orders.add_prefix("order_"),
            left_on=["log_coin", "log_oid"],
            right_on=["order_symbol", "order_orderId"],
            how="left",
        )

        df_merged["tick2OrderTime"] = df_merged["order_time"] - df_merged["log_exch_ts"]
        df_merged["tick2FirstUpdate"] = (
            df_merged["log_update_time"] - df_merged["log_exch_ts"]
        )
        df_merged["tick2LastUpdate"] = (
            df_merged["order_updateTime"] - df_merged["log_exch_ts"]
        )
        df_merged["recv2reply"] = (
            df_merged["log_ts_reply"] - df_merged["log_ts_recv"]
        )

        return df_merged


if __name__ == "__main__":
    # Usage example
    trade_log_path = "./trade.log"
    merger = BinanceTradeLogMerger(trade_log_path)
    merged_df = merger.merge_orders_with_trades()
    print(merged_df.head())
