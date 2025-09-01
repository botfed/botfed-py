import pandas as pd
import json
from datetime import datetime, timedelta
from binance.client import Client
from dotenv import load_dotenv
import os
import argparse

# Load environment variables
load_dotenv()
api_key = os.getenv("BIN_API_KEY")
api_secret = os.getenv("BIN_API_SECRET")

# Initialize Binance client
client = Client(api_key, api_secret)


def get_recent_orders():
    """Get all futures orders from the last 1 days with pagination."""
    end_time = datetime.now()
    start_time = end_time - timedelta(days=1)
    all_orders = []
    last_order_id = None

    while True:
        # Retrieve up to 500 orders in each call
        if last_order_id:
            orders = client.futures_get_all_orders(
                limit=1000,
                orderId=last_order_id
            )
        else:
            orders = client.futures_get_all_orders(
                startTime=int(start_time.timestamp() * 1000),
                endTime=int(end_time.timestamp() * 1000),
                limit=1000,
            )
        
        if not orders:
            break  # Exit if no more orders are returned
        
        all_orders.extend(orders)
        
        # Update last_order_id to the ID of the last retrieved order
        last_order_id = orders[-1]["orderId"] + 1
        print(f"Retrieved {len(orders)} orders. Last order ID: {last_order_id}")

        # Stop if fewer than 500 orders were returned (no more orders available)
        if len(orders) < 1000:
            break

    return pd.DataFrame(all_orders)



# Load local JSON data
def load_local_data(file_path):
    with open(file_path, "r") as file:
        lines = file.readlines()
    data = [json.loads(line) for line in lines]
    return data


# Extract relevant information and check for failed cancellations
def process_local_data(data):
    processed_data = []
    for entry in data:
        # Check if 'resp' contains an error
        resp = entry.get("resp")
        if isinstance(resp, dict) and "error" in resp:
            error_message = resp["error"]
            client_order_id = entry["orders"][0].get("cloid")
            processed_data.append(
                {
                    "clientOrderId": client_order_id,
                    "status": "ERROR",
                    "error_message": error_message,
                    "coin": entry["orders"][0].get("coin"),
                    "qty": entry["orders"][0].get("qty"),
                    "side": entry["orders"][0].get("side"),
                    "price": entry["orders"][0].get("price"),
                    "cancellation_attempt": (entry["type"] == "cancel"),
                }
            )
        else:
            for order in entry.get("orders", []):
                client_order_id = order.get("cloid")
                status = order.get("status")
                cancellation_attempt = (
                    entry["type"] == "cancel" and status == "pending_cancel"
                )
                processed_data.append(
                    {
                        "clientOrderId": client_order_id,
                        "status": (
                            resp[0]["status"]
                            if resp and isinstance(resp, list)
                            else "UNKNOWN"
                        ),
                        "update_time": (
                            resp[0].get("update_time")
                            if resp and isinstance(resp, list)
                            else None
                        ),
                        "coin": order.get("coin"),
                        "qty": order.get("qty"),
                        "side": order.get("side"),
                        "price": order.get("price"),
                        "cancellation_attempt": cancellation_attempt,
                        "error_message": None,
                    }
                )
    return pd.DataFrame(processed_data)


# Merge Binance orders with local data on 'clientOrderId'
def merge_data(binance_df, local_df):
    merged_df = pd.merge(
        binance_df,
        local_df,
        on="clientOrderId",
        how="inner",
        suffixes=("_binance", "_local"),
    )
    return merged_df


# Main execution
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Merge Binance futures orders with local JSON data."
    )
    parser.add_argument(
        "file_path", type=str, help="Path to the local JSON file with order data"
    )
    args = parser.parse_args()

    binance_orders_df = get_recent_orders()
    local_data_df = process_local_data(load_local_data(args.file_path))
    print(local_data_df)
    print(binance_orders_df)
    merged_df = merge_data(binance_orders_df, local_data_df)

    # Display merged data
    print(merged_df)
