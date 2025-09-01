import json
import threading
import time
import dotenv
import os
import websocket

from queue import Queue
from binance.client import Client

from ..core.feed import Feed
from ..logger import get_logger


logger = get_logger(__name__)

dotenv.load_dotenv()

# Replace with your Binance API Key and Secret
api_key = os.getenv("BIN_API_KEY")
api_secret = os.getenv("BIN_API_SECRET")


import requests

BINANCE_FAPI_URL = "https://fapi.binance.com"


def get_listen_key(api_key):
    headers = {"X-MBX-APIKEY": api_key}
    resp = requests.post(f"{BINANCE_FAPI_URL}/fapi/v1/listenKey", headers=headers)
    resp.raise_for_status()
    return resp.json()["listenKey"]


class UserFeed(Feed):

    def __init__(self, stop_event):
        self.stop_event = stop_event
        self.api_key = api_key
        self.api_secret = api_secret
        # Initialize the client
        self.client = Client(
            self.api_key, self.api_secret, testnet=False
        )  # Set testnet=True for testnet trading
        self.queue = Queue()
        self.thread = None
        Feed.__init__(self)

    def poll(self):
        while not self.stop_event.is_set():
            self.fetch_initial_data()
            time.sleep(3)
        logger.info("UserFeed Poller stopped")

    def start(self):
        listen_key = get_listen_key(self.api_key)
        ws_url = f"wss://fstream.binance.com/ws/{listen_key}"

        self.ws = websocket.WebSocketApp(
            ws_url,
            on_message=self.handle_user_data,
            on_close=self.on_close,
            on_error=self.on_error,
        )

        self.t = threading.Thread(target=self._run)
        self.t.start()

    def on_close(self, ws, close_status_code, close_msg):
        logger.info("WebSocket closed.")

    def on_error(self, ws, error):
        logger.error(f"WebSocket error: {error}")

    def _run(self):
        while not self.stop_event.is_set():
            try:
                self.ws.run_forever(ping_interval=60)
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
            time.sleep(5)  # Retry delay

    # Function to fetch initial account balances, open orders, and positions
    def fetch_initial_data(self):
        try:

            # Fetch futures account balances
            account_info = self.client.futures_account()
            account_info["positions"] = [
                pos
                for pos in account_info["positions"]
                if float(pos.get("positionAmt", 0)) != 0
            ]
            self.queue.put(
                {"type": "bin_account_info", "data": account_info, "ts": time.time()}
            )

            # Fetch open orders
            open_orders = self.client.futures_get_open_orders()
            self.queue.put(
                {"type": "bin_open_orders", "data": open_orders, "ts": time.time()}
            )

        except Exception as e:
            logger.error(f"An error occurred while fetching initial data: {e}")

    # Function to handle incoming user events via WebSocket
    def handle_user_data(self, _, msg):
        self.queue.put({"type": "bin_user_event", "data": json.loads(msg), "ts": time.time()})

    # Main function to start the thread
    def run(self):
        # Fetch the initial account balance before starting the WebSocket listener
        self.fetch_initial_data()
        # Start WebSocket in a separate thread
        self.t_poller = threading.Thread(target=self.poll)
        self.t_poller.start()
        self.start()

    def run_ticks(self):
        while not self.queue.empty():
            tick = self.queue.get()
            for listener in self.listeners:
                listener(tick)

    def close(self):
        self.ws.close()
        self.t.join()
        self.t_poller.join()
        logger.info("UserFeed: Closed UserFeed.")


# Run the main function
if __name__ == "__main__":
    user_feed = UserFeed()
    user_feed.run()
    user_feed.thread.join()
