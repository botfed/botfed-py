import traceback
import threading
import requests
import queue
import websocket
import json
import time
import logging
from ..core.signal_and_stop import stop_event
from ..core.feed import Feed


from .hl_interface import setup

logger = logging.getLogger(__name__)

import logging
logging.getLogger("websocket").setLevel(logging.WARNING)



class UserFeed(Feed):
    def __init__(
        self,
        address: str,
        stop_event: threading.Event,
        base_url: str = "wss://api.hyperliquid.xyz/ws",
        queue_size=1000,
    ):
        self.address = address
        self.base_url = base_url
        self.q = queue.Queue(maxsize=queue_size)
        self._stop_event = stop_event
        self.t_ws = threading.Thread(target=self._run_ws, daemon=True)
        self.t_poll = threading.Thread(target=self._poll, daemon=True)
        self.t_ping = threading.Thread(target=self._ping, daemon=True)
        self.t_cleanup = threading.Thread(target=self._cleanup, daemon=True)
        self.ws = None
        Feed.__init__(self)

    def run(self):
        logger.info("UserFeed started")
        self.t_ws.start()
        self.t_poll.start()
        self.t_ping.start()
        self.t_cleanup.start()

    def _cleanup(self):
        while not self._stop_event.is_set():
            time.sleep(0.1)
        self.close()
        logger.info("Cleanup")

    def get_event(self, timeout=None):
        try:
            return self.q.get(timeout=timeout)
        except queue.Empty:
            return None

    def _poll(self):
        last_poll = 0
        while not self._stop_event.is_set():
            ts = time.time() * 1e3
            if ts >= last_poll + 10e3:
                last_poll = ts
                try:
                    resp = requests.post(
                        "https://api.hyperliquid.xyz/info",
                        json={"type": "clearinghouseState", "user": self.address},
                        headers={"Content-Type": "application/json"},
                    )
                    data = resp.json()
                    self.q.put({"type": "hl_account_info", "data": data, "ts": ts})
                except Exception as e:
                    logger.warning(f"Polling error: {e}")
            time.sleep(0.01)

    def _ping(self):
        time.sleep(10)
        last_ping = 0
        while not self._stop_event.is_set():
            if time.time() > last_ping + 50 and self.ws:
                last_ping = time.time()
                try:
                    self.ws.send(json.dumps({"method": "ping"}))
                except Exception as e:
                    logger.warning(f"Ping failed: {e}")
            time.sleep(0.01)
        logger.info("Run ping done")

    def _run_ws(self):
        def on_message(ws, message):
            try:
                data = json.loads(message)
                if data.get("channel") == "userEvents":
                    try:
                        self.q.put(data["data"], block=False)
                    except queue.Full:
                        logger.warning(
                            "UserFeed queue is full — dropped incoming WebSocket event."
                        )
            except Exception as e:
                logger.exception("Error processing message: %s", message)

        def on_error(ws, error):
            logger.warning("WebSocket error: %s", error)

        def on_close(ws, close_status_code, close_msg):
            logger.info("WebSocket closed: %s %s", close_status_code, close_msg)

        def on_open(ws):
            if not ws:
                return
            logger.info("WebSocket opened")
            sub_msg = {
                "method": "subscribe",
                "subscription": {"type": "userEvents", "user": self.address},
            }
            ws.send(json.dumps(sub_msg))
            # sub_msg = {
            #     "method": "subscribe",
            #     "subscription": {"type": "userEvents", "user": self.address},
            # }
            # ws.send(json.dumps(sub_msg))

        while not self._stop_event.is_set():
            try:
                self.ws = websocket.WebSocketApp(
                    self.base_url,
                    on_open=on_open,
                    on_message=on_message,
                    on_error=on_error,
                    on_close=on_close,
                )
                self.ws.run_forever()
            except Exception as e:
                logger.exception("WebSocket connection error")
                time.sleep(5)
        logger.info("run ws done")

    def stop(self):
        logger.info("Running stop")
        self._stop_event.set()
        if self.ws:
            self.ws.keep_running = False  # tell run_forever to exit
            self.ws.close()  # forcibly break socket loop
        if self.t_ws.is_alive():
            self.t_ws.join()
        if self.t_poll.is_alive():
            self.t_poll.join()
        if self.t_ping.is_alive():
            self.t_ping.join()
        # self.t_cleanup.join()
        logger.info("All stopped")

    def run_ticks(self):
        while not self.q.empty():
            tick = self.q.get()
            for listener in self.listeners:
                listener(tick)


if __name__ == "__main__":
    import os
    import dotenv

    dotenv.load_dotenv()
    # Setup
    address, info, exchange = setup(
        os.environ["HEDGER_HYPER_EOA"], os.environ["HEDGER_HYPER_SECRET"]
    )

    def on_event(event):
        print("Received user event:", event)

    # Start the user feed
    feed = UserFeed(address, stop_event)
    feed.add_listener(on_event)
    feed.run()

    while not stop_event.is_set():
        feed.run_ticks()
        time.sleep(0.01)
