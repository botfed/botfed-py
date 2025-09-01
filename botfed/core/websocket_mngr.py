import time
import websocket
import threading
from threading import Event
import socket
import logging
import traceback

# websocket.enableTrace(True)


class WebsocketManager:

    def __init__(self, websocket_url, timeout_threshold=10, warn_threshold=1, exit_event=None):
        self.websocket_url = websocket_url
        self.warn_threshold = warn_threshold
        hostname = self.websocket_url.split("//")[1].split("/")[0]
        ip_address = socket.gethostbyname(hostname)
        logging.info(f"Connecting to {hostname} with IP address {ip_address}")
        self.stop_event = Event()
        self.exit_event = exit_event

        self.start_ws()

        self.timeout_threshold = timeout_threshold
        self.last_message_time = time.time()
        self.timer_thread = threading.Thread(target=self.check_timeout)
        self.timer_thread.start()
        self.close_count = 0
        self.msg_count = 0
        self.start_ts = time.time()
        self.close_stats_thread = threading.Thread(target=self.print_close_stats)
        self.close_stats_thread.start()

    def start_ws(self):
        self.ws = websocket.WebSocketApp(
            self.websocket_url,
            on_message=self._on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open,
        )

        self.thread = threading.Thread(target=self.run_forever)
        self.thread.start()

    def _on_message(self, ws, message):
        self.msg_count += 1
        if self.exit_event and self.exit_event.is_set():
            self.ws.close()
            return
        try:
            self.last_message_time = time.time()
            self.on_message(ws, message)
        except Exception as e:
            traceback.print_exc()
            logging.error("Error processing message: %s" % e)

    def on_message(self, ws, message):
        raise NotImplementedError

    def on_open(self, ws):
        """On open connection"""
        logging.info("### OPENED CONNECTION ###")
        try:
            if self.ws and self.ws.sock and self.ws.sock.connected:
                self.resubscribe()
        except Exception as e:
            logging.error("Error resubscribing: %s" % e)
            traceback.print_exc()

    def on_error(self, ws, error):
        """On error"""
        logging.error("on_error: %s" % error)

    def on_close(self, *args):
        """On close connection"""
        logging.info("### CLOSED CONNECTION ###")
        self.close_count += 1
        close_per_minute = self.close_count / ((time.time() - self.start_ts) / 60)
        logging.info(
            f"Close count: {self.close_count} Close per minute: {close_per_minute: .2f}"
        )
        if not self.stop_event.is_set():
            self.restart_connection()

    def run_forever(self):
        if self.ws and self.ws.sock and self.ws.sock.connected:
            try:
                self.ws.close(timeout=10)
            except Exception as e:
                logging.error("Error closing websocket connection: %s" % e)
        self.ws.run_forever()

    def restart_connection(self, wait_time=10):
        """Only called from inside the ws thread via on close."""
        logging.info("### RESTARTING CONNECTION ###")
        try:
            self.ws.close()
        except Exception as e:
            logging.error("Error closing websocket connection: %s" % e)
        self.ws = websocket.WebSocketApp(
            self.websocket_url,
            on_message=self._on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open,
        )
        self.run_forever()

    def resubscribe(self):
        pass

    def restart_thread(self):
        """Called from outside the ws thread"""
        self.ws.close()
        print("Restarting thread")
        self.thread.join()
        print("Theead joined")
        self.stop_event.clear()
        self.start_ws()

    def print_close_stats(self):
        while True:
            logging.info(f"Close count: {self.close_count}")
            logging.info(
                f"Close per minute: {self.close_count / ((time.time() - self.start_ts) / 60): .2f}"
            )
            msg_per_second = self.msg_count / ((time.time() - self.start_ts))
            logging.info(f"Avg msg per second: {msg_per_second: .2f}")
            total_runtime_minutes = (time.time() - self.start_ts) / 60
            logging.info(f"Total runtime: {total_runtime_minutes: .2f} minutes")
            time.sleep(60)

    def check_timeout(self):
        """Check if the connection has timed out"""
        while True:
            time_since_last_message = time.time() - self.last_message_time
            if time_since_last_message > self.timeout_threshold:
                logging.warning(
                    "No message received for the threshold period, restarting connection."
                )
                self.last_message_time = time.time()
                self.stop_event.set()
                self.restart_thread()
            elif time_since_last_message >= self.warn_threshold:
                logging.warning(f"No message received in last {self.warn_threshold} second(s) ..")
            time.sleep(1)  # Check every second
