import argparse
import os
import json
import gzip
import threading
import time
import websocket
from datetime import datetime, timezone
import socket
from ..heartbeat.healthcheckio import HeartBeat

# --- Config ---
WS_URL = "wss://api.hyperliquid.xyz/ws"


def get_dir_path(symbol: str, date: datetime):
    data_dir = f"../data/hyperliquid_hft/{symbol}"
    year = date.strftime("%Y")
    month = date.strftime("%m")
    day = date.strftime("%d")
    return os.path.join(data_dir, year, month, day)


def get_fpath(feed_name: str, symbol: str, date: datetime):
    dir_path = get_dir_path(symbol, date)
    hour_tag = date.strftime("%Y%m%d_%H")
    return os.path.join(dir_path, f"{hour_tag}_{feed_name}.jsonl.gz")


class OrderbookSnapshotManager:
    feed_name = "l2snapshot"

    def __init__(self, symbol: str, interval_min: int = 15):
        self.symbol = symbol
        self.interval_sec = interval_min * 60
        self.hostname = socket.gethostname()
        self.running = True
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()

    def _run_loop(self):
        while self.running:
            try:
                now = datetime.now(timezone.utc)
                self.save_snapshot(now)
            except Exception as e:
                print("Snapshot error:", e)
            time.sleep(self.interval_sec)

    def save_snapshot(self, now: datetime):
        try:
            ws = websocket.create_connection("wss://api.hyperliquid.xyz/ws")
            payload = {"method": "book", "params": {"coin": self.symbol}}
            ws.send(json.dumps(payload))
            response = json.loads(ws.recv())
            ws.close()

            response["ts_recv_host"] = time.time() * 1e3
            response["host"] = self.hostname

            fpath = get_fpath(self.feed_name, self.symbol, now)
            os.makedirs(os.path.dirname(fpath), exist_ok=True)
            with gzip.open(fpath, "at") as f:
                f.write(json.dumps(response) + "\n")

            print(f"📸 Snapshot saved: {fpath}")
        except Exception as e:
            print("WebSocket snapshot error:", e)

    def stop(self):
        self.running = False
        self.thread.join(timeout=2)


# --- File Writers ---
class FeedWriter:
    def __init__(self, symbol: str, feed_name: str):
        self.symbol = symbol
        self.feed_name = feed_name
        self.buffer = []
        self.lock = threading.Lock()
        self.flush_interval = 5  # seconds
        self.running = True
        self.thread = threading.Thread(target=self._flush_loop, daemon=True)
        self.thread.start()

    def write(self, message):
        with self.lock:
            self.buffer.append(message)

    def _flush_loop(self):
        while self.running:
            time.sleep(self.flush_interval)
            self.flush()

    def flush(self):
        with self.lock:
            if not self.buffer:
                return
            now = datetime.now(tz=timezone.utc)
            fpath = get_fpath(self.feed_name, self.symbol, now)
            os.makedirs(os.path.dirname(fpath), exist_ok=True)
            with gzip.open(fpath, "at") as f:
                for msg in self.buffer:
                    f.write(json.dumps(msg) + "\n")
            self.buffer.clear()

    def stop(self):
        self.running = False
        self.flush()
        self.thread.join(timeout=2)


# --- WebSocket Listener ---
class HyperliquidDataCollector:
    def __init__(self, symbol: str, health_checker: HeartBeat = None):
        self.symbol = symbol
        self.health_checker = health_checker
        self.trade_writer = FeedWriter(symbol, "trades")
        self.bbo_writer = FeedWriter(symbol, "bbo")
        self.ob_writer = OrderbookSnapshotManager(symbol)
        self.hostname = socket.gethostname()

    def on_message(self, ws, message):
        tnow = time.time() * 1e3
        try:
            data = json.loads(message)
            chan = data["channel"]
            data["ts_recv_host"] = tnow
            data["host"] = self.hostname
            if chan == "trades":
                self.trade_writer.write(data)
            elif chan == "bbo":
                self.bbo_writer.write(data)

            if self.health_checker:
                self.health_checker.beat()
        except Exception as e:
            print("Parse error:", e)

    def on_open(self, ws):
        print("WebSocket opened.")
        payload1 = {
            "method": "subscribe",
            "subscription": {"type": "trades", "coin": self.symbol},
        }
        payload2 = {
            "method": "subscribe",
            "subscription": {"type": "bbo", "coin": self.symbol},
        }
        ws.send(json.dumps(payload1))
        ws.send(json.dumps(payload2))

    def on_error(self, ws, error):
        print("WebSocket error:", error)

    def on_close(self, ws, code, msg):
        print("WebSocket closed:", msg)

    def stop(self):
        self.running = False
        if hasattr(self, "ws") and hasattr(self.ws, "sock"):
            try:
                self.ws.keep_running = False
                self.ws.sock.shutdown()
                self.ws.sock.close()
            except Exception as e:
                print("Socket shutdown error:", e)
        self.trade_writer.stop()
        self.bbo_writer.stop()
        self.ob_writer.stop()

    def run(self):
        self.running = True
        while self.running:
            try:
                self.ws = websocket.WebSocketApp(
                    WS_URL,
                    on_open=self.on_open,
                    on_message=self.on_message,
                    on_error=self.on_error,
                    on_close=self.on_close,
                )
                self.ws.run_forever()
            except Exception as e:
                print("WebSocket crash, retrying in 3s:", e)
                time.sleep(3)


# --- Main Entrypoint ---
def main(symbols):
    collectors = []
    threads = []

    for symbol in symbols:
        # heart_beat = HeartBeat(
        #     url="https://hc-ping.com/<your-check-id>"
        # )
        heart_beat = None
        collector = HyperliquidDataCollector(symbol, health_checker=heart_beat)
        thread = threading.Thread(target=collector.run, daemon=True)
        threads.append(thread)
        collectors.append(collector)
        thread.start()
        print(f"Started data collector for {symbol}")

    try:
        while any(thread.is_alive() for thread in threads):
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Ctrl+C detected. Shutting down...")
        for collector in collectors:
            collector.stop()
        for thread in threads:
            thread.join(timeout=5)
        print("✅ Shutdown complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--symbols",
        nargs="+",
        type=str,
        default=["BTC", "ETH", "MOVE", "MELANIA", "AAVE"],
        help="Trading symbols to collect data for, e.g. ETH BTC DOGE",
    )
    args = parser.parse_args()
    main(args.symbols)
