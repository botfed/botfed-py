import websocket
import orjson as json
import time
import threading
import struct
from ..core.shm_constants import SYMBOL_LEN, BBO_STRUCT_FORMAT


SECONDS_IN_HOUR = 60 * 60
DISCONNECT_RECONNECT_TIME_SEC = 2 * SECONDS_IN_HOUR


messages_received = 0
start_time = time.time()


def websocket_listener(stop_event, writer, ticker_idx):
    """Listens to l2book on hyperliquid"""

    def on_message(ws, message):
        # record recv time
        ts_recv = time.time() * 1000
        global messages_received
        global start_time
        if stop_event.is_set():
            ws.close()
            return
        messages_received += 1
        # Deserialize JSON message
        data = json.loads(message)
        if data["channel"] != "l2Book":
            return
        # tag it with receive time
        data = data["data"]
        timestamp = int(data["time"])
        symbol = data["coin"]
        # Serialize JSON to string and encode to bytes
        bid, ask = data["levels"][0][0], data["levels"][1][0]
        # Write to shared memory
        symbol_bytes = symbol.encode("utf-8").ljust(SYMBOL_LEN)[:SYMBOL_LEN]
        packed = struct.pack(
            BBO_STRUCT_FORMAT,
            timestamp,
            symbol_bytes,
            float(bid["px"]),
            float(bid["sz"]),
            float(ask["px"]),
            float(ask["sz"]),
            timestamp,
            timestamp,
            ts_recv,
        )
        # send to writer for writing
        writer.write(symbol, packed)
        if time.time() - start_time >= 1:
            # print(
            #     f"Messages received this second (hyp) ({int(time.time() * 1000)}): {messages_received}, latency {tnow - timestamp:.6f} ms"
            # )
            start_time = time.time()
            messages_received = 0

    def on_error(ws, error):
        print("\nWebSocket error:", error)

    def on_close(ws, close_status_code, close_msg):
        print("WebSocket closed")

    def on_open(ws):
        print("WebSocket connection opened")
        # Setup the disconnect timer to run every 24 hours, check for stop event before closing
        # t = Process(target=timer_process, args=(DISCONNECT_RECONNECT_TIME_SEC, ws.close, stop_event))
        # t.start()
        msgs = [{"type": "l2Book", "coin": coin} for coin in ticker_idx]
        print(f"Subscribing to {len(msgs)} tickers on hyperliquid")
        for msg in msgs:
            ws.send(json.dumps({"method": "subscribe", "subscription": msg}))
        threading.Thread(target=shutdown_reconnect, args=(ws, stop_event)).start()

    def on_ping(ws, message):
        """Respond to ping messages from the server."""
        if stop_event.is_set():
            ws.close()
            return
        print("Ping (hyp) received and sending pong with payload: ", message)
        ws.send(message, opcode=websocket.ABNF.OPCODE_PONG)

    while True:
        # WebSocket app setup
        ws = websocket.WebSocketApp(
            "wss://api.hyperliquid.xyz/ws",
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_ping=on_ping,
        )
        ws.run_forever()
        time.sleep(1)  # Wait a bit before reconnecting
        if ws.sock:
            try:
                print("Forcefull shutdown of sock")
                # Shutdown socket connections
                ws.sock.shutdown()
            except Exception as e:
                print(f"Error while shutting down the socket: {e}")
            finally:
                # Close the socket directly
                if ws.sock:
                    ws.sock.close()
        if stop_event.is_set():
            break
        print("Attempting to reconnect...")


def shutdown_reconnect(ws, stop_event):
    start_time = time.time()
    while True:
        if stop_event.is_set():
            break
        time.sleep(1)
        if time.time() - start_time >= DISCONNECT_RECONNECT_TIME_SEC:
            print("Disconnecting and reconnecting...", int(time.time()))
            ws.close()
            return