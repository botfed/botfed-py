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
    """Listens to the Binance Futures bookTicker WebSocket and writes data to shared memory."""

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
        data = json.loads(message)["data"]
        # get latency
        symbol = data["s"]
        # Write to shared memory
        symbol_bytes = symbol.encode("utf-8").ljust(SYMBOL_LEN)[:SYMBOL_LEN]
        packed = struct.pack(
            BBO_STRUCT_FORMAT,
            data["u"],
            symbol_bytes,
            float(data["b"]),
            float(data["B"]),
            float(data["a"]),
            float(data["A"]),
            int(data["T"]),
            int(data["E"]),
            float(ts_recv),
        )
        ts_recv = time.time() * 1000
        writer.write(symbol, packed)
        if time.time() - start_time >= 1:
            print(
                f"Messages received this second (bin) ({int(time.time() * 1000)}): {messages_received}, latency {ts_recv - data['T']:.6f} ms"
            )
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
        threading.Thread(target=shutdown_reconnect, args=(ws, stop_event)).start()

    def on_ping(ws, message):
        """Respond to ping messages from the server."""
        if stop_event.is_set():
            ws.close()
            return
        print("Ping received (bin) and sending pong with payload: ", message)
        ws.send(message, opcode=websocket.ABNF.OPCODE_PONG)

    while True:
        # WebSocket app setup
        ws = websocket.WebSocketApp(
            "wss://fstream.binance.com/stream?streams="
            + "/".join([f"{ticker.lower()}@bookTicker" for ticker in ticker_idx]),
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
