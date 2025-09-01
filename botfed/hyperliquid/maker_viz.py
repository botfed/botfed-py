import time
import struct
from rich.console import Console
from rich.table import Table
from ..core import shared_memory
import psutil

# Get the current process object
p = psutil.Process()

# Set CPU affinity to CPU core 0 (replace '0' with the core number you want to use)
core_to_use = 0

try:
    p.cpu_affinity([core_to_use])
    print(f"Python script bound to CPU core {core_to_use}.")
except Exception as e:
    print(f"Error setting CPU affinity: {e}")

def read_from_shared_memory(name):

    ticker_length = 10  # Fixed length for ticker symbols
    record_length = ticker_length + 6 * 8  # Each record size in bytes

    console = Console()
    while True:
        num_tickers = 0
        packed_data = None
        data_bytes = None
        try:
            # Connect to the existing shared memory block
            shm = shared_memory.SharedMemory(name=name, create=False)
            # Read the number of tickers from the beginning of the shared memory
            data_bytes = bytes(shm.buf)
            shm.close()
            num_tickers = struct.unpack("Q", data_bytes[:8])[0]
        except FileNotFoundError:
            time.sleep(0.1)
            continue

        table = Table(title="Live Target Bid and Ask Prices and Quantities")
        table.add_column("Ticker", justify="center", style="cyan", no_wrap=True)
        table.add_column("Bid Price", justify="right", style="green")
        table.add_column("Ask Price", justify="right", style="red")
        table.add_column("Spread (bps)", justify="right", style="red")
        table.add_column("Price std (bps)", justify="right", style="red")
        table.add_column("Bid Quantity", justify="right", style="yellow")
        table.add_column("Ask Quantity", justify="right", style="magenta")
        table.add_column("Ntl", justify="right", style="magenta")

        for i in range(num_tickers):
            packed_data = data_bytes[8 + i * record_length : 8 + (i + 1) * record_length]
            # Unpack the ticker, bid_price, ask_price, bid_qty, and ask_qty from bytes
            ticker_bytes, bid_price, ask_price, bid_qty, ask_qty, price_std, ntl = (
                struct.unpack(f">{ticker_length}sdddddd", packed_data)
            )
            ticker = ticker_bytes.decode("utf-8").strip()
            spread_bps = (
                2 * (ask_price - bid_price) / (1e-6 + ask_price + bid_price) * 1e4
            )
            price_std_bps = 2 * price_std / (1e-6 + ask_price + bid_price) * 1e4
            table.add_row(
                ticker,
                f"{bid_price:.6f}",
                f"{ask_price:.6f}",
                f"{spread_bps:.6f}",
                f"{price_std_bps:.6f}",
                f"{bid_qty:.6f}",
                f"{ask_qty:.6f}",
                f"{ntl:.6f}",
            )

        console.clear()
        console.print(table)
        time.sleep(1)


if __name__ == "__main__":
    read_from_shared_memory("maker_shm")
