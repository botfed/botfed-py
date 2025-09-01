import time
import logging
import posix_ipc
import struct
from queue import SimpleQueue as Queue
import threading
import traceback

# import monkey patched version, otherwise shared memory gets destroyed on exit even when create=False
from ..core import shared_memory
from .feed import Feed
from .shm_circ_buffer import CircularBuffer


from ..core.shm_constants import SIZE_PER_TICKER, RECORD_LEN, SYMBOL_LEN


# Try to acquire the lock with a custom timeout loop
def acquire_with_timeout(semaphore, timeout):
    start_time = time.time()
    while True:
        try:
            acquired = semaphore.acquire(0)  # Non-blocking attempt
            if acquired:
                return True
        except posix_ipc.BusyError:
            pass  # Semaphore is currently locked

        elapsed_time = time.time() - start_time
        if elapsed_time >= timeout:
            return False
        time.sleep(0)  # Sleep for a short period before retrying


class FastBBOFeed(Feed):

    def __init__(self, tickers, bbo, stop_event, shm_name="bin_bbo.out"):
        self.stop_event = stop_event
        self.shm_name = shm_name
        self.tickers = tickers
        self.bbo = bbo
        self.shm_name = shm_name
        self.read_time = {}
        self.buff = CircularBuffer(shm_name, RECORD_LEN * len(tickers) * 100)
        self.queue = Queue()
        self.thread = threading.Thread(target=self.run)
        self.thread.start()

    def run(self, sleep=0):
        while not self.stop_event.is_set():
            time.sleep(0)
            # Read data from shared memory
            t_start = time.time() * 1000
            # iterate over tickers and check if there is an update
            try:
                # self.lock[ticker].acquire(timeout=1)
                packed_data = self.buff.read(RECORD_LEN)
                if packed_data == b"":
                    sleep = min(1e-3, sleep * 10)
                    continue
                seq_num, symbol_bytes, b, B, a, A, T, E, ts_recv = struct.unpack(
                    f">Q{SYMBOL_LEN}sddddQQd", packed_data
                )
                symbol = symbol_bytes.decode("utf-8").strip()
                if self.read_time.get(symbol, 0) >= ts_recv:
                    sleep = min(1e-3, sleep * 10)
                    continue
                if t_start - ts_recv > 1:
                    sleep = min(1e-3, sleep * 10)
                    continue
                sleep = 1e-6
                self.read_time[symbol] = ts_recv
                t_put = time.time() * 1000
                self.queue.put_nowait(
                    {
                        "e": "bookTicker",
                        "s": symbol,
                        "b": b,
                        "B": B,
                        "a": a,
                        "A": A,
                        "T": T,
                        "E": E,
                        "u": seq_num,
                        "ts_recv": ts_recv,
                        "ts_feed_start": t_start,
                        "ts_feed_put": time.time() * 1000,
                    }
                )
                t_read = time.time() * 1000
                # print(t_read - t_put)
            except FileNotFoundError as e:
                logging.error(f"Shared memory not found {e}")
                return
            except Exception as e:
                traceback.print_exc()
                logging.error(f"Error reading data: {e}")
                continue
            finally:
                pass

    def run_ticks(self):
        # Read data from shared memory
        while not self.queue.empty():
            self.bbo.on_book_update(self.queue.get())

    def run_read(self):
        while not self.stop_event.is_set():
            # Read data from shared memory
            self.bbo.on_book_update(self.queue.get())

    def close(self):
        if self.thread:
            self.thread.join()
        logging.info("Closed FastBBOFeed")


if __name__ == "__main__":
    from threading import Event, Thread
    import signal

    from ..core.fast_bbo import FastBBO

    stop_event = Event()

    def signal_handler(signum, frame, stop_event):
        print("Signal received, shutting down...")
        stop_event.set()

    signal.signal(signal.SIGINT, lambda s, f: signal_handler(s, f, stop_event))

    # Order books
    obs = {"bin": FastBBO(lambda x: x)}
    # trade client
    symbols = ["1000PEPEUSDT", "WIFUSDT", "ETHFIUSDT", "ENAUSDT", "EIGENUSDT"]
    # setup shm bbo feeds
    fast_bbo_feed_bin = FastBBOFeed(
        symbols, obs["bin"], stop_event, shm_name="bin_bbo.out"
    )

    read = Thread(target=fast_bbo_feed_bin.run_read)
    read.start()
    read.join()
