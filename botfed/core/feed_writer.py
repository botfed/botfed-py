import logging
import posix_ipc
from .shm_constants import RECORD_LEN, SIZE_PER_TICKER
from .shm_utils import create_shared_memory, delete_semaphore
from .shm_circ_buffer import CircularBuffer


class SHMWriterCircular:

    def __init__(
        self,
        tickers,
        shm_name,
    ):
        self.tickers = tickers
        buff_size = RECORD_LEN * len(tickers) * 100
        self.buffer = CircularBuffer(shm_name, buff_size)

    def write(self, _: str, packed: bytes):
        self.buffer.write(packed)


class SHMWriter:

    def __init__(
        self,
        tickers,
        shm_name,
        size_per_ticker=SIZE_PER_TICKER,
    ):
        self.ticker_idx = {ticker: idx for idx, ticker in enumerate(tickers)}
        self.shm_name = shm_name
        self.lock = {}
        self.size_per_ticker = size_per_ticker
        self.shm = {}
        for ticker in tickers:
            shm_name = f"{self.shm_name}_{ticker}"
            sem_name = f"{shm_name}.sem"
            delete_semaphore(sem_name)
            self.lock[ticker] = posix_ipc.Semaphore(
                sem_name,
                flags=posix_ipc.O_CREAT,
                initial_value=1,
            )
            self.shm[ticker] = create_shared_memory(shm_name, self.size_per_ticker)

    def write(self, symbol, packed):
        try:
            # self.lock[symbol].acquire(timeout=1)
            self.shm[symbol].buf[0:RECORD_LEN] = packed
        except Exception as e:
            logging.error(f"Error writing to shared memory: {e}")
        finally:
            # self.lock[symbol].release()
            pass


class FileWriter:
    def __init__(self, file_name, overwrite=True):
        self.file_name = file_name
        self.fh = open(file_name, "wb" if overwrite else "ab")

    def write(self, _, packed):
        self.fh.write(packed + b"\n")
