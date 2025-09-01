import logging
import posix_ipc
import orjson as json
from multiprocessing import Process, Event
from ..core import shared_memory
import time
import signal
import sys
import asyncio
import aiohttp

import time
from .hl_interface import setup
from ..core.shm_utils import create_shared_memory, delete_semaphore




url = "https://api.hyperliquid.xyz/info"


async def post(url, session, data):
    """Helper function to perform GET request."""
    async with session.post(url, json=data) as response:
        assert response.status == 200
        return await response.json()


async def poll_user_data(lock, session, stop_event, SHM_NAME, address, sleep=1):
    try:
        while True:
            if stop_event.is_set():
                break
            data = await post(
                url, session, {"type": "clearinghouseState", "user": address}
            )
            encoded_data = json.dumps(data)
            # Write to shared memory
            timestamp_bytes = int(time.time() * 1000).to_bytes(8, "big")
            lock.acquire(timeout=1)
            try:
                shm = shared_memory.SharedMemory(name=SHM_NAME)
                shm.buf[:8] = timestamp_bytes
                shm.buf[8:16] = len(encoded_data).to_bytes(8, "big")
                shm.buf[16 : 16 + len(encoded_data)] = encoded_data
                shm.close()
            finally:
                lock.release()
            await asyncio.sleep(sleep)
    except asyncio.CancelledError:
        print("Poll user data canceled.")
    except Exception as e:
        logging.error(f"Error polling user data: {e}")


async def poll_user_orders(lock, session, stop_event, SHM_NAME, address, sleep=1):
    try:
        while True:
            if stop_event.is_set():
                break
            data = await post(url, session, {"type": "openOrders", "user": address})
            encoded_data = json.dumps(data)
            # Write to shared memory
            timestamp_bytes = int(time.time() * 1000).to_bytes(8, "big")
            lock.acquire(timeout=1)
            try:
                shm = shared_memory.SharedMemory(name=SHM_NAME)
                shm.buf[:8] = timestamp_bytes
                shm.buf[8:16] = len(encoded_data).to_bytes(8, "big")
                shm.buf[16 : 16 + len(encoded_data)] = encoded_data
                shm.close()
            finally:
                lock.release()
            await asyncio.sleep(sleep)
    except asyncio.CancelledError:
        print("Poll user orders canceled.")
    except Exception as e:
        logging.error(f"Error polling user orders: {e}")


def signal_handler(signum, frame, stop_event):
    print("Signal received, shutting down...")
    stop_event.set()
    sys.exit(1)


class UserDataProducer:

    def __init__(self, shm_name="hyp_user", shm_size=102400):
        self.address, _, _ = setup(skip_ws=True)
        self.shm_name_user = shm_name + "_data"
        self.shm_name_orders = shm_name + "_orders"
        self.shm_size = shm_size

    async def _start(self, stop_event):
        delete_semaphore(f"/{self.shm_name_user}.sem")
        delete_semaphore(f"/{self.shm_name_orders}.sem")
        self.lock1 = posix_ipc.Semaphore(
            f"/{self.shm_name_user}.sem", flags=posix_ipc.O_CREAT, initial_value=1
        )
        self.lock2 = posix_ipc.Semaphore(
            f"/{self.shm_name_orders}.sem", flags=posix_ipc.O_CREAT, initial_value=1
        )

        shm1 = create_shared_memory(self.shm_name_user, self.shm_size)
        shm2 = create_shared_memory(self.shm_name_orders, self.shm_size)
        try:
            await self.run(stop_event)
        finally:
            # Cleanup shared memory when done
            print("Cleaning up shared memory", self.shm_name_user, self.shm_name_orders)
            shm1.close()
            shm1.unlink()
            shm2.close()
            shm2.unlink()

    async def run(self, stop_event):
        # Setup for catching SIGINT gracefully
        loop = asyncio.get_running_loop()
        stop = loop.create_future()

        def handle_sigint():
            stop.set_result(None)

        loop.add_signal_handler(signal.SIGINT, handle_sigint)
        async with aiohttp.ClientSession() as session:
            # Start both polling coroutines
            tasks = [
                asyncio.create_task(
                    poll_user_data(
                        self.lock1,
                        session,
                        stop_event,
                        self.shm_name_user,
                        self.address,
                    )
                ),
                asyncio.create_task(
                    poll_user_orders(
                        self.lock2,
                        session,
                        stop_event,
                        self.shm_name_orders,
                        self.address,
                    )
                ),
            ]
            await stop
            for task in tasks:
                task.cancel()
            # Wait for both tasks to complete (they won't in this setup unless cancelled)
            await asyncio.gather(*tasks, return_exceptions=True)
            print("User data loop shutdown complete")

    def start(self, stop_event):
        asyncio.run(self._start(stop_event))


if __name__ == "__main__":
    import dotenv

    dotenv.load_dotenv()
    # setup stop event
    stop_event = Event()
    signal.signal(signal.SIGINT, lambda s, f: signal_handler(s, f, stop_event))
    tickers = ["BTC", "ETH", "kPEPE"]
    producer = UserDataProducer()
    # Start the WebSocket listener process
    ws_process = Process(target=producer.start, args=(stop_event,))
    ws_process.start()
    # Wait for the processes to terminate (they won't in this script)
    ws_process.join()
