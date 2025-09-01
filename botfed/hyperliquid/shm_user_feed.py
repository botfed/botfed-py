import logging
import posix_ipc
import orjson as json

# import monkey patched version, otherwise shared memory gets destroyed on exit even when create=False
from ..core import shared_memory

from .feed import Feed


class UserFeed(Feed):

    def __init__(
        self,
        shm_name_user="hyp_user_data",
        shm_name_orders="hyp_user_orders",
    ):
        self.shm_name_user = shm_name_user
        self.shm_name_orders = shm_name_orders
        self.lock_user = posix_ipc.Semaphore(f"/{self.shm_name_user}.sem")
        self.lock_orders = posix_ipc.Semaphore(f"/{self.shm_name_orders}.sem")
        try:
            print("Read lock value", self.lock_user.value)
            print("Write lock value", self.lock_orders.value)
        except AttributeError:
            pass
        self.last_update = {}
        self.last_update_orders = {}
        self.user_listeners = []
        self.orders_listeners = []

    def add_user_listener(self, listener):
        self.user_listeners.append(listener)

    def add_orders_listener(self, listener):
        self.orders_listeners.append(listener)

    def run_ticks(self):
        # Read data from shared memory
        self.run_user_data()
        self.run_user_orders()

    def run_user_data(self):
        data = None
        data_bytes = None
        try:
            self.lock_user.acquire(timeout=10)
            try:
                shm = shared_memory.SharedMemory(name=self.shm_name_user, create=False)
                data_bytes = bytes(shm.buf)
                shm.close()
            except FileNotFoundError:
                pass
            finally:
                self.lock_user.release()
            if not data_bytes:
                return
            timestamp = int.from_bytes(data_bytes[0:8])
            if timestamp <= self.last_update.get("user", 0):
                return
            self.last_update["user"] = timestamp
            len_enc = int.from_bytes(data_bytes[8:16])
            data = json.loads(data_bytes[16 : 16 + len_enc].decode("utf-8"))
        except Exception as e:
            logging.error(f"Error reading user data: {e}")
        if data:
            for listener in self.user_listeners:
                listener(data)

    def run_user_orders(self):
        data_bytes = None
        self.lock_orders.acquire(timeout=10)
        try:
            shm = shared_memory.SharedMemory(name=self.shm_name_orders, create=False)
            data_bytes = bytes(shm.buf)
            shm.close()
        except FileNotFoundError:
            pass
        except Exception as e:
            logging.error(f"Error reading user orders: {e}")
        finally:
            self.lock_orders.release()
        if not data_bytes:
            return
        timestamp = int.from_bytes(data_bytes[0:8])
        if timestamp <= self.last_update.get("user", 0):
            return
        self.last_update["user"] = timestamp
        len_enc = int.from_bytes(data_bytes[8:16])
        data = json.loads(data_bytes[16 : 16 + len_enc].decode("utf-8"))
        for listener in self.orders_listeners:
            listener(data)

    def close(self):
        print("Closing UserFeed", self.shm_name_user, self.shm_name_orders)
