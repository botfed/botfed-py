import logging
import orjson as json
import posix_ipc
from . import shared_memory


def create_shared_memory(SHM_NAME, SHM_SIZE, overwrite=False):
    try:
        # Try to create a new shared memory segment
        shm = shared_memory.SharedMemory(name=SHM_NAME, create=True, size=SHM_SIZE)
    except FileExistsError:
        # If it already exists, unlink (remove) it and recreate it
        if overwrite:
            existing_shm = shared_memory.SharedMemory(name=SHM_NAME)
            existing_shm.unlink()
            shm = shared_memory.SharedMemory(name=SHM_NAME, create=True, size=SHM_SIZE)
        else:
            shm = shared_memory.SharedMemory(name=SHM_NAME)
    return shm


def delete_semaphore(sem_name):
    try:
        # Attempt to open the semaphore
        semaphore = posix_ipc.Semaphore(sem_name)
        # Unlink (delete) the semaphore
        semaphore.unlink()
        print(f"Semaphore {sem_name} deleted.")
    except posix_ipc.ExistentialError:
        # Semaphore does not exist
        print(f"Semaphore {sem_name} does not exist.")


class SHMJsonWriter:

    def __init__(
        self,
        shm_name,
        num_slots,
        size_per_slot,
    ):
        self.shm_name = shm_name
        self.size_per_slot = size_per_slot
        self.shm_size = self.size_per_slot * num_slots + 128
        delete_semaphore(f"/{self.shm_name}.sem")
        self.lock = posix_ipc.Semaphore(
            f"/{self.shm_name}.sem", flags=posix_ipc.O_CREAT, initial_value=1
        )

    def write(self, slot, data: {}):
        self.lock.acquire(timeout=1)
        idx = slot * self.size_per_slot
        data_enc = json.dumps(data)
        try:
            shm = shared_memory.SharedMemory(name=self.shm_name)
            shm.buf[idx : idx + 8] = int.to_bytes(len(data_enc), 8, "big")
            shm.buf[idx + 8 : idx + 8 + len(data_enc)] = data_enc
            shm.close()
        except Exception as e:
            logging.error(f"Error writing to shared memory: {e}")
        finally:
            self.lock.release()


class SHMJsonReader:

    def __init__(
        self,
        shm_name,
        num_slots,
        size_per_slot,
    ):
        self.shm_name = shm_name
        self.size_per_slot = size_per_slot
        self.shm_size = self.size_per_slot * num_slots + 128
        self.lock = posix_ipc.Semaphore(f"/{self.shm_name}.sem")

    def read(self):
        data = []
        buf = None
        self.lock.acquire(timeout=1)
        try:
            shm = shared_memory.SharedMemory(name=self.shm_name)
            buf = bytes(shm.buf)
            shm.close()
        except Exception as e:
            logging.error(f"Error writing to shared memory: {e}")
        finally:
            self.lock.release()
        if not buf:
            return data
        for slot in range(self.num_slots):
            idx = slot * self.size_per_slot
            len_enc = int.from_bytes(buf[idx : idx + 8], "big")
            data_enc = buf[idx + 8 : idx + 8 + len_enc]
            data.append(json.loads(data_enc))
        return data
