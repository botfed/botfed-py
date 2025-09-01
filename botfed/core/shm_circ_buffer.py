import numpy as np

from . import shared_memory
from .shm_utils import create_shared_memory


class CircularBuffer:

    def __init__(self, shm_name, buffer_size, overwrite=False):
        # Create shared memory segments for the buffer and indices
        self.shm_buffer = create_shared_memory(shm_name, buffer_size, overwrite=overwrite)
        self.buffer = np.ndarray(
            (buffer_size,), dtype=np.uint8, buffer=self.shm_buffer.buf
        )

        # Create another shared memory segment for the read and write indices
        self.shm_indices =  create_shared_memory(shm_name + "_idx", 8 * 2, overwrite=overwrite)
        self.indices = np.ndarray((2,), dtype=np.int64, buffer=self.shm_indices.buf)

        # Initialize read and write indices to 0
        self.indices[0] = 0  # write_index
        self.indices[1] = 0  # read_index

        self.buffer_size = buffer_size

    def write(self, data: bytes):
        data_len = len(data)
        if data_len > self.buffer_size:
            raise ValueError("Data size exceeds buffer size")

        # Load the current write and read indices
        write_index = self.indices[0]
        read_index = self.indices[1]

        # Check if we are overwriting unread data
        next_write_index = (write_index + data_len) % self.buffer_size
        if next_write_index < write_index and next_write_index >= read_index:
            print("Warning: Overwriting unread data")

        # Write data to the buffer (with wrapping)
        end_space = self.buffer_size - write_index
        if data_len <= end_space:
            self.buffer[write_index : write_index + data_len] = np.frombuffer(
                data, dtype=np.uint8
            )
        else:
            # Handle wrap-around
            self.buffer[write_index:] = np.frombuffer(data[:end_space], dtype=np.uint8)
            self.buffer[: data_len - end_space] = np.frombuffer(
                data[end_space:], dtype=np.uint8
            )

        # Update the write index in shared memory
        self.indices[0] = next_write_index

    def read(self, data_len: int) -> bytes:
        if data_len > self.buffer_size:
            raise ValueError("Data size exceeds buffer size")

        # Load the current write and read indices
        write_index = self.indices[0]
        read_index = self.indices[1]

        # Check if there is enough data to read
        if read_index == write_index:
            return b""  # No new data available

        # Read data from the buffer (with wrapping)
        end_space = self.buffer_size - read_index
        if data_len <= end_space:
            data = self.buffer[read_index : read_index + data_len].tobytes()
        else:
            # Handle wrap-around
            part1 = self.buffer[read_index:].tobytes()
            part2 = self.buffer[: data_len - end_space].tobytes()
            data = part1 + part2

        # Update the read index in shared memory
        self.indices[1] = (read_index + data_len) % self.buffer_size


        return data

    def close(self):
        self.shm_buffer.close()
        self.shm_buffer.unlink()
        self.shm_indices.close()
        self.shm_indices.unlink()
