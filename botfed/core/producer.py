from .feed_writer import SHMWriter, FileWriter, SHMWriterCircular


class Producer:

    def __init__(
        self,
        tickers,
        websocket_listener,
        output_mode="shared_memory",
        output_destination="hyp_bbo.out",
        shm_size_per_ticker=1024,
    ):
        self.tickers = tickers
        self.websocket_listener = websocket_listener
        self.output_mode = output_mode
        self.output_destination = output_destination
        self.shm_size_per_ticker = shm_size_per_ticker

    def start(self, stop_event):
        if self.output_mode == "shared_memory":
            self.writer = SHMWriterCircular(self.tickers, self.output_destination)
        elif self.output_mode == "file":
            self.writer = FileWriter(self.output_destination)
        else:
            raise ValueError("Invalid output mode")
        try:
            self.websocket_listener(stop_event, self.writer, self.tickers)
        finally:
            if self.output_mode == "shared_memory":
                print("Cleaning up shared memory", self.output_destination)
