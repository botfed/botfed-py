import signal
from threading import Event

from ..logger import get_logger


logger = get_logger(__name__)

def signal_handler(signum, frame, stop_event):
    logger.info("Signal received, shutting down...")
    stop_event.set()

stop_event = Event()
signal.signal(signal.SIGINT, lambda s, f: signal_handler(s, f, stop_event))