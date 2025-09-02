# logger.py
import logging
import os

from .core import time as clock

PKG_ROOT = "botfed"


class SimFormatter(logging.Formatter):
    """Render %(asctime)s using our sim/real clock."""

    def formatTime(self, record, datefmt=None):
        ts = clock.time()
        dt = clock.dt.datetime.fromtimestamp(ts, tz=clock.dt.timezone.utc)
        return (
            dt.strftime(datefmt)
            if datefmt
            else dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")
        )


_CONFIGURED = False


def _running_under_pytest() -> bool:
    # heuristic; flip with env if needed
    return "PYTEST_CURRENT_TEST" in os.environ or os.getenv("SIM_LOG_CAPTURE") == "1"


def _ensure_pkg_logger(level=logging.INFO):
    """Configure ONLY our package root logger once; leave root/uvicorn/pytest alone."""
    global _CONFIGURED
    if _CONFIGURED:
        return

    pkg_logger = logging.getLogger(PKG_ROOT)

    if _running_under_pytest():
        # Let pytest's caplog capture via root handlers.
        # Do NOT add our own handler; just ensure level is set.
        pkg_logger.setLevel(level)
        pkg_logger.propagate = True
    else:
        # Normal runtime (incl. uvicorn): attach our own handler + formatter
        if not pkg_logger.handlers:
            h = logging.StreamHandler()
            h.setFormatter(
                SimFormatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            )
            pkg_logger.addHandler(h)
        pkg_logger.setLevel(level)
        pkg_logger.propagate = False  # avoid duplicate lines via root/uvicorn

    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    """Use get_logger(__name__) everywhere in our package."""
    _ensure_pkg_logger()
    return logging.getLogger(name)
