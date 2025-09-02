# time.py
import time as _time
import datetime as dt
import threading

gmtime = _time.gmtime

sim = False
_MS_PER_S = 1000

# Protects access to _time_ms
_lock = threading.RLock()
_time_ms: int = 0


def set_time_ms(time_ms: int):
    global _time_ms
    if not sim:
        raise Exception("Cannot set time_ms in production")
    with _lock:
        _time_ms = int(time_ms)


def time() -> float:
    if sim:
        with _lock:
            return _time_ms / _MS_PER_S
    else:
        return _time.time()


def time_dt_utc() -> dt.datetime:
    return dt.datetime.fromtimestamp(time(), tz=dt.timezone.utc)


def time_ms() -> int:
    if sim:
        with _lock:
            return _time_ms
    else:
        return int(_time.time() * _MS_PER_S)


def time_rts() -> float:
    """Real-time seconds (wall clock), regardless of sim."""
    return _time.time()


def strftime(fmt, ct) -> str:
    return _time.strftime(fmt, ct)


def sleep(s: float):
    global _time_ms
    if sim:
        inc = int(s * _MS_PER_S)
        with _lock:
            _time_ms += inc
    else:
        _time.sleep(s)


def now_utc() -> dt.datetime:
    return time_dt_utc()
