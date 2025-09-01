import time as time_
import datetime as dt


gmtime = time_.gmtime

sim = False
_time_ms = 0


def set_time_ms(time_ms):
    global _time_ms
    if not sim:
        raise Exception("Cannot set time_ms in production")
    else:
        _time_ms = time_ms


def time():
    if sim:
        return _time_ms / 1000
    else:
        return time_.time()


def time_dt_utc():
    return dt.datetime.fromtimestamp(time(), tz=dt.timezone.utc)


def time_ms():
    if sim:
        return _time_ms
    else:
        return time_.time() * 1000


def time_rts():
    return time_.time()

    
def strftime(fmt, ct):
    return time_.strftime(fmt, ct)


def sleep(s):
    global _time_ms
    if sim:
        # global _time_ms
        _time_ms += s * 1000
    else:
        time_.sleep(s)
