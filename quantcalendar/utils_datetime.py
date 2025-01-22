import time
from datetime import datetime

# from numpy import datetime64


def pydt_from_second(sec: int) -> datetime:
    """
    Convert native timestamp to local naive datetime
    """
    y, m, d, hh, mm, ss, weekday, jday, dst = time.gmtime(sec)
    ss = min(ss, 59)  # clamp out leap seconds if the platform has them
    return datetime(y, m, d, hh, mm, ss, 0)


def pydt_from_millisecond(milli: int) -> datetime:
    """
    Convert native timestamp to local naive datetime
    """
    sec, millis = divmod(milli, 1000)
    y, m, d, hh, mm, ss, weekday, jday, dst = time.gmtime(sec)
    ss = min(ss, 59)  # clamp out leap seconds if the platform has them
    return datetime(y, m, d, hh, mm, ss, millis * 1000)


def pydt_from_microsecond(micro: int) -> datetime:
    """
    Convert native timestamp to local naive datetime
    """
    sec, micros = divmod(micro, 1000000)
    y, m, d, hh, mm, ss, weekday, jday, dst = time.gmtime(sec)
    ss = min(ss, 59)  # clamp out leap seconds if the platform has them
    return datetime(y, m, d, hh, mm, ss, micros)


def pydt_from_sec_list(seconds: list[int]) -> list[datetime]:
    """
    Convert list of native timestamp to list of local naive datetime
    """
    return [pydt_from_second(sec) for sec in seconds]


# def npydt_from_second(sec: int) -> datetime64:
#     """
#     Convert to a datetime64 representation of native timestamp
#     """
#     return datetime64(sec, "s")


# def npydt_from_sec_list(seconds: list[int]) -> list[datetime64]:
#     """
#     Convert to datetime64 representation of native timestamp in list
#     """
#     return [datetime64(sec, "s") for sec in seconds]
