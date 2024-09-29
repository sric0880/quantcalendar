import datetime

from .common import IntervalType


def is_opening_time(tm, open_period):
    total_sec = tm.hour * 3600 + tm.minute * 60 + tm.second + tm.microsecond / 1000000
    for start, _ in open_period:
        if start == total_sec:
            return True
    return False


def get_next_second_bartime(ticktime, interval):
    if ticktime.microsecond == 0:
        ticktime = ticktime - datetime.timedelta(seconds=1)
    else:
        ticktime = ticktime.replace(microsecond=0)
    while True:
        ticktime = ticktime + datetime.timedelta(seconds=1)
        if ticktime.second % interval == 0:
            return ticktime


def get_next_minute_bartime(ticktime, interval):
    if ticktime.second == 0 and ticktime.microsecond == 0:
        ticktime = ticktime - datetime.timedelta(minutes=1)
    else:
        ticktime = ticktime.replace(second=0, microsecond=0)
    while True:
        ticktime = ticktime + datetime.timedelta(minutes=1)
        if ticktime.minute % interval == 0:
            return ticktime


def get_next_bartime(
    dt,
    interval: int,
    interval_type: IntervalType,
    opentime=datetime.timedelta(),
    closetime=datetime.time(hour=0, minute=0, second=0),
):
    ticktime = dt

    if interval_type == IntervalType.SECOND:
        return get_next_second_bartime(ticktime, interval)

    elif interval_type == IntervalType.MINUTE:
        return get_next_minute_bartime(ticktime, interval)

    elif interval_type == IntervalType.HOUR:
        if ticktime.minute == 0 and ticktime.second == 0 and ticktime.microsecond == 0:
            ticktime = ticktime - datetime.timedelta(hours=1)
        else:
            ticktime = ticktime.replace(minute=0, second=0, microsecond=0)
        ticktime = ticktime - opentime
        while True:
            ticktime = ticktime + datetime.timedelta(hours=1)
            if ticktime.hour % interval == 0:
                next_bar_time = ticktime + opentime
                return next_bar_time

    elif interval_type == IntervalType.DAILY:
        if interval > 1:
            raise ValueError("Day interval only support 1 day")
        next_bar_time = ticktime.replace(
            hour=closetime.hour,
            minute=closetime.minute,
            second=closetime.second,
            microsecond=0,
        )
        if ticktime > next_bar_time:
            next_bar_time = next_bar_time + datetime.timedelta(days=1)
        return next_bar_time

    return None
