from datetime import datetime, timezone

from ._quantcalendar import PyCalendar as Calendar  # only used for annotation
from ._quantcalendar import PyCalendarAstock as CalendarAstock
from ._quantcalendar import PyCalendarCTP as CalendarCTP
from ._quantcalendar import PyTime7x24Calendar as Time7x24Calendar


class bar_unit:
    sec = 1
    min = 60
    hour = 3600
    day = 86400
    week = 7 * 86400
    mon = 30 * 86400


def to_seconds(year, month, day, hour=0, minute=0, second=0):
    """return integer seconds"""
    dt = datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)
    return int(dt.timestamp())


def to_timepoint(year, month, day, hour=0, minute=0, second=0, microsecond=0):
    """return float timestamp"""
    return datetime(
        year,
        month,
        day,
        hour,
        minute,
        second,
        microsecond=microsecond,
        tzinfo=timezone.utc,
    ).timestamp()


__all__ = [
    "Calendar",
    "CalendarAstock",
    "CalendarCTP",
    "Time7x24Calendar",
    "to_seconds",
    "to_timepoint",
    "bar_unit",
]
