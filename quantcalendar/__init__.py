from ._quantcalendar import PyCalendar as Calendar  # only used for annotation
from ._quantcalendar import PyCalendarAstock as CalendarAstock
from ._quantcalendar import PyCalendarCTP as CalendarCTP
from ._quantcalendar import PyTime7x24Calendar as Time7x24Calendar
from ._quantcalendar import RangeClosed
from ._quantcalendar import (
    combine,
    timedelta_ms,
    timedelta_ns,
    timedelta_s,
    timedelta_us,
    timestamp_ms,
    timestamp_ns,
    timestamp_s,
    timestamp_us,
    to_daily,
)
from .utils_datetime import (
    pydt_from_microsecond,
    pydt_from_millisecond,
    pydt_from_sec_list,
    pydt_from_second,
)


class bar_unit:
    sec = 1
    min = 60
    hour = 3600
    day = 86400
    week = 7 * 86400
    mon = 30 * 86400


__all__ = [
    "Calendar",
    "CalendarAstock",
    "CalendarCTP",
    "Time7x24Calendar",
    "bar_unit",
    "timedelta_s",
    "timedelta_ms",
    "timedelta_us",
    "timedelta_ns",
    "to_daily",
    "timestamp_s",
    "timestamp_ns",
    "timestamp_ms",
    "timestamp_us",
    "combine",
    "pydt_from_second",
    "pydt_from_millisecond",
    "pydt_from_microsecond",
    "pydt_from_sec_list",
]
