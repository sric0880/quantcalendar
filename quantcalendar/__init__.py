from ._quantcalendar import PyCalendarAstock as CalendarAstock
from ._quantcalendar import PyCalendarCTP as CalendarCTP


class bar_unit:
    sec = 1
    min = 60
    hour = 3600
    day = 86400
    week = 7 * 86400
    mon = 30 * 86400


__all__ = ["CalendarAstock", "CalendarCTP", "bar_unit"]
