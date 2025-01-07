# cython: language_level=3
# distutils: language = c++
# distutils: sources = src/calendar.cpp src/calendar_data.cpp
from cython.operator cimport preincrement, predecrement
from cython.operator cimport dereference as deref
from libcpp.pair cimport pair

from ._quantcalendar cimport tp, seconds
from ._quantcalendar cimport Calendar, CalendarData, CalendarAstock
# from ._quantcalendar cimport CalendarCTP
# from ._quantcalendar cimport Time7x24Calendar

ctypedef CalendarData.tradedays_iterator tradedays_iterator
ctypedef CalendarData.month_end_iterator month_end_iterator
ctypedef CalendarData.month_begin_iterator month_begin_iterator
ctypedef CalendarData.week_end_iterator week_end_iterator
ctypedef CalendarData.week_begin_iterator week_begin_iterator
ctypedef CalendarData.weekday_iterator weekday_iterator

_maxsize = 2**32-1
cdef class PyCalendar:
    cdef const Calendar* c_cal

    def get_tradedays_gte(self, dt: int, count: int=_maxsize):
        ret = []
        cdef tradedays_iterator it = self.c_cal.TradedaysUpper(dt)
        while not it.is_end() and count > 0:
            ret.append(deref(it).first)
            preincrement(it)
            count -= 1
        return ret

    def get_tradedays_lte(self, dt: int, count: int=_maxsize):
        ret = []
        cdef tradedays_iterator it = self.c_cal.TradedaysLower(dt)
        while not it.is_end() and count > 0:
            ret.append(deref(it).first)
            predecrement(it)
            count -= 1
        ret.reverse()
        return ret

    def get_tradedays_between(self, start: int, end: int):
        ret = []
        if (start >= end):
            return ret
        cdef pair[tradedays_iterator, tradedays_iterator] p = self.c_cal.TradedaysBetween(start, end)
        if (p.first.is_end() or p.second.is_end()):
            return ret
        while (p.first <= p.second):
            ret.append(deref(p.first).first)
            preincrement(p.first)
        return ret

    def get_tradeday_next(self, dt: int):
        cdef tradedays_iterator it = self.c_cal.TradedaysUpper(dt)
        return None if it.is_end() else deref(it).first

    def get_tradeday_last(self, dt: int):
        cdef tradedays_iterator it = self.c_cal.TradedaysLower(dt)
        return None if it.is_end() else deref(it).first

    def get_month_ends_gte(self, dt: int, count: int = _maxsize):
        ret = []
        cdef month_end_iterator it = self.c_cal.MonthEndUpper(dt)
        while not it.is_end() and count > 0:
            ret.append(deref(it).first)
            preincrement(it)
            count -= 1
        return ret

    def get_month_ends_lte(self, dt: int, count: int = _maxsize):
        ret = []
        cdef month_end_iterator it = self.c_cal.MonthEndLower(dt)
        while not it.is_end() and count > 0:
            ret.append(deref(it).first)
            predecrement(it)
            count -= 1
        ret.reverse()
        return ret

    def get_month_ends_between(self, start: int, end: int):
        ret = []
        if (start >= end):
            return ret
        cdef pair[month_end_iterator, month_end_iterator] p = self.c_cal.MonthEndBetween(start, end)
        if (p.first.is_end() or p.second.is_end()):
            return ret
        while (p.first <= p.second):
            ret.append(deref(p.first).first)
            preincrement(p.first)
        return ret

    def get_month_end_next(self, dt: int):
        cdef month_end_iterator it = self.c_cal.MonthEndUpper(dt)
        return None if it.is_end() else deref(it).first

    def get_month_end_last(self, dt: int):
        cdef month_end_iterator it = self.c_cal.MonthEndLower(dt)
        return None if it.is_end() else deref(it).first

    def get_month_begins_gte(self, dt: int, count: int = _maxsize):
        ret = []
        cdef month_begin_iterator it = self.c_cal.MonthBeginUpper(dt)
        while not it.is_end() and count > 0:
            ret.append(deref(it).first)
            preincrement(it)
            count -= 1
        return ret

    def get_month_begins_lte(self, dt: int, count: int = _maxsize):
        ret = []
        cdef month_begin_iterator it = self.c_cal.MonthBeginLower(dt)
        while not it.is_end() and count > 0:
            ret.append(deref(it).first)
            predecrement(it)
            count -= 1
        ret.reverse()
        return ret

    def get_month_begins_between(self, start: int, end: int):
        ret = []
        if (start >= end):
            return ret
        cdef pair[month_begin_iterator, month_begin_iterator] p = self.c_cal.MonthBeginBetween(start, end)
        if (p.first.is_end() or p.second.is_end()):
            return ret
        while (p.first <= p.second):
            ret.append(deref(p.first).first)
            preincrement(p.first)
        return ret

    def get_month_begin_next(self, dt: int):
        cdef month_begin_iterator it = self.c_cal.MonthBeginUpper(dt)
        return None if it.is_end() else deref(it).first

    def get_month_begin_last(self, dt: int):
        cdef month_begin_iterator it = self.c_cal.MonthBeginLower(dt)
        return None if it.is_end() else deref(it).first

    def get_week_ends_gte(self, dt: int, count: int = _maxsize):
        ret = []
        cdef week_end_iterator it = self.c_cal.WeekEndUpper(dt)
        while not it.is_end() and count > 0:
            ret.append(deref(it).first)
            preincrement(it)
            count -= 1
        return ret

    def get_week_ends_lte(self, dt: int, count: int = _maxsize):
        ret = []
        cdef week_end_iterator it = self.c_cal.WeekEndLower(dt)
        while not it.is_end() and count > 0:
            ret.append(deref(it).first)
            predecrement(it)
            count -= 1
        ret.reverse()
        return ret

    def get_week_ends_between(self, start: int, end: int):
        ret = []
        if (start >= end):
            return ret
        cdef pair[week_end_iterator, week_end_iterator] p = self.c_cal.WeekEndBetween(start, end)
        if (p.first.is_end() or p.second.is_end()):
            return ret
        while (p.first <= p.second):
            ret.append(deref(p.first).first)
            preincrement(p.first)
        return ret

    def get_week_end_next(self, dt: int):
        cdef week_end_iterator it = self.c_cal.WeekEndUpper(dt)
        return None if it.is_end() else deref(it).first

    def get_week_end_last(self, dt: int):
        cdef week_end_iterator it = self.c_cal.WeekEndLower(dt)
        return None if it.is_end() else deref(it).first

    def get_week_begins_gte(self, dt: int, count: int = _maxsize):
        ret = []
        cdef week_begin_iterator it = self.c_cal.WeekBeginUpper(dt)
        while not it.is_end() and count > 0:
            ret.append(deref(it).first)
            preincrement(it)
            count -= 1
        return ret

    def get_week_begins_lte(self, dt: int, count: int = _maxsize):
        ret = []
        cdef week_begin_iterator it = self.c_cal.WeekBeginLower(dt)
        while not it.is_end() and count > 0:
            ret.append(deref(it).first)
            predecrement(it)
            count -= 1
        ret.reverse()
        return ret

    def get_week_begins_between(self, start: int, end: int):
        ret = []
        if (start >= end):
            return ret
        cdef pair[week_begin_iterator, week_begin_iterator] p = self.c_cal.WeekBeginBetween(start, end)
        if (p.first.is_end() or p.second.is_end()):
            return ret
        while (p.first <= p.second):
            ret.append(deref(p.first).first)
            preincrement(p.first)
        return ret

    def get_week_begin_next(self, dt: int):
        cdef week_begin_iterator it = self.c_cal.WeekBeginUpper(dt)
        return None if it.is_end() else deref(it).first

    def get_week_begin_last(self, dt: int):
        cdef week_begin_iterator it = self.c_cal.WeekBeginLower(dt)
        return None if it.is_end() else deref(it).first

    def get_week_days_gte(self, weekday: int, dt: int, count: int = _maxsize):
        ret = []
        cdef weekday_iterator it = self.c_cal.WeekDayUpper(dt, weekday)
        while not it.is_end() and count > 0:
            ret.append(deref(it).first)
            preincrement(it)
            count -= 1
        return ret

    def get_week_days_lte(self, weekday: int, dt: int, count: int = _maxsize):
        ret = []
        cdef weekday_iterator it = self.c_cal.WeekDayLower(dt, weekday)
        while not it.is_end() and count > 0:
            ret.append(deref(it).first)
            predecrement(it)
            count -= 1
        ret.reverse()
        return ret

    def get_week_days_between(self, weekday: int, start: int, end: int):
        ret = []
        if (start >= end):
            return ret
        cdef pair[weekday_iterator, weekday_iterator] p = self.c_cal.WeekDayBetween(start, end, weekday)
        if (p.first.is_end() or p.second.is_end()):
            return ret
        while (p.first <= p.second):
            ret.append(deref(p.first).first)
            preincrement(p.first)
        return ret

    def get_week_day_next(self, weekday: int, dt: int):
        cdef weekday_iterator it = self.c_cal.WeekDayUpper(dt, weekday)
        return None if it.is_end() else deref(it).first

    def get_week_day_last(self, weekday: int, dt: int):
        cdef weekday_iterator it = self.c_cal.WeekDayLower(dt, weekday)
        return None if it.is_end() else deref(it).first

    def get_bartime_next(self, dt: float, interval: int):
        return self.c_cal.GetCurrentBartime(to_time_point(dt), seconds(interval))

    def get_bartimes_gte(self, interval: int, start: float, count: int=_maxsize):
        # count must cast to <size_t>, otherwise it is PyObject* type and no suitable overloading method found.
        return self.c_cal.GetBartimes(seconds(interval), to_time_point(start), <size_t>count)

    def get_bartimes_between(self, interval: int, start: float, end: float):
        return self.c_cal.GetBartimes(seconds(interval), to_time_point(start), to_time_point(end))

    def get_next_open_close(self, dt: float):
        return self.c_cal.GetNextOpenClose(to_time_point(dt))

    def get_next_session(self, dt: float):
        return self.c_cal.GetNextSession(to_time_point(dt))

    def get_sessions(self):
        return self.c_cal.GetSessions()

    def get_ordered_sessions(self):
        return self.c_cal.GetOrderedSessions()

    def get_open_close_time(self):
        return self.c_cal.GetOpenCloseTime()

    def is_trading(self, dt: float):
        return self.c_cal.IsTrading(to_time_point(dt))

    def is_trading_day(self, dt: float):
        return self.c_cal.IsTradingDay(to_time_point(dt))

    def is_trading_time(self, dt: int):
        return self.c_cal.IsTradingTime(to_time_point(dt))

# cdef class CalendarCTP(PyCalendar):
#     def __cinit__(self):
#         self.c_cal = new c_CalendarCTP()

#     def __init__(self, int x0, int y0, int x1, int y1):
#         self.c_rect.x0 = x0
#         self.c_rect.y0 = y0
#         self.c_rect.x1 = x1
#         self.c_rect.y1 = y1

#     def __dealloc__(self):
#         del self.c_cal

cdef class PyCalendarAstock(PyCalendar):
    def __cinit__(self):
        self.c_cal = &CalendarAstock.GetInstance(b"")

    @staticmethod
    def InitData(data):
        CalendarAstock.InitData(data)

# cdef class Time7x24Calendar(Calendar):
#     def __cinit__(self):
#         self.c_cal = new c_Time7x24Calendar()

#     def __init__(self, int x0, int y0, int x1, int y1):
#         self.c_rect.x0 = x0
#         self.c_rect.y0 = y0
#         self.c_rect.x1 = x1
#         self.c_rect.y1 = y1

#     def __dealloc__(self):
#         del self.c_cal