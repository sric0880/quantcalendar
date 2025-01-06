# cython: language_level=3
# distutils: language = c++
# distutils: sources = src/calendar.cpp src/calendar_data.cpp
from cython.operator cimport preincrement, predecrement
from cython.operator cimport dereference as deref

from ._quantcalendar cimport Calendar, CalendarData
# from ._quantcalendar cimport CalendarCTP
from ._quantcalendar cimport CalendarAstock
# from ._quantcalendar cimport Time7x24Calendar

maxsize = 2**32-1
cdef class PyCalendar:
    cdef const Calendar* c_cal

    def get_tradedays_gte(self, dt: int, count: int=maxsize):
        ret = []
        it = self.c_cal.TradedaysUpper(dt)
        while not it.is_end() and count > 0:
            ret.append(deref(it).first)
            preincrement(it)
            count -= 1
        return ret

    def get_tradedays_lte(self, dt: int, count: int=maxsize):
        ret = []
        it = self.c_cal.TradedaysLower(dt)
        while not it.is_end() and count > 0:
            ret.append(deref(it).first)
            predecrement(it)
            count -= 1
        ret.reverse()
        return ret

    def get_tradedays_between(self, start: int, end: int):
        pass

    def get_tradeday_next(self, dt: int):
        return self.get_tradedays_gte(dt, 1)

    def get_tradeday_last(self, dt: int):
        return self.get_tradedays_lte(dt, 1)

    def get_month_ends_gte(self, dt: int, count: int = 0):
        pass

    def get_month_ends_lte(self, dt: int, count: int = 0):
        pass

    def get_month_ends_between(self, start: int, end: int):
        pass

    def get_month_end_next(self, dt: int):
        return self.get_month_ends_gte(dt, 1)[0]

    def get_month_end_last(self, dt: int):
        return self.get_month_ends_lte(dt, 1)[0]

    def get_month_begins_gte(self, dt: int, count: int = 0):
        pass
    def get_month_begins_lte(self, dt: int, count: int = 0):
        pass
    def get_month_begins_between(self, start: int, end: int = None):
        pass
    def get_month_begin_next(self, dt: int):
        pass
    def get_month_begin_last(self, dt: int):
        pass

    def get_week_ends_gte(self, dt: int, count: int = 0):
        pass
    def get_week_ends_lte(self, dt: int, count: int = 0):
        pass
    def get_week_ends_between(self, start: int, end: int = None):
        pass
    def get_week_end_next(self, dt: int):
        pass
    def get_week_end_last(self, dt: int):
        pass

    def get_week_begins_gte(self, dt: int, count: int = 0):
        pass
    def get_week_begins_lte(self, dt: int, count: int = 0):
        pass
    def get_week_begins_between(self, start: int, end: int = None):
        pass
    def get_week_begin_next(self, dt: int):
        pass
    def get_week_begin_last(self, dt: int):
        pass

    def get_week_days_gte(self, weekday: int, dt: int, count: int = 0):
        pass
    def get_week_days_lte(self, weekday: int, dt: int, count: int = 0):
        pass
    def get_week_days_between(self, weekday: int, start: int, end: int = None):
        pass
    def get_week_day_next(self, weekday: int, dt: int):
        pass
    def get_week_day_last(self, weekday: int, dt: int):
        pass

    def get_current_bartime(self, dt: int, interval: int):
        pass

    def get_bartimes(self, interval: int, start: int, end: int = None, count=0):
        pass

    def get_special_sessions(self, dt: int):
        pass

    def get_open_close_dt(self, dt: int):
        pass

    def get_session_dt(self, dt: int):
        pass

    def get_sessions(self):
        pass

    def get_ordered_sessions(self):
        pass

    def get_open_close_time(self):
        pass

    def is_trading(self, dt: int):
        pass

    def is_trading_day(self, dt: int):
        pass

    def is_trading_time(self, dt: int):
        pass

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