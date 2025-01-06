# cython: language_level=3
# distutils: language = c++
# distutils: sources = src/calendar.cpp src/calendar_data.cpp
from datetime import datetime

from ._quantcalendar cimport Calendar
# from ._quantcalendar cimport CalendarCTP
from ._quantcalendar cimport CalendarAstock
# from ._quantcalendar cimport Time7x24Calendar

cdef class PyCalendar:
    cdef const Calendar* c_cal

    def get(self, symbol: str = None):
        pass

    def get_tradedays_gte(self, dt: datetime):
        pass

    def get_tradedays_lte(self, dt: datetime):
        pass

    def get_tradedays_next(self, dt: datetime):
        pass

    def get_tradedays_last(self, dt: datetime):
        pass

    def get_tradedays_between(self, start_dt: datetime, end_dt: datetime):
        pass

    def get_current_bartime(self, dt: datetime, interval: int):
        pass

    def get_bartimes(self, interval: int, start: datetime, end: datetime = None, count=0):
        pass

    def get_tradedays_month_end(self, start: datetime, end: datetime = None, count: int = 0):
        pass

    def get_tradedays_month_begin(self, start: datetime, end: datetime = None, count: int = 0):
        pass

    def get_tradedays_week_end(self, start: datetime, end: datetime = None, count: int = 0):
        pass

    def get_tradedays_week_begin(self, start: datetime, end: datetime = None, count: int = 0):
        pass

    def get_tradedays_week_day(self, weekday: int, start: datetime, end: datetime = None, count: int = 0):
        pass

    def get_special_sessions(self, dt: datetime):
        pass

    def get_open_close_dt(self, dt: datetime):
        pass

    def get_session_dt(self, dt: datetime):
        pass

    def get_sessions(self):
        pass

    def get_ordered_sessions(self):
        pass

    def get_open_close_time(self):
        pass

    def is_trading(self, dt: datetime):
        pass

    def is_trading_day(self, dt: datetime):
        pass

    def is_trading_time(self, dt: datetime):
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