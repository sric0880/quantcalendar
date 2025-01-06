# fmt: off
class PyCalendar:
    def get(self, symbol: str = None):
        """"""

    def get_tradedays_gte(self, dt: int, count: int = 2**32-1) -> list[int]:
        """
        get `count` trade days which >= `dt`
        """

    def get_tradedays_lte(self, dt: int, count: int = 2**32-1) -> list[int]:
        """
        get `count` trade days which <= `dt`
        """

    def get_tradedays_between(self, start: int, end: int) -> list[int]:
        """
        get trade days which is between [`start`, `end`]
        """

    def get_tradeday_next(self, dt: int):
        """equal to get_tradedays_gte(dt)[0]"""

    def get_tradeday_last(self, dt: int):
        """equal to get_tradedays_lte(dt)[-1]"""

    def get_month_ends_gte(self, dt: int, count: int = 2**32-1):
        pass

    def get_month_ends_lte(self, dt: int, count: int = 2**32-1):
        pass

    def get_month_ends_between(self, start: int, end: int):
        pass

    def get_month_end_next(self, dt: int):
        return self.get_month_ends_gte(dt, 1)[0]

    def get_month_end_last(self, dt: int):
        return self.get_month_ends_lte(dt, 1)[0]

    def get_month_begins_gte(self, dt: int, count: int = 2**32-1):
        """"""
    def get_month_begins_lte(self, dt: int, count: int = 2**32-1):
        """"""
    def get_month_begins_between(self, start: int, end: int = None):
        """"""
    def get_month_begin_next(self, dt: int):
        """"""
    def get_month_begin_last(self, dt: int):
        """"""

    def get_week_ends_gte(self, dt: int, count: int = 2**32-1):
        """"""
    def get_week_ends_lte(self, dt: int, count: int = 2**32-1):
        """"""
    def get_week_ends_between(self, start: int, end: int = None):
        """"""
    def get_week_end_next(self, dt: int):
        """"""
    def get_week_end_last(self, dt: int):
        """"""

    def get_week_begins_gte(self, dt: int, count: int = 2**32-1):
        """"""
    def get_week_begins_lte(self, dt: int, count: int = 2**32-1):
        """"""
    def get_week_begins_between(self, start: int, end: int = None):
        """"""
    def get_week_begin_next(self, dt: int):
        """"""
    def get_week_begin_last(self, dt: int):
        """"""

    def get_week_days_gte(self, weekday: int, dt: int, count: int = 2**32-1):
        """"""
    def get_week_days_lte(self, weekday: int, dt: int, count: int = 2**32-1):
        """"""
    def get_week_days_between(self, weekday: int, start: int, end: int = None):
        """"""
    def get_week_day_next(self, weekday: int, dt: int):
        """"""
    def get_week_day_last(self, weekday: int, dt: int):
        """"""

    def get_current_bartime(self, dt: int, interval: int):
        """"""

    def get_bartimes(
        self, interval: int, start: int, end: int = None, count=2**32-1
    ):
        """"""

    def get_special_sessions(self, dt: int):
        """"""

    def get_open_close_dt(self, dt: int):
        """"""

    def get_session_dt(self, dt: int):
        """"""

    def get_sessions(self):
        """"""

    def get_ordered_sessions(self):
        """"""

    def get_open_close_time(self):
        """"""

    def is_trading(self, dt: int):
        """"""

    def is_trading_day(self, dt: int):
        """"""

    def is_trading_time(self, dt: int):
        """"""

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

class PyCalendarAstock(PyCalendar):
    @staticmethod
    def InitData(data):
        """"""

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
