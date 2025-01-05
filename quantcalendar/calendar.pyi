from datetime import datetime

class PyCalendar:
    def get(self, symbol: str = None):
        """"""

    def get_tradedays_gte(self, dt: datetime):
        """get trade days >= dt"""

    def get_tradedays_lte(self, dt: datetime):
        """get trade days <= dt"""

    def get_tradedays_next(self, dt: datetime):
        """equal to get_tradedays_gte(dt)[0]"""

    def get_tradedays_last(self, dt: datetime):
        """equal to get_tradedays_lte(dt)[-1]"""

    def get_tradedays_between(self, start_dt: datetime, end_dt: datetime):
        """get start_dt <= trade days <= end_dt"""

    def get_current_bartime(self, dt: datetime, interval: int):
        """"""

    def get_bartimes(
        self, interval: int, start: datetime, end: datetime = None, count=0
    ):
        """"""

    def get_tradedays_month_end(
        self, start: datetime, end: datetime = None, count: int = 0
    ):
        """"""

    def get_tradedays_month_begin(
        self, start: datetime, end: datetime = None, count: int = 0
    ):
        """"""

    def get_tradedays_week_end(
        self, start: datetime, end: datetime = None, count: int = 0
    ):
        """"""

    def get_tradedays_week_begin(
        self, start: datetime, end: datetime = None, count: int = 0
    ):
        """"""

    def get_tradedays_week_day(
        self, weekday: int, start: datetime, end: datetime = None, count: int = 0
    ):
        """"""

    def get_special_sessions(self, dt: datetime):
        """"""

    def get_open_close_dt(self, dt: datetime):
        """"""

    def get_session_dt(self, dt: datetime):
        """"""

    def get_sessions(self):
        """"""

    def get_ordered_sessions(self):
        """"""

    def get_open_close_time(self):
        """"""

    def is_trading(self, dt: datetime):
        """"""

    def is_trading_day(self, dt: datetime):
        """"""

    def is_trading_time(self, dt: datetime):
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
