# distutils: language = c++

from calendar cimport Calendar as c_Calendar
from calendar cimport CalendarCTP as c_CalendarCTP
from calendar cimport CalendarAstock as c_CalendarAstock
from calendar cimport Time7x24Calendar as c_Time7x24Calendar

cdef class Calendar:
    cdef c_Calendar * c_cal

    def get(self, symbol: str = None):
        pass

    def get_tradedays_gte(self, dt: datetime) -> List[datetime]:
        """get trade days >= dt"""
        pass

    @abstractmethod
    def get_tradedays_lte(self, dt: datetime) -> List[datetime]:
        """get trade days <= dt"""
        pass

    @abstractmethod
    def get_tradedays_next(self, dt: datetime) -> datetime:
        """equal to get_tradedays_gte(dt)[0]"""
        pass

    @abstractmethod
    def get_tradedays_last(self, dt: datetime) -> datetime:
        """equal to get_tradedays_lte(dt)[-1]"""
        pass

    @abstractmethod
    def get_tradedays_between(
        self, start_dt: datetime, end_dt: datetime
    ) -> List[datetime]:
        """get start_dt <= trade days <= end_dt"""
        pass

    def get_current_bartime(self, dt: datetime, interval: int):
        pass

    def get_bartimes(
        self, interval: int, start: datetime, end: datetime = None, count=0
    ) -> List[datetime]:
        pass

    def get_tradedays_month_end(
        self, start: datetime, end: datetime = None, count: int = 0
    ) -> List[datetime]:
        pass
    def get_tradedays_month_begin(
            self, start: datetime, end: datetime = None, count: int = 0
        ) -> List[datetime]:
    def get_tradedays_week_end(
            self, start: datetime, end: datetime = None, count: int = 0
        ) -> List[datetime]:
    def get_tradedays_week_begin(
            self, start: datetime, end: datetime = None, count: int = 0
        ) -> List[datetime]:
    def get_tradedays_week_day(
        self, weekday: int, start: datetime, end: datetime = None, count: int = 0
    ) -> List[datetime]:
    def get_special_sessions(self, dt: datetime):
        pass
    def get_open_close_dt(self, dt: datetime) -> Tuple[datetime, datetime]:
        pass

    def get_session_dt(self, dt: datetime) -> Tuple[datetime, datetime]:
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

cdef class CalendarCTP(PyCalendar):
    def __cinit__(self):
        self.c_cal = new c_CalendarCTP()

    def __init__(self, int x0, int y0, int x1, int y1):
        self.c_rect.x0 = x0
        self.c_rect.y0 = y0
        self.c_rect.x1 = x1
        self.c_rect.y1 = y1

    def __dealloc__(self):
        del self.c_cal

cdef class CalendarCTP(Calendar):
    def __cinit__(self):
        self.c_cal = new c_CalendarCTP()

    def __init__(self, int x0, int y0, int x1, int y1):
        self.c_rect.x0 = x0
        self.c_rect.y0 = y0
        self.c_rect.x1 = x1
        self.c_rect.y1 = y1

    def __dealloc__(self):
        del self.c_cal

cdef class CalendarAstock(Calendar):
    def __cinit__(self):
        self.c_cal = new c_CalendarAstock()

    def __init__(self, int x0, int y0, int x1, int y1):
        self.c_rect.x0 = x0
        self.c_rect.y0 = y0
        self.c_rect.x1 = x1
        self.c_rect.y1 = y1

    def __dealloc__(self):
        del self.c_cal

cdef class Time7x24Calendar(Calendar):
    def __cinit__(self):
        self.c_cal = new c_Time7x24Calendar()

    def __init__(self, int x0, int y0, int x1, int y1):
        self.c_rect.x0 = x0
        self.c_rect.y0 = y0
        self.c_rect.x1 = x1
        self.c_rect.y1 = y1

    def __dealloc__(self):
        del self.c_cal