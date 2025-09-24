# cython: language_level=3
# distutils: language = c++
# distutils: sources = src/calendar.cpp src/dates.cpp
from libcpp.utility cimport move
from ._quantcalendar cimport CalendarAstock, CalendarCTP, Time7x24Calendar, RangeClosed

cdef str time_fmt(time: sec_t):
    cdef int hour = time // 3600
    cdef int min = (time % 3600) // 60
    cdef int sec = time - 3600 * hour - 60 * min
    return "{:02d}:{:02d}:{:02d}".format(hour, min, sec)

include "_np_datetime.pxi"
include "_quantcalendar_DatesArray.pxi"
include "_quantcalendar_Date7x24Array.pxi"

cdef class PyCalendar:
    """only used for annotation"""
    pass

cdef class PyCalendarAstock(PyCalendar_DatesArray):
    def __cinit__(self):
        self.c_cal = &CalendarAstock.GetInstance(b"")

    def __reduce__(self):
        # for support pickling
        return (PyCalendarAstock,())

    @staticmethod
    def Init(dates_arr):
        CalendarAstock.Init(dates_arr)

    def __str__(self) -> str:
        return super().__str__()

    def is_cancel_order_allowed(self, dt):
        return (<CalendarAstock*>self.c_cal).IsCancelOrderAllowed(to_timepoint(dt))

cdef class PyCalendarCTP(PyCalendar_DatesArray):
    cdef public str symbol

    def __cinit__(self, symbol):
        self.symbol = symbol
        self.c_cal = &CalendarCTP.GetInstance(symbol.encode("ascii"))

    def __reduce__(self):
        # for support pickling
        return (PyCalendarCTP,(self.symbol,))

    def has_night(self) -> bool:
        return (<const CalendarCTP*>self.c_cal).HasNight()

    @staticmethod
    def Init(dates_arr, sessions, bartime_right: bool = True):
        CalendarCTP.Init(dates_arr, move(sessions), bartime_right)

    def __str__(self) -> str:
        return super().__str__()

    def is_cancel_order_allowed(self, dt):
        return (<CalendarCTP*>self.c_cal).IsCancelOrderAllowed(to_timepoint(dt))

cdef class PyTime7x24Calendar(PyCalendar_Date7x24Array):
    def __cinit__(self):
        self.c_cal = &Time7x24Calendar.GetInstance(b"")

    def __reduce__(self):
        # for support pickling
        return (PyTime7x24Calendar,())

    def is_trading(self, dt):
        return True

    def is_trading_day(self, dt):
        return True

    def is_trading_time(self, tm, rangeclosed: RangeClosed = RangeClosed.BOTH):
        return True

    def is_cancel_order_allowed(self, dt):
        return (<Time7x24Calendar*>self.c_cal).IsCancelOrderAllowed(to_timepoint(dt))

    def __str__(self) -> str:
        return super().__str__()
