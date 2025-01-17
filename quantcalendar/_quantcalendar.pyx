# cython: language_level=3
# distutils: language = c++
# distutils: sources = src/calendar.cpp src/dates.cpp
from libcpp.utility cimport move
from ._quantcalendar cimport CalendarAstock, CalendarCTP, Time7x24Calendar

include "_np_datetime.pxi"
include "_quantcalendar_DatesArray.pxi"
include "_quantcalendar_Date7x24Array.pxi"

cdef class PyCalendar:
    """only used for annotation"""
    pass

cdef class PyCalendarAstock(PyCalendar_DatesArray):
    def __cinit__(self):
        self.c_cal = &CalendarAstock.GetInstance(b"")

    @staticmethod
    def Init(dates_arr):
        CalendarAstock.Init(dates_arr)

    def __str__(self) -> str:
        return super().__str__()

cdef class PyCalendarCTP(PyCalendar_DatesArray):
    def __cinit__(self, symbol):
        self.c_cal = &CalendarCTP.GetInstance(symbol.encode("ascii"))

    def has_night(self) -> bool:
        return (<CalendarCTP*>self.c_cal).HasNight()

    @staticmethod
    def Init(dates_arr, sessions):
        CalendarCTP.Init(dates_arr, move(sessions))

    def __str__(self) -> str:
        return super().__str__()

cdef class PyTime7x24Calendar(PyCalendar_Date7x24Array):
    def __cinit__(self):
        self.c_cal = &Time7x24Calendar.GetInstance(b"")

    def is_trading(self, dt: float):
        return True

    def is_trading_day(self, dt: float):
        return True

    def is_trading_time(self, dt: float):
        return True

    def __str__(self) -> str:
        return super().__str__()
