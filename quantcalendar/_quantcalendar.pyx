# cython: language_level=3
# distutils: language = c++
# distutils: sources = src/calendar.cpp src/calendar_data.cpp
from libcpp.utility cimport move
from ._quantcalendar cimport CalendarAstock, CalendarCTP, Time7x24Calendar

include "_quantcalendar_CalendarData.pxi"
include "_quantcalendar_Calendar7x24Data.pxi"

cdef class PyCalendarAstock(PyCalendar_CalendarData):
    def __cinit__(self):
        self.c_cal = &CalendarAstock.GetInstance(b"")

    @staticmethod
    def InitData(data):
        CalendarAstock.InitData(data)

    def __str__(self) -> str:
        return super().__str__()

cdef class PyCalendarCTP(PyCalendar_CalendarData):
    def __cinit__(self, symbol):
        self.c_cal = &CalendarCTP.GetInstance(symbol.encode("ascii"))

    def has_night(self) -> bool:
        return (<CalendarCTP*>self.c_cal).HasNight()

    @staticmethod
    def InitData(data, sessions):
        CalendarCTP.InitData(data, move(sessions))

    def __str__(self) -> str:
        return super().__str__()

cdef class PyTime7x24Calendar(PyCalendar_Calendar7x24Data):
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
