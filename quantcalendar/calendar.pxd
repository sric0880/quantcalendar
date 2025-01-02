# distutils: sources = Rectangle.cpp

cdef extern from "quantcalendar/calendar.h" namespace "qmc":
	cdef cppclass Rectangle:
		Rectangle() except +
        Rectangle(int, int, int, int) except +