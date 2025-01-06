# cython: language_level=3
from libcpp.vector cimport vector
from libcpp.pair cimport pair
from libcpp.string cimport string

cdef extern from "<chrono>" namespace "std" nogil:
	cdef cppclass seconds:
		pass
	cdef cppclass system_clock:
		pass
	cdef cppclass time_point[_Clock, _Duration=*]:
		pass

cdef extern from "quantcalendar/calendar_data.h" namespace "qmc" nogil:
	ctypedef long long sec_t
	# cdef (sec_t, char) calendar_item
	ctypedef pair[sec_t, char] calendar_item
	cdef cppclass CalendarDataNode:
		char status_
		bint IsTrading()
	cdef cppclass CalendarData:
		cppclass iterator:
			ctypedef pair[sec_t, CalendarDataNode] node
			const node operator*()
			bint operator==(iterator)
			bint operator!=(iterator)
		cppclass tradedays_iterator(iterator):
			tradedays_iterator operator++()
			tradedays_iterator operator--()
		cppclass month_end_iterator(iterator):
			month_end_iterator operator++()
			month_end_iterator operator--()
		cppclass month_begin_iterator(iterator):
			month_begin_iterator operator++()
			month_begin_iterator operator--()
		cppclass week_end_iterator(iterator):
			week_end_iterator operator++()
			week_end_iterator operator--()
		cppclass week_begin_iterator(iterator):
			week_begin_iterator operator++()
			week_begin_iterator operator--()
		cppclass weekday_iterator(iterator):
			weekday_iterator operator++()
			weekday_iterator operator--()


cdef extern from "quantcalendar/calendar.h" namespace "qmc" nogil:
	ctypedef pair[sec_t, sec_t] session_t
	ctypedef time_point[system_clock] tp
	cdef cppclass Calendar:
		CalendarData.tradedays_iterator TradedaysUpper(sec_t dt) const
		CalendarData.tradedays_iterator TradedaysLower(sec_t dt) const
		pair[CalendarData.tradedays_iterator, CalendarData.tradedays_iterator] TradedaysBetween(sec_t start, sec_t end) const
		CalendarData.month_end_iterator MonthEndUpper(sec_t dt) const
		CalendarData.month_end_iterator MonthEndLower(sec_t dt) const
		pair[CalendarData.month_end_iterator, CalendarData.month_end_iterator] MonthEndBetween(sec_t start, sec_t end) const
		CalendarData.month_begin_iterator MonthBeginUpper(sec_t dt) const
		CalendarData.month_begin_iterator MonthBeginLower(sec_t dt) const
		pair[CalendarData.month_begin_iterator, CalendarData.month_begin_iterator] MonthBeginBetween(sec_t start, sec_t end) const
		CalendarData.week_end_iterator WeekEndUpper(sec_t dt) const
		CalendarData.week_end_iterator WeekEndLower(sec_t dt) const
		pair[CalendarData.week_end_iterator, CalendarData.week_end_iterator] WeekEndBetween(sec_t start, sec_t end) const
		CalendarData.week_begin_iterator WeekBeginUpper(sec_t dt) const
		CalendarData.week_begin_iterator WeekBeginLower(sec_t dt) const
		pair[CalendarData.week_begin_iterator, CalendarData.week_begin_iterator] WeekBeginBetween(sec_t start, sec_t end) const
		CalendarData.weekday_iterator WeekDayUpper(sec_t dt, int weekday) const
		CalendarData.weekday_iterator WeekDayLower(sec_t dt, int weekday) const
		pair[CalendarData.weekday_iterator, CalendarData.weekday_iterator] WeekDayBetween(sec_t start, sec_t end, int weekday)
		vector[sec_t] GetBartimes(seconds interval, tp start, tp end) const
		vector[sec_t] GetBartimes(seconds interval, tp start, int count) const
		sec_t GetCurrentBartime(tp dt, seconds interval) const
		session_t GetNextOpenClose(tp dt) const
		session_t GetNextSession(tp dt) const
		const vector[session_t] &GetSessions()
		const vector[session_t] &GetOrderedSessions()
		const session_t &GetOpenCloseTime()
		bint IsTrading(tp dt) const
		bint IsTradingDay(tp dt) const
		bint IsTradingTime(tp dt) const


	cdef cppclass CalendarAstock(Calendar):
		@staticmethod
		void InitData(const vector[calendar_item] &data)
		@staticmethod
		CalendarAstock &GetInstance(string symbol)
