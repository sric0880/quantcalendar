# cython: language_level=3
from libcpp.vector cimport vector
from libcpp.pair cimport pair
from libcpp.string cimport string

cdef extern from "<chrono>" namespace "std::chrono" nogil:
	cdef cppclass seconds:
		seconds(long long)
	cdef cppclass system_clock:
		pass
	cdef cppclass time_point[_Clock, _Duration=*]:
		pass

cdef extern from "quantdata/datetime.h" nogil:
	cdef cppclass Date:
		int year
		int mon
		int day
	cdef cppclass Time:
		int hour
		int min
		int sec
		int subseconds
	cdef cppclass Datetime:
		Date date
		Time time

cdef extern from "quantcalendar/calendar_data.h" namespace "qmc" nogil:
	ctypedef long long sec_t
	# cdef (sec_t, char) calendar_item
	ctypedef pair[sec_t, char] calendar_item

	cdef cppclass CalendarDataNode:
		Datetime dt_
		char status_
		bint IsTrading()

	cdef cppclass CalendarData:
		cppclass iterator:
			ctypedef pair[sec_t, CalendarDataNode] node
			node operator*()
			bint operator==(iterator)
			bint operator!=(iterator)
			bint operator>(iterator)
			bint operator>=(iterator)
			bint operator<(iterator)
			bint operator<=(iterator)
			bint is_end() const
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

		tradedays_iterator TradedaysUpper(sec_t dt)
		tradedays_iterator TradedaysLower(sec_t dt)
		pair[tradedays_iterator, tradedays_iterator] TradedaysBetween(sec_t start, sec_t end)
		month_end_iterator MonthEndUpper(sec_t dt)
		month_end_iterator MonthEndLower(sec_t dt)
		pair[month_end_iterator, month_end_iterator] MonthEndBetween(sec_t start, sec_t end)
		month_begin_iterator MonthBeginUpper(sec_t dt)
		month_begin_iterator MonthBeginLower(sec_t dt)
		pair[month_begin_iterator, month_begin_iterator] MonthBeginBetween(sec_t start, sec_t end)
		week_end_iterator WeekEndUpper(sec_t dt)
		week_end_iterator WeekEndLower(sec_t dt)
		pair[week_end_iterator, week_end_iterator] WeekEndBetween(sec_t start, sec_t end)
		week_begin_iterator WeekBeginUpper(sec_t dt)
		week_begin_iterator WeekBeginLower(sec_t dt)
		pair[week_begin_iterator, week_begin_iterator] WeekBeginBetween(sec_t start, sec_t end)
		weekday_iterator WeekDayUpper(sec_t dt, int weekday)
		weekday_iterator WeekDayLower(sec_t dt, int weekday)
		pair[weekday_iterator, weekday_iterator] WeekDayBetween(sec_t start, sec_t end, int weekday)

	cdef cppclass Calendar7x24DataNode:
		Datetime dt_

	cdef cppclass Calendar7x24Data:
		cppclass iterator:
			ctypedef pair[sec_t, Calendar7x24DataNode] node
			node operator*()
			bint operator==(iterator)
			bint operator!=(iterator)
			bint operator>(iterator)
			bint operator>=(iterator)
			bint operator<(iterator)
			bint operator<=(iterator)
			bint is_end() const
		cppclass tradedays_iterator(iterator):
			tradedays_iterator operator++()
			tradedays_iterator operator--()
		cppclass month_end_iterator(iterator):
			month_end_iterator operator++()
			month_end_iterator operator--()
		cppclass month_begin_iterator(iterator):
			month_begin_iterator operator++()
			month_begin_iterator operator--()
		cppclass weekday_iterator(iterator):
			weekday_iterator operator++()
			weekday_iterator operator--()
		ctypedef weekday_iterator week_end_iterator
		ctypedef weekday_iterator week_begin_iterator

		tradedays_iterator TradedaysUpper(sec_t dt)
		tradedays_iterator TradedaysLower(sec_t dt)
		pair[tradedays_iterator, tradedays_iterator] TradedaysBetween(sec_t start, sec_t end)
		month_end_iterator MonthEndUpper(sec_t dt)
		month_end_iterator MonthEndLower(sec_t dt)
		pair[month_end_iterator, month_end_iterator] MonthEndBetween(sec_t start, sec_t end)
		month_begin_iterator MonthBeginUpper(sec_t dt)
		month_begin_iterator MonthBeginLower(sec_t dt)
		pair[month_begin_iterator, month_begin_iterator] MonthBeginBetween(sec_t start, sec_t end)
		week_end_iterator WeekEndUpper(sec_t dt)
		week_end_iterator WeekEndLower(sec_t dt)
		pair[week_end_iterator, week_end_iterator] WeekEndBetween(sec_t start, sec_t end)
		week_begin_iterator WeekBeginUpper(sec_t dt)
		week_begin_iterator WeekBeginLower(sec_t dt)
		pair[week_begin_iterator, week_begin_iterator] WeekBeginBetween(sec_t start, sec_t end)
		weekday_iterator WeekDayUpper(sec_t dt, int weekday)
		weekday_iterator WeekDayLower(sec_t dt, int weekday)
		pair[weekday_iterator, weekday_iterator] WeekDayBetween(sec_t start, sec_t end, int weekday)


cdef extern from "quantcalendar/calendar.h" namespace "qmc" nogil:
	ctypedef pair[sec_t, sec_t] session_t
	ctypedef time_point[system_clock] tp
	cdef tp to_time_point(double ts)
	cdef cppclass Calendar[T]:
		const T * data
		vector[sec_t] GetBartimes(seconds interval, tp start, tp end) except +
		vector[sec_t] GetBartimes(seconds interval, tp start, size_t count) except +
		sec_t GetCurrentBartime(tp dt, seconds interval) except +
		session_t GetNextOpenClose(tp dt)
		session_t GetNextSession(tp dt)
		const vector[session_t] &GetSessions()
		const vector[session_t] &GetOrderedSessions()
		const session_t &GetOpenCloseTime()
		bint IsTrading(tp dt)
		bint IsTradingDay(tp dt)
		bint IsTradingTime(tp dt)
		string ToString()

	cdef cppclass CalendarAstock(Calendar[CalendarData]):
		@staticmethod
		void InitData(const vector[calendar_item] &data) except +
		@staticmethod
		CalendarAstock &GetInstance(const string &symbol) except +

	cdef cppclass CalendarCTP(Calendar[CalendarData]):
		ctypedef pair[string, vector[session_t]] session_item
		bint HasNight()
		@staticmethod
		void InitData(const vector[calendar_item] &data, vector[session_item] sessions) except + # sessions is rvalue
		@staticmethod
		CalendarCTP &GetInstance(const string &symbol) except +

	cdef cppclass Time7x24Calendar(Calendar[Calendar7x24Data]):
		@staticmethod
		Time7x24Calendar &GetInstance(const string &symbol) except +