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

ctypedef time_point[system_clock] tp

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
	cdef tp fromtimestamp(long long ts)
	cdef tp fromtimestamp(double ts)
	cdef tp fromtimestamp_milli(long long ts)
	cdef tp fromtimestamp_micro(long long ts)
	cdef tp fromtimestamp_nano(long long ts)
	cdef tp fromisoformat(string time_string)

cdef extern from "quantcalendar/dates.h" namespace "qmc" nogil:
	ctypedef long long sec_t
	ctypedef pair[sec_t, char] date_status_item

	cdef cppclass DateNode:
		Datetime dt_
		char status_
		bint IsTrading()

	cdef cppclass DatesArray:
		cppclass iterator:
			ctypedef pair[sec_t, DateNode] node
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

		tradedays_iterator Upper(sec_t dt)
		tradedays_iterator Lower(sec_t dt)
		pair[tradedays_iterator, tradedays_iterator] Between(sec_t start, sec_t end)
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

	cdef cppclass AllDayTradingNode:
		Datetime dt_

	cdef cppclass Date7x24Array:
		cppclass iterator:
			ctypedef pair[sec_t, AllDayTradingNode] node
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

		tradedays_iterator Upper(sec_t dt)
		tradedays_iterator Lower(sec_t dt)
		pair[tradedays_iterator, tradedays_iterator] Between(sec_t start, sec_t end)
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
	cdef cppclass Calendar[T]:
		const T * tradedays
		vector[sec_t] GetBartimes(seconds interval, tp start, tp end) except +
		vector[sec_t] GetBartimes(seconds interval, tp start, size_t count) except +
		sec_t GetCurrentBartime(seconds interval, tp dt) except +
		session_t GetNextOpenClose(tp dt) except +
		session_t GetNextSession(tp dt) except +
		const vector[session_t] &GetSessions()
		const vector[session_t] &GetOrderedSessions()
		const session_t &GetOpenCloseTime()
		const vector[int] &GetIntervals()
		const string &GetTimezone()
		bint IsTrading(tp dt) except +
		bint IsTradingDay(tp dt) except +
		bint IsTradingTime(tp dt)
		string ToString()

	cdef cppclass CalendarAstock(Calendar[DatesArray]):
		@staticmethod
		void Init(const vector[date_status_item] &dates_arr) except +
		@staticmethod
		CalendarAstock &GetInstance(const string &symbol) except +

	cdef cppclass CalendarCTP(Calendar[DatesArray]):
		ctypedef pair[string, vector[session_t]] session_item
		bint HasNight()
		@staticmethod
		void Init(const vector[date_status_item] &dates_arr, vector[session_item] sessions) except + # sessions is rvalue
		@staticmethod
		CalendarCTP &GetInstance(const string &symbol) except +

	cdef cppclass Time7x24Calendar(Calendar[Date7x24Array]):
		@staticmethod
		Time7x24Calendar &GetInstance(const string &symbol) except +