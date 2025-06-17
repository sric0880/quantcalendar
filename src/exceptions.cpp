#include "quantcalendar/exceptions.h"

NS_QMC_BEGIN

OutOfCalendar::OutOfCalendar() : std::out_of_range("request out of calendar, update it please.") {}

CalendarNotInit::CalendarNotInit() : std::runtime_error("calendar should initialize before using.") {}

InvalidInterval::InvalidInterval(int interval) : std::invalid_argument("argument interval" + std::to_string(interval) + " is invalid") {}

InvalidTimeOrder::InvalidTimeOrder(long long start, long long end) : std::invalid_argument("argument start timestamp " + std::to_string(start) + " must be less equal(<=) than end " + std::to_string(end)) {}

CalendarNotFound::CalendarNotFound(std::string calendar_name, std::string symbol) : std::out_of_range("calendar " + calendar_name + " of " + symbol + " is not found") {}

NS_QMC_END
