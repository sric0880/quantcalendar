#include "quantcalendar/exceptions.h"
#include "fmt/format.h"

NS_QMC_BEGIN

OutOfCalendar::OutOfCalendar() : std::out_of_range("request out of calendar, update it please.") {}

CalendarNotInit::CalendarNotInit() : std::runtime_error("calendar should initialize before using.") {}

InvalidInterval::InvalidInterval(int interval) : std::invalid_argument(fmt::format("argument interval {} is invalid", interval)) {}

InvalidTimeOrder::InvalidTimeOrder(long long start, long long end) : std::invalid_argument(fmt::format("argument start timestamp {} must be less equal(<=) than end {}", start, end)) {}

CalendarNotFound::CalendarNotFound(std::string_view calendar_name, std::string_view symbol) : std::out_of_range(fmt::format("calendar {} of {} is not found", calendar_name, symbol)) {}

NS_QMC_END
