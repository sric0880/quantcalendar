#pragma once
#include <stdexcept>
#include "fmt/format.h"
#include "quantcalendar/qmc_globals.h"

NS_QMC_BEGIN

class OutOfCalendar : public std::out_of_range
{
public:
  OutOfCalendar() : std::out_of_range("日历越界，请更新日历") {}
};

class InvalidInterval : public std::invalid_argument
{
public:
  InvalidInterval(int interval) : std::invalid_argument(fmt::format("argument interval {} is invalid", interval)) {}
};

class CalendarNotFound : public std::out_of_range
{
public:
  CalendarNotFound(std::string_view calendar_name, std::string_view symbol) : std::out_of_range(fmt::format("calendar {} of {} is not found", calendar_name, symbol)) {}
};

NS_QMC_END
