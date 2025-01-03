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

class InvalidArgumentInterval : public std::invalid_argument
{
public:
  InvalidArgumentInterval(int interval) : std::invalid_argument(fmt::format("argument interval {} is invalid", interval)) {}
};

NS_QMC_END
