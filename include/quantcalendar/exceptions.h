#pragma once
#include <stdexcept>
#include "fmt/format.h"
#include "quantcalendar/qmc_globals.h"

NS_QMC_BEGIN

class OutOfCalendar : public std::out_of_range
{
public:
  OutOfCalendar() : std::out_of_range("request out of calendar, update it please.") {}
};

class CalendarNotInit : public std::runtime_error
{
public:
  CalendarNotInit() : std::runtime_error("calendar should initialize before using.") {}
};

class InvalidInterval : public std::invalid_argument
{
public:
  InvalidInterval(int interval) : std::invalid_argument(fmt::format("argument interval {} is invalid", interval)) {}
};

class InvalidTimeOrder : public std::invalid_argument
{
public:
  InvalidTimeOrder(long long start, long long end) : std::invalid_argument(fmt::format("argument start timestamp {} must be less equal(<=) than end {}", start, end)) {}
};

class CalendarNotFound : public std::out_of_range
{
public:
  CalendarNotFound(std::string_view calendar_name, std::string_view symbol) : std::out_of_range(fmt::format("calendar {} of {} is not found", calendar_name, symbol)) {}
};

NS_QMC_END
