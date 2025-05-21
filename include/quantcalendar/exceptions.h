#pragma once
#include <stdexcept>
#include "quantcalendar/qmc_globals.h"

NS_QMC_BEGIN

class OutOfCalendar : public std::out_of_range
{
public:
  OutOfCalendar();
};

class CalendarNotInit : public std::runtime_error
{
public:
  CalendarNotInit();
};

class InvalidInterval : public std::invalid_argument
{
public:
  InvalidInterval(int interval);
};

class InvalidTimeOrder : public std::invalid_argument
{
public:
  InvalidTimeOrder(long long start, long long end);
};

class CalendarNotFound : public std::out_of_range
{
public:
  CalendarNotFound(std::string_view calendar_name, std::string_view symbol);
};

NS_QMC_END
