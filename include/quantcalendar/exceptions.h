#pragma once
#include <stdexcept>
#include "quantcalendar/qmc_globals.h"

NS_QMC_BEGIN

class OutOfCalendar : public std::out_of_range
{
public:
  OutOfCalendar() : std::out_of_range("日历越界，请更新日历") {}
};

NS_QMC_END
