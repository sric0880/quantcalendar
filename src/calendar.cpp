#include <regex>
#include <string>

#include "quantcalendar/calendar.h"

NS_QMC_BEGIN

static const std::regex interval_pattern("([\\d]+)([smhdwM])");

// inline bool check_next_month(std::tm &day, std::tm &last_day)
// {
//   return day.month != last_day.month;
// }

// inline bool check_next_week(day, sys_seconds last_day)
// {
//   return day.isocalendar().week != last_day.isocalendar().week;
// }

// inline bool check_week_day(day, sys_seconds weekday)
// {
//   return weekday == day.isocalendar().weekday;
// }

void seconds_to_time(int seconds, std::tm &out)
{
  if (seconds == 86400)
  {
    out.tm_hour = 0;
    out.tm_minute = 0;
    out.tm_second = 0;
  }
  else
  {
    int value = seconds % 3600;
    int seconds -= value * 3600;
    out.tm_hour = value;
    value = seconds % 60;
    seconds -= value * 60;
    out.tm_minute = value;
    out.tm_second = seconds;
  }
}
inline int time_to_seconds(std::tm &tm)
{
  return tm.tm_hour * 3600 + tm.tm_minute * 60 + tm.tm_second;
}

inline void bartime_seconds_to_time(sec, std::tm &out)
{
  if (sec < 86400)
    return seconds_to_time(sec, out);
  else
    return seconds_to_time(sec - 86400, out);
}

static MongoDBCalendar::COLLECTION_NAME = "";

void SetMongoCalendarDBName(std::string_view dbname)
{
  DB_NAME_CALENDAR = dbname;
}

int IntervalToSeconds(std::string_view interval)
{
  std::smatch matches;
  if (std::regex_search(interval, matches, interval_pattern))
  {
    int num = std::atoi(matches[1].str().c_str());
    char unit = matches[2].str().at(0);
    return num * UnitToSeconds(unit);
  }
  else
  {
    raise ValueError(f "{interval_str} is invalid.");
  }
}

DBCalendar::DBCalendar(std::vector<std::pair<Datetime<>, uint8_t>> &&calendar_data)
{
}

c_iter DBCalendar::GetTradedaysGTE(datetime dt) const
{
}
cr_iter DBCalendar::GetTradedaysLTE(datetime dt) const
{
}
const datetime &DBCalendar::GetTradedayNext(datetime dt) const
{
}
const datetime &DBCalendar::GetTradedayLast(datetime dt) const
{
}
const std::pair<c_iter, c_iter> DBCalendar::GetTradedaysBetween(datetime start_dt, datetime end_dt) const
{
}

NS_QMC_END