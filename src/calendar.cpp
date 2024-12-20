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

DBCalendar::DBCalendar(const std::vector<std::pair<sec_t, uint8_t>> &calendar_data)
{
  for (auto& [ts, status] : calendar_data)
  {
    int index = tradedays_.size();
    if (status == 1)
    {
      // trading day
      tradedays_.emplace_back(datetime::precision(ts));
    }
    tradedays_indexers_.try_emplace(ts, index);
  }
}

c_iter DBCalendar::GetTradedaysGTE(const datetime &dt) const
{
  return GetTradedaysGTE(dt.to_timestamp());
}
c_iter DBCalendar::GetTradedaysGTE(sec_t dt) const
{
  return tradedays_.cbegin() + tradedays_indexers_.at(dt);
}

cr_iter DBCalendar::GetTradedaysLTE(const datetime &dt) const
{
  return GetTradedaysLTE(dt.to_timestamp());
}
cr_iter DBCalendar::GetTradedaysLTE(sec_t dt) const
{
  return tradedays_.crbegin() + (tradedays_.size() - tradedays_indexers_.at(dt));
}

const datetime* DBCalendar::GetTradedayNext(const datetime &dt) const
{
  return GetTradedayNext(dt.to_timestamp());
}
const datetime* DBCalendar::GetTradedayNext(sec_t dt) const
{
  auto i = tradedays_indexers_.at(dt);
  return i >= tradedays_.size() ? nullptr : &tradedays_[i];
}

const datetime *DBCalendar::GetTradedayLast(const datetime &dt) const
{
  return GetTradedayLast(dt.to_timestamp());
}
const datetime* DBCalendar::GetTradedayLast(sec_t dt) const
{
  auto i = tradedays_indexers_.at(dt) - 1;
  return i < 0 ? nullptr : &tradedays_[i];
}

std::pair<c_iter, c_iter> DBCalendar::GetTradedaysBetween(const datetime &start_dt, const datetime &end_dt) const
{
  return GetTradedaysBetween(start_dt.to_timestamp(), end_dt.to_timestamp());
}
std::pair<c_iter, c_iter> DBCalendar::GetTradedaysBetween(sec_t start_dt, sec_t end_dt) const
{
  auto s = tradedays_indexers_.at(start_dt);
  auto e = tradedays_indexers_.at(end_dt);
  auto cit = tradedays_.cbegin();
  return { cit + s, cit + e };
}

NS_QMC_END