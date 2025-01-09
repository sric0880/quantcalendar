#include <functional>

#include "quantcalendar/calendar_data.h"

NS_QMC_BEGIN

#pragma region CalendarData
CalendarData::tradedays_iterator &CalendarData::tradedays_iterator::operator++()
{
  do
  {
    if (it_ == end() || (++it_) == end())
      break;
  } while (!it_->second.IsTrading());
  return *this;
}

CalendarData::tradedays_iterator &CalendarData::tradedays_iterator::operator--()
{
  do
  {
    if (it_ == rend() || (--it_) == rend())
      break;
  } while (!it_->second.IsTrading());
  return *this;
}

CalendarData::month_begin_iterator &CalendarData::month_begin_iterator::operator++()
{
  do
  {
    if (it_ == end() || (++it_) == end())
      break;
  } while (!it_->second.IsMonthBegin());
  return *this;
}

CalendarData::month_begin_iterator &CalendarData::month_begin_iterator::operator--()
{
  do
  {
    if (it_ == rend() || (--it_) == rend())
      break;
  } while (!it_->second.IsMonthBegin());
  return *this;
}

CalendarData::month_end_iterator &CalendarData::month_end_iterator::operator++()
{
  do
  {
    if (it_ == end() || (++it_) == end())
      break;
  } while (!it_->second.IsMonthEnd());
  return *this;
}

CalendarData::month_end_iterator &CalendarData::month_end_iterator::operator--()
{
  do
  {
    if (it_ == rend() || (--it_) == rend())
      break;
  } while (!it_->second.IsMonthEnd());
  return *this;
}

CalendarData::week_begin_iterator &CalendarData::week_begin_iterator::operator++()
{
  do
  {
    if (it_ == end() || (++it_) == end())
      break;
  } while (!it_->second.IsWeekBegin());
  return *this;
}

CalendarData::week_begin_iterator &CalendarData::week_begin_iterator::operator--()
{
  do
  {
    if (it_ == rend() || (--it_) == rend())
      break;
  } while (!it_->second.IsWeekBegin());
  return *this;
}

CalendarData::week_end_iterator &CalendarData::week_end_iterator::operator++()
{
  do
  {
    if (it_ == end() || (++it_) == end())
      break;
  } while (!it_->second.IsWeekEnd());
  return *this;
}

CalendarData::week_end_iterator &CalendarData::week_end_iterator::operator--()
{
  do
  {
    if (it_ == rend() || (--it_) == rend())
      break;
  } while (!it_->second.IsWeekEnd());
  return *this;
}

CalendarData::weekday_iterator &CalendarData::weekday_iterator::operator++()
{
  do
  {
    if (it_ == end() || (++it_) == end())
      break;
  } while (!it_->second.IsWeekDay(weekday_));
  return *this;
}

CalendarData::weekday_iterator &CalendarData::weekday_iterator::operator--()
{
  do
  {
    if (it_ == rend() || (--it_) == rend())
      break;
  } while (!it_->second.IsWeekDay(weekday_));
  return *this;
}

void CalendarData::InitData(const std::vector<calendar_item> &calendar_data)
{
  for (auto &[ts, status] : calendar_data)
  {
    calendar_data_.emplace(ts, CalendarDataNode(ts, status));
  }
  end_ = calendar_data_.cend();
  rend_ = calendar_data_.cbegin() - 1;
  const CalendarDataNode *pre_node = nullptr;
  for (auto &value : calendar_data_.values())
  {
    auto &node = value.second;
    if (!node.IsTrading())
      continue;
    if (pre_node)
    {
      if (node.dt_.date.mon != pre_node->dt_.date.mon)
      {
        pre_node->SetMonthEnd(true);
        node.SetMonthBegin(true);
      }
      if (node.cdate_.week != pre_node->cdate_.week)
      {
        pre_node->SetWeekEnd(true);
        node.SetWeekBegin(true);
      }
    }
    pre_node = &node;
  }
}

CalendarData::tradedays_iterator CalendarData::TradedaysUpper(sec_t dt) const
{
  auto it = SafeFind<CalendarData::tradedays_iterator>(dt);
  if (!it.is_end() && !it->second.IsTrading())
    ++it;
  return it;
}

CalendarData::tradedays_iterator CalendarData::TradedaysLower(sec_t dt) const
{
  auto it = SafeFind<CalendarData::tradedays_iterator>(dt);
  if (!it.is_end() && !it->second.IsTrading())
    --it;
  return it;
}

CalendarData::month_end_iterator CalendarData::MonthEndUpper(sec_t dt) const
{
  auto it = SafeFind<CalendarData::month_end_iterator>(dt);
  if (!it.is_end() && !it->second.IsMonthEnd())
    ++it;
  return it;
}

CalendarData::month_end_iterator CalendarData::MonthEndLower(sec_t dt) const
{
  auto it = SafeFind<CalendarData::month_end_iterator>(dt);
  if (!it.is_end() && !it->second.IsMonthEnd())
    --it;
  return it;
}

CalendarData::month_begin_iterator CalendarData::MonthBeginUpper(sec_t dt) const
{
  auto it = SafeFind<CalendarData::month_begin_iterator>(dt);
  if (!it.is_end() && !it->second.IsMonthBegin())
    ++it;
  return it;
}

CalendarData::month_begin_iterator CalendarData::MonthBeginLower(sec_t dt) const
{
  auto it = SafeFind<CalendarData::month_begin_iterator>(dt);
  if (!it.is_end() && !it->second.IsMonthBegin())
    --it;
  return it;
}

CalendarData::week_end_iterator CalendarData::WeekEndUpper(sec_t dt) const
{
  auto it = SafeFind<CalendarData::week_end_iterator>(dt);
  if (!it.is_end() && !it->second.IsWeekEnd())
    ++it;
  return it;
}

CalendarData::week_end_iterator CalendarData::WeekEndLower(sec_t dt) const
{
  auto it = SafeFind<CalendarData::week_end_iterator>(dt);
  if (!it.is_end() && !it->second.IsWeekEnd())
    --it;
  return it;
}

CalendarData::week_begin_iterator CalendarData::WeekBeginUpper(sec_t dt) const
{
  auto it = SafeFind<CalendarData::week_begin_iterator>(dt);
  if (!it.is_end() && !it->second.IsWeekBegin())
    ++it;
  return it;
}

CalendarData::week_begin_iterator CalendarData::WeekBeginLower(sec_t dt) const
{
  auto it = SafeFind<CalendarData::week_begin_iterator>(dt);
  if (!it.is_end() && !it->second.IsWeekBegin())
    --it;
  return it;
}

CalendarData::weekday_iterator CalendarData::WeekDayUpper(sec_t dt, int weekday) const
{
  auto it = SafeFind<CalendarData::weekday_iterator>(dt, weekday);
  if (!it.is_end() && !it->second.IsWeekDay(weekday))
    ++it;
  return it;
}

CalendarData::weekday_iterator CalendarData::WeekDayLower(sec_t dt, int weekday) const
{
  auto it = SafeFind<CalendarData::weekday_iterator>(dt, weekday);
  if (!it.is_end() && !it->second.IsWeekDay(weekday))
    --it;
  return it;
}
#pragma endregion

#pragma region Calendar7x24Data
inline bool is_leap_year(int year)
{
  return (year % 100 != 0 && year % 4 == 0) || (year % 400 == 0);
}

const std::vector<int> month_days{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
int num_days_month(int year, int month)
{
  if (month == 2 && is_leap_year(year))
    return 29;
  return month_days[month - 1];
}

void next_month_end(Date &date)
{
  int mon = date.mon + 1;
  if (mon == 13)
  {
    date.year += 1;
    date.mon = 1;
    date.day = 31;
  }
  else
  {
    date.mon = mon;
    date.day = num_days_month(date.year, mon);
  }
}

void pre_month_end(Date &date)
{
  int mon = date.mon - 1;
  if (mon == 0)
  {
    date.year -= 1;
    date.mon = 12;
    date.day = 31;
  }
  else
  {
    date.mon = mon;
    date.day = num_days_month(date.year, mon);
  }
}

void next_month_begin(Date &date)
{
  date.day = 1;
  int mon = date.mon + 1;
  if (mon == 13)
  {
    date.year += 1;
    date.mon = 1;
  }
  else
  {
    date.mon = mon;
  }
}

void pre_month_begin(Date &date)
{
  date.day = 1;
  int mon = date.mon - 1;
  if (mon == 0)
  {
    date.year -= 1;
    date.mon = 12;
  }
  else
  {
    date.mon = mon;
  }
}

void next_date(Date &date)
{
  int y = date.year;
  int m = date.mon;
  int d = date.day;
  int days = num_days_month(y, m);
  if (d < days)
    ++date.day;
  else
  {
    next_month_begin(date);
  }
}

void pre_date(Date &date)
{
  int y = date.year;
  int m = date.mon;
  int d = date.day;
  if (d > 1)
    --date.day;
  else
  {
    pre_month_end(date);
  }
}

inline void next_week(Date &date)
{
  int next = 7;
  do
  {
    next_date(date);
  } while (--next);
}

inline void pre_week(Date &date)
{
  int next = 7;
  do
  {
    pre_date(date);
  } while (--next);
}

inline void update_current(Calendar7x24Data::iterator::value_type &current)
{
  current.second.cdate_ = current.second.dt_.date.isocalendar();
  current.second.dt_.calc_timestamp();
  current.first = current.second.dt_.to_timestamp();
}

inline void check_month_end_upper(Calendar7x24Data::iterator::value_type &current)
{
  int year = current.second.dt_.date.year;
  int mon = current.second.dt_.date.mon;
  current.second.dt_.date.day = num_days_month(year, mon);
  update_current(current);
}

void check_month_end_lower(Calendar7x24Data::iterator::value_type &current)
{
  int year = current.second.dt_.date.year;
  int mon = current.second.dt_.date.mon;
  int day = current.second.dt_.date.day;
  int days = num_days_month(year, mon);
  if (day == days)
    return;
  else
  {
    pre_month_end(current.second.dt_.date);
    update_current(current);
  }
}

void check_month_begin_upper(Calendar7x24Data::iterator::value_type &current)
{
  int day = current.second.dt_.date.day;
  if (day == 1)
    return;
  else
  {
    next_month_begin(current.second.dt_.date);
    update_current(current);
  }
}

inline void check_month_begin_lower(Calendar7x24Data::iterator::value_type &current)
{
  current.second.dt_.date.day = 1;
  update_current(current);
}

void check_week_day_upper(Calendar7x24Data::iterator::value_type &current, int weekday)
{
  while (current.second.cdate_.weekday != weekday)
  {
    next_date(current.second.dt_.date);
    update_current(current);
  }
}

void check_week_day_lower(Calendar7x24Data::iterator::value_type &current, int weekday)
{
  while (current.second.cdate_.weekday != weekday)
  {
    pre_date(current.second.dt_.date);
    update_current(current);
  }
}

Calendar7x24Data::tradedays_iterator &Calendar7x24Data::tradedays_iterator::operator++()
{
  if (!is_end())
  {
    next_date(current_.second.dt_.date);
    update_current(current_);
  }
  return *this;
}

Calendar7x24Data::tradedays_iterator &Calendar7x24Data::tradedays_iterator::operator--()
{
  if (!is_end())
  {
    pre_date(current_.second.dt_.date);
    update_current(current_);
  }
  return *this;
}

Calendar7x24Data::month_begin_iterator &Calendar7x24Data::month_begin_iterator::operator++()
{
  if (!is_end())
  {
    next_month_begin(current_.second.dt_.date);
    update_current(current_);
  }
  return *this;
}

Calendar7x24Data::month_begin_iterator &Calendar7x24Data::month_begin_iterator::operator--()
{
  if (!is_end())
  {
    pre_month_begin(current_.second.dt_.date);
    update_current(current_);
  }
  return *this;
}

Calendar7x24Data::month_end_iterator &Calendar7x24Data::month_end_iterator::operator++()
{
  if (!is_end())
  {
    next_month_end(current_.second.dt_.date);
    update_current(current_);
  }
  return *this;
}

Calendar7x24Data::month_end_iterator &Calendar7x24Data::month_end_iterator::operator--()
{
  if (!is_end())
  {
    pre_month_end(current_.second.dt_.date);
    update_current(current_);
  }
  return *this;
}

Calendar7x24Data::weekday_iterator &Calendar7x24Data::weekday_iterator::operator++()
{
  if (!is_end())
  {
    next_week(current_.second.dt_.date);
    update_current(current_);
  }
  return *this;
}

Calendar7x24Data::weekday_iterator &Calendar7x24Data::weekday_iterator::operator--()
{
  if (!is_end())
  {
    pre_week(current_.second.dt_.date);
    update_current(current_);
  }
  return *this;
}

Calendar7x24Data::month_end_iterator Calendar7x24Data::MonthEndUpper(sec_t dt) const
{
  auto it = SafeFind<Calendar7x24Data::month_end_iterator>(dt);
  if (!it.is_end())
    check_month_end_upper(it.current_);
  return it;
}

Calendar7x24Data::month_end_iterator Calendar7x24Data::MonthEndLower(sec_t dt) const
{
  auto it = SafeFind<Calendar7x24Data::month_end_iterator>(dt);
  if (!it.is_end())
    check_month_end_lower(it.current_);
  return it;
}

Calendar7x24Data::month_begin_iterator Calendar7x24Data::MonthBeginUpper(sec_t dt) const
{
  auto it = SafeFind<Calendar7x24Data::month_begin_iterator>(dt);
  if (!it.is_end())
    check_month_begin_upper(it.current_);
  return it;
}

Calendar7x24Data::month_begin_iterator Calendar7x24Data::MonthBeginLower(sec_t dt) const
{
  auto it = SafeFind<Calendar7x24Data::month_begin_iterator>(dt);
  if (!it.is_end())
    check_month_begin_lower(it.current_);
  return it;
}

Calendar7x24Data::weekday_iterator Calendar7x24Data::WeekDayUpper(sec_t dt, int weekday) const
{
  auto it = SafeFind<Calendar7x24Data::weekday_iterator>(dt, weekday);
  if (!it.is_end())
    check_week_day_upper(it.current_, weekday);
  return it;
}

Calendar7x24Data::weekday_iterator Calendar7x24Data::WeekDayLower(sec_t dt, int weekday) const
{
  auto it = SafeFind<Calendar7x24Data::weekday_iterator>(dt, weekday);
  if (!it.is_end())
    check_week_day_lower(it.current_, weekday);
  return it;
}
#pragma endregion

NS_QMC_END
