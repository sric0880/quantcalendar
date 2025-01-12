#include <functional>

#include "quantcalendar/dates.h"

NS_QMC_BEGIN

#pragma region DatesArray
DatesArray::tradedays_iterator &DatesArray::tradedays_iterator::operator++()
{
  do
  {
    if (it_ == end() || (++it_) == end())
      break;
  } while (!it_->second.IsTrading());
  return *this;
}

DatesArray::tradedays_iterator &DatesArray::tradedays_iterator::operator--()
{
  do
  {
    if (it_ == rend() || (--it_) == rend())
      break;
  } while (!it_->second.IsTrading());
  return *this;
}

DatesArray::month_begin_iterator &DatesArray::month_begin_iterator::operator++()
{
  do
  {
    if (it_ == end() || (++it_) == end())
      break;
  } while (!it_->second.IsMonthBegin());
  return *this;
}

DatesArray::month_begin_iterator &DatesArray::month_begin_iterator::operator--()
{
  do
  {
    if (it_ == rend() || (--it_) == rend())
      break;
  } while (!it_->second.IsMonthBegin());
  return *this;
}

DatesArray::month_end_iterator &DatesArray::month_end_iterator::operator++()
{
  do
  {
    if (it_ == end() || (++it_) == end())
      break;
  } while (!it_->second.IsMonthEnd());
  return *this;
}

DatesArray::month_end_iterator &DatesArray::month_end_iterator::operator--()
{
  do
  {
    if (it_ == rend() || (--it_) == rend())
      break;
  } while (!it_->second.IsMonthEnd());
  return *this;
}

DatesArray::week_begin_iterator &DatesArray::week_begin_iterator::operator++()
{
  do
  {
    if (it_ == end() || (++it_) == end())
      break;
  } while (!it_->second.IsWeekBegin());
  return *this;
}

DatesArray::week_begin_iterator &DatesArray::week_begin_iterator::operator--()
{
  do
  {
    if (it_ == rend() || (--it_) == rend())
      break;
  } while (!it_->second.IsWeekBegin());
  return *this;
}

DatesArray::week_end_iterator &DatesArray::week_end_iterator::operator++()
{
  do
  {
    if (it_ == end() || (++it_) == end())
      break;
  } while (!it_->second.IsWeekEnd());
  return *this;
}

DatesArray::week_end_iterator &DatesArray::week_end_iterator::operator--()
{
  do
  {
    if (it_ == rend() || (--it_) == rend())
      break;
  } while (!it_->second.IsWeekEnd());
  return *this;
}

DatesArray::weekday_iterator &DatesArray::weekday_iterator::operator++()
{
  do
  {
    if (it_ == end() || (++it_) == end())
      break;
  } while (!it_->second.IsWeekDay(weekday_));
  return *this;
}

DatesArray::weekday_iterator &DatesArray::weekday_iterator::operator--()
{
  do
  {
    if (it_ == rend() || (--it_) == rend())
      break;
  } while (!it_->second.IsWeekDay(weekday_));
  return *this;
}

void DatesArray::Init(const std::vector<date_status_item> &arr)
{
  for (auto &[ts, status] : arr)
  {
    calendar_data_.emplace(ts, DateNode(ts, status));
  }
  end_ = calendar_data_.cend();
  rend_ = calendar_data_.cbegin() - 1;
  const DateNode *pre_node = nullptr;
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

DatesArray::tradedays_iterator DatesArray::Upper(sec_t dt) const
{
  auto it = SafeFind<DatesArray::tradedays_iterator>(dt);
  if (!it.is_end() && !it->second.IsTrading())
    ++it;
  return it;
}

DatesArray::tradedays_iterator DatesArray::Lower(sec_t dt) const
{
  auto it = SafeFind<DatesArray::tradedays_iterator>(dt);
  if (!it.is_end() && !it->second.IsTrading())
    --it;
  return it;
}

DatesArray::month_end_iterator DatesArray::MonthEndUpper(sec_t dt) const
{
  auto it = SafeFind<DatesArray::month_end_iterator>(dt);
  if (!it.is_end() && !it->second.IsMonthEnd())
    ++it;
  return it;
}

DatesArray::month_end_iterator DatesArray::MonthEndLower(sec_t dt) const
{
  auto it = SafeFind<DatesArray::month_end_iterator>(dt);
  if (!it.is_end() && !it->second.IsMonthEnd())
    --it;
  return it;
}

DatesArray::month_begin_iterator DatesArray::MonthBeginUpper(sec_t dt) const
{
  auto it = SafeFind<DatesArray::month_begin_iterator>(dt);
  if (!it.is_end() && !it->second.IsMonthBegin())
    ++it;
  return it;
}

DatesArray::month_begin_iterator DatesArray::MonthBeginLower(sec_t dt) const
{
  auto it = SafeFind<DatesArray::month_begin_iterator>(dt);
  if (!it.is_end() && !it->second.IsMonthBegin())
    --it;
  return it;
}

DatesArray::week_end_iterator DatesArray::WeekEndUpper(sec_t dt) const
{
  auto it = SafeFind<DatesArray::week_end_iterator>(dt);
  if (!it.is_end() && !it->second.IsWeekEnd())
    ++it;
  return it;
}

DatesArray::week_end_iterator DatesArray::WeekEndLower(sec_t dt) const
{
  auto it = SafeFind<DatesArray::week_end_iterator>(dt);
  if (!it.is_end() && !it->second.IsWeekEnd())
    --it;
  return it;
}

DatesArray::week_begin_iterator DatesArray::WeekBeginUpper(sec_t dt) const
{
  auto it = SafeFind<DatesArray::week_begin_iterator>(dt);
  if (!it.is_end() && !it->second.IsWeekBegin())
    ++it;
  return it;
}

DatesArray::week_begin_iterator DatesArray::WeekBeginLower(sec_t dt) const
{
  auto it = SafeFind<DatesArray::week_begin_iterator>(dt);
  if (!it.is_end() && !it->second.IsWeekBegin())
    --it;
  return it;
}

DatesArray::weekday_iterator DatesArray::WeekDayUpper(sec_t dt, int weekday) const
{
  auto it = SafeFind<DatesArray::weekday_iterator>(dt, weekday);
  if (!it.is_end() && !it->second.IsWeekDay(weekday))
    ++it;
  return it;
}

DatesArray::weekday_iterator DatesArray::WeekDayLower(sec_t dt, int weekday) const
{
  auto it = SafeFind<DatesArray::weekday_iterator>(dt, weekday);
  if (!it.is_end() && !it->second.IsWeekDay(weekday))
    --it;
  return it;
}
#pragma endregion

#pragma region Date7x24Array
inline bool is_leap_year(int year)
{
  return (year % 100 != 0 && year % 4 == 0) || (year % 400 == 0);
}

const int month_days[]{-1, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
int num_days_month(int year, int month)
{
  if (month == 2 && is_leap_year(year))
    return 29;
  return month_days[month];
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

inline void update_current(Date7x24Array::iterator::value_type &current)
{
  current.second.cdate_ = current.second.dt_.date.isocalendar();
  current.second.dt_.calc_timestamp();
  current.first = current.second.dt_.to_timestamp();
}

inline void check_month_end_upper(Date7x24Array::iterator::value_type &current)
{
  int year = current.second.dt_.date.year;
  int mon = current.second.dt_.date.mon;
  current.second.dt_.date.day = num_days_month(year, mon);
  update_current(current);
}

void check_month_end_lower(Date7x24Array::iterator::value_type &current)
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

void check_month_begin_upper(Date7x24Array::iterator::value_type &current)
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

inline void check_month_begin_lower(Date7x24Array::iterator::value_type &current)
{
  current.second.dt_.date.day = 1;
  update_current(current);
}

void check_week_day_upper(Date7x24Array::iterator::value_type &current, int weekday)
{
  while (current.second.cdate_.weekday != weekday)
  {
    next_date(current.second.dt_.date);
    update_current(current);
  }
}

void check_week_day_lower(Date7x24Array::iterator::value_type &current, int weekday)
{
  while (current.second.cdate_.weekday != weekday)
  {
    pre_date(current.second.dt_.date);
    update_current(current);
  }
}

Date7x24Array::tradedays_iterator &Date7x24Array::tradedays_iterator::operator++()
{
  if (!is_end())
  {
    next_date(current_.second.dt_.date);
    update_current(current_);
  }
  return *this;
}

Date7x24Array::tradedays_iterator &Date7x24Array::tradedays_iterator::operator--()
{
  if (!is_end())
  {
    pre_date(current_.second.dt_.date);
    update_current(current_);
  }
  return *this;
}

Date7x24Array::month_begin_iterator &Date7x24Array::month_begin_iterator::operator++()
{
  if (!is_end())
  {
    next_month_begin(current_.second.dt_.date);
    update_current(current_);
  }
  return *this;
}

Date7x24Array::month_begin_iterator &Date7x24Array::month_begin_iterator::operator--()
{
  if (!is_end())
  {
    pre_month_begin(current_.second.dt_.date);
    update_current(current_);
  }
  return *this;
}

Date7x24Array::month_end_iterator &Date7x24Array::month_end_iterator::operator++()
{
  if (!is_end())
  {
    next_month_end(current_.second.dt_.date);
    update_current(current_);
  }
  return *this;
}

Date7x24Array::month_end_iterator &Date7x24Array::month_end_iterator::operator--()
{
  if (!is_end())
  {
    pre_month_end(current_.second.dt_.date);
    update_current(current_);
  }
  return *this;
}

Date7x24Array::weekday_iterator &Date7x24Array::weekday_iterator::operator++()
{
  if (!is_end())
  {
    next_week(current_.second.dt_.date);
    update_current(current_);
  }
  return *this;
}

Date7x24Array::weekday_iterator &Date7x24Array::weekday_iterator::operator--()
{
  if (!is_end())
  {
    pre_week(current_.second.dt_.date);
    update_current(current_);
  }
  return *this;
}

Date7x24Array::month_end_iterator Date7x24Array::MonthEndUpper(sec_t dt) const
{
  auto it = SafeFind<Date7x24Array::month_end_iterator>(dt);
  if (!it.is_end())
    check_month_end_upper(it.current_);
  return it;
}

Date7x24Array::month_end_iterator Date7x24Array::MonthEndLower(sec_t dt) const
{
  auto it = SafeFind<Date7x24Array::month_end_iterator>(dt);
  if (!it.is_end())
    check_month_end_lower(it.current_);
  return it;
}

Date7x24Array::month_begin_iterator Date7x24Array::MonthBeginUpper(sec_t dt) const
{
  auto it = SafeFind<Date7x24Array::month_begin_iterator>(dt);
  if (!it.is_end())
    check_month_begin_upper(it.current_);
  return it;
}

Date7x24Array::month_begin_iterator Date7x24Array::MonthBeginLower(sec_t dt) const
{
  auto it = SafeFind<Date7x24Array::month_begin_iterator>(dt);
  if (!it.is_end())
    check_month_begin_lower(it.current_);
  return it;
}

Date7x24Array::weekday_iterator Date7x24Array::WeekDayUpper(sec_t dt, int weekday) const
{
  auto it = SafeFind<Date7x24Array::weekday_iterator>(dt, weekday);
  if (!it.is_end())
    check_week_day_upper(it.current_, weekday);
  return it;
}

Date7x24Array::weekday_iterator Date7x24Array::WeekDayLower(sec_t dt, int weekday) const
{
  auto it = SafeFind<Date7x24Array::weekday_iterator>(dt, weekday);
  if (!it.is_end())
    check_week_day_lower(it.current_, weekday);
  return it;
}
#pragma endregion

NS_QMC_END
