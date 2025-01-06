#include <functional>

#include "quantcalendar/calendar_data.h"

NS_QMC_BEGIN

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

inline CalendarData::iter_range<CalendarData::tradedays_iterator> CalendarData::TradedaysBetween(sec_t start, sec_t end) const
{
  return CalendarData::iter_range<CalendarData::tradedays_iterator>{TradedaysUpper(start), TradedaysUpper(end)};
}

CalendarData::month_end_iterator CalendarData::MonthEndUpper(sec_t dt) const
{
  auto it = SafeFind<CalendarData::month_end_iterator>(dt);
  if (!it.is_end()  && !it->second.IsMonthEnd())
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

inline CalendarData::iter_range<CalendarData::month_end_iterator> CalendarData::MonthEndBetween(sec_t start, sec_t end) const
{
  return CalendarData::iter_range<CalendarData::month_end_iterator>{MonthEndUpper(start), MonthEndUpper(end)};
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

inline CalendarData::iter_range<CalendarData::month_begin_iterator> CalendarData::MonthBeginBetween(sec_t start, sec_t end) const
{
  return CalendarData::iter_range<CalendarData::month_begin_iterator>{MonthBeginUpper(start), MonthBeginUpper(end)};
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

inline CalendarData::iter_range<CalendarData::week_end_iterator> CalendarData::WeekEndBetween(sec_t start, sec_t end) const
{
  return CalendarData::iter_range<CalendarData::week_end_iterator>{WeekEndUpper(start), WeekEndUpper(end)};
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

inline CalendarData::iter_range<CalendarData::week_begin_iterator> CalendarData::WeekBeginBetween(sec_t start, sec_t end) const
{
  return CalendarData::iter_range<CalendarData::week_begin_iterator>{WeekBeginUpper(start), WeekBeginUpper(start)};
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

inline CalendarData::iter_range<CalendarData::weekday_iterator> CalendarData::WeekDayBetween(sec_t start, sec_t end, int weekday) const
{
  return CalendarData::iter_range<CalendarData::weekday_iterator>{WeekDayUpper(start, weekday), WeekDayUpper(end, weekday)};
}

NS_QMC_END
