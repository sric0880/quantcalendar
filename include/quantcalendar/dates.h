#pragma once
#include <string>
#include <tuple>
#include <bitset>
#include "quantcalendar/datetime.h"

#include "quantcalendar/qmc_globals.h"
#include "quantcalendar/exceptions.h"
#include "ankerl/unordered_dense.h"

NS_QMC_BEGIN

using namespace std::literals::chrono_literals;
using datetime = Datetime<seconds>;
using calendar_date = IsoCalendarDate;
using sec_t = datetime::precision_type::rep;
using date_status_item = std::tuple<sec_t /*timestamp*/, char /*status*/>;
template <class Iter>
using dpair = std::pair<Iter, Iter>;

constexpr const int iseconds_a_day = 86400;
constexpr const seconds seconds_a_day(86400);

constexpr seconds operator""_d(unsigned long long __d)
{
  return seconds(static_cast<sec_t>(__d * iseconds_a_day));
}

constexpr seconds operator""_w(unsigned long long __d)
{
  return seconds(static_cast<sec_t>(__d * 7 * iseconds_a_day));
}

constexpr seconds operator""_m(unsigned long long __d)
{
  return seconds(static_cast<sec_t>(__d * 30 * iseconds_a_day));
}

inline bool is_daily(sec_t dt)
{
  return dt % iseconds_a_day == 0;
}

struct DateNode
{
  DateNode(sec_t ts, uint8_t status) : dt_(datetime::precision_type(ts)), cdate_(dt_.date.isocalendar()), status_(status)
  {
    SetTrading(status == 1);
  }
  datetime dt_;
  calendar_date cdate_;
  uint8_t status_;
  mutable std::bitset<8> state_;
  bool IsTrading() const { return state_[0]; };
  bool IsMonthBegin() const { return state_[1]; };
  bool IsMonthEnd() const { return state_[2]; };
  bool IsWeekBegin() const { return state_[3]; };
  bool IsWeekEnd() const { return state_[4]; };
  bool IsWeekDay(int weekday) const { return cdate_.weekday == weekday && state_[0]; };
  void SetTrading(bool on) const { state_[0] = on; }
  void SetMonthBegin(bool on) const { state_[1] = on; }
  void SetMonthEnd(bool on) const { state_[2] = on; }
  void SetWeekBegin(bool on) const { state_[3] = on; }
  void SetWeekEnd(bool on) const { state_[4] = on; }
};

class DatesArray
{
public:
  using calendar_data_map = ankerl::unordered_dense::map<sec_t, DateNode>;
  using const_map_iterator = calendar_data_map::value_container_type::const_iterator;
  class iterator
  {
  protected:
    const_map_iterator it_;
    const DatesArray *arr_;
    const const_map_iterator &rend() const { return arr_->rend_; };
    const const_map_iterator &end() const { return arr_->end_; };

  public:
    // iterator traits
    using difference_type = long;
    using value_type = const_map_iterator::value_type;
    using pointer = const_map_iterator::pointer;
    using reference = const_map_iterator::reference;
    using iterator_category = std::bidirectional_iterator_tag;
    iterator() = default;
    iterator(const_map_iterator &&it, const DatesArray *container) : it_(std::move(it)), arr_(container) {}
    iterator(const iterator &) = default;
    iterator &operator=(const iterator &iter) = default;
    bool operator==(const iterator &other) const { return it_ == other.it_; }
    bool operator!=(const iterator &other) const { return it_ != other.it_; }
    bool operator>(const iterator &other) const { return it_->first > other.it_->first; }
    bool operator>=(const iterator &other) const { return it_->first >= other.it_->first; }
    bool operator<(const iterator &other) const { return it_->first < other.it_->first; }
    bool operator<=(const iterator &other) const { return it_->first <= other.it_->first; }
    virtual iterator &operator++() = 0;
    virtual iterator &operator--() = 0;
    reference operator*() const { return *it_; }
    pointer operator->() const { return it_.const_map_iterator::operator->(); }
    bool is_end() const { return (it_ == end()) || (it_ == rend()); }
  };
  friend iterator;

  class tradedays_iterator : public iterator
  {
  public:
    using iterator::iterator;
    tradedays_iterator &operator++() final override;
    tradedays_iterator &operator--() final override;
  };

  class month_begin_iterator : public iterator
  {
  public:
    using iterator::iterator;
    month_begin_iterator &operator++() final override;
    month_begin_iterator &operator--() final override;
  };

  class month_end_iterator : public iterator
  {
  public:
    using iterator::iterator;
    month_end_iterator &operator++() final override;
    month_end_iterator &operator--() final override;
  };

  class week_begin_iterator : public iterator
  {
  public:
    using iterator::iterator;
    week_begin_iterator &operator++() final override;
    week_begin_iterator &operator--() final override;
  };

  class week_end_iterator : public iterator
  {
  public:
    using iterator::iterator;
    week_end_iterator &operator++() final override;
    week_end_iterator &operator--() final override;
  };

  class weekday_iterator : public iterator
  {
  public:
    weekday_iterator() = default;
    weekday_iterator(const weekday_iterator &) = default;
    weekday_iterator &operator=(const weekday_iterator &iter) = default;
    weekday_iterator(const_map_iterator &&it, const DatesArray *container, int weekday) : iterator(std::move(it), container), weekday_(weekday) {}
    weekday_iterator &operator++() final override;
    weekday_iterator &operator--() final override;

  private:
    int weekday_;
  };

  void Init(const std::vector<date_status_item> &arr);
  tradedays_iterator Upper(sec_t dt) const;
  tradedays_iterator Lower(sec_t dt) const;
  dpair<tradedays_iterator> Between(sec_t start, sec_t end) const
  {
    if (start > end)
      throw InvalidTimeOrder(start, end);
    return {Upper(start), Lower(end)};
  };
  month_end_iterator MonthEndUpper(sec_t dt) const;
  month_end_iterator MonthEndLower(sec_t dt) const;
  dpair<month_end_iterator> MonthEndBetween(sec_t start, sec_t end) const
  {
    if (start > end)
      throw InvalidTimeOrder(start, end);
    return {MonthEndUpper(start), MonthEndLower(end)};
  };
  month_begin_iterator MonthBeginUpper(sec_t dt) const;
  month_begin_iterator MonthBeginLower(sec_t dt) const;
  dpair<month_begin_iterator> MonthBeginBetween(sec_t start, sec_t end) const
  {
    if (start > end)
      throw InvalidTimeOrder(start, end);
    return {MonthBeginUpper(start), MonthBeginLower(end)};
  };
  week_end_iterator WeekEndUpper(sec_t dt) const;
  week_end_iterator WeekEndLower(sec_t dt) const;
  dpair<week_end_iterator> WeekEndBetween(sec_t start, sec_t end) const
  {
    if (start > end)
      throw InvalidTimeOrder(start, end);
    return {WeekEndUpper(start), WeekEndLower(end)};
  };
  week_begin_iterator WeekBeginUpper(sec_t dt) const;
  week_begin_iterator WeekBeginLower(sec_t dt) const;
  dpair<week_begin_iterator> WeekBeginBetween(sec_t start, sec_t end) const
  {
    if (start > end)
      throw InvalidTimeOrder(start, end);
    return {WeekBeginUpper(start), WeekBeginLower(end)};
  };
  weekday_iterator WeekDayUpper(sec_t dt, int weekday) const;
  weekday_iterator WeekDayLower(sec_t dt, int weekday) const;
  dpair<weekday_iterator> WeekDayBetween(sec_t start, sec_t end, int weekday) const
  {
    if (start > end)
      throw InvalidTimeOrder(start, end);
    return {WeekDayUpper(start, weekday), WeekDayLower(end, weekday)};
  };
  const DateNode &At(sec_t dt) const { return calendar_data_.at(dt); };
  const_map_iterator cbegin() const { return calendar_data_.cbegin(); }
  const_map_iterator cend() const { return calendar_data_.cend(); }

private:
  calendar_data_map calendar_data_;
  const_map_iterator end_;
  const_map_iterator rend_;

  template <typename Iter, typename... Args>
  Iter SafeFind(sec_t dt, Args... args) const
  {
    if (calendar_data_.empty())
      throw CalendarNotInit();
    return Iter{calendar_data_.find(dt), this, std::forward<Args>(args)...};
  }
};

int num_days_month(int year, int month);

struct AllDayTradingNode
{
  AllDayTradingNode(sec_t ts) : dt_(datetime::precision_type(ts)), cdate_(dt_.date.isocalendar()) {}
  datetime dt_;
  calendar_date cdate_;
  bool IsTrading() const { return true; };
  bool IsMonthBegin() const { return dt_.date.day == 1; };
  bool IsMonthEnd() const { return dt_.date.day == num_days_month(dt_.date.year, dt_.date.mon); };
  bool IsWeekBegin() const { return cdate_.weekday == 1; };
  bool IsWeekEnd() const { return cdate_.weekday == 7; };
  bool IsWeekDay(int weekday) const { return cdate_.weekday == weekday; };
};

class Date7x24Array
{
public:
  class iterator
  {
  public:
    // iterator traits
    using difference_type = long;
    using value_type = std::pair<sec_t, AllDayTradingNode>;
    using pointer = value_type *;
    using reference = value_type &;
    using iterator_category = std::bidirectional_iterator_tag;
    iterator() : current_{-1, AllDayTradingNode(0)} {} // for end iterator
    iterator(sec_t dt) : current_{dt, AllDayTradingNode(dt)} {}
    iterator(const iterator &) = default;
    iterator &operator=(const iterator &iter) = default;
    bool operator==(const iterator &other) const { return current_.first == other.current_.first; }
    bool operator!=(const iterator &other) const { return current_.first != other.current_.first; }
    bool operator>(const iterator &other) const { return current_.first > other.current_.first; }
    bool operator>=(const iterator &other) const { return current_.first >= other.current_.first; }
    bool operator<(const iterator &other) const { return current_.first < other.current_.first; }
    bool operator<=(const iterator &other) const { return current_.first <= other.current_.first; }
    virtual iterator &operator++() = 0;
    virtual iterator &operator--() = 0;
    const value_type &operator*() const { return current_; }
    const value_type *operator->() const { return &current_; }
    bool is_end() const { return current_.first == -1; }

    friend Date7x24Array;

  protected:
    value_type current_;
  };

  class tradedays_iterator : public iterator
  {
  public:
    using iterator::iterator;
    tradedays_iterator &operator++() final override;
    tradedays_iterator &operator--() final override;
  };

  class month_begin_iterator : public iterator
  {
  public:
    using iterator::iterator;
    month_begin_iterator &operator++() final override;
    month_begin_iterator &operator--() final override;
  };

  class month_end_iterator : public iterator
  {
  public:
    using iterator::iterator;
    month_end_iterator &operator++() final override;
    month_end_iterator &operator--() final override;
  };

  class weekday_iterator : public iterator
  {
  public:
    weekday_iterator() : iterator() {}
    weekday_iterator(const weekday_iterator &) = default;
    weekday_iterator &operator=(const weekday_iterator &iter) = default;
    weekday_iterator(sec_t dt, int weekday) : iterator(dt), weekday_(weekday) {}
    weekday_iterator &operator++() override;
    weekday_iterator &operator--() override;

  private:
    int weekday_;
  };
  using week_begin_iterator = weekday_iterator;
  using week_end_iterator = weekday_iterator;

  tradedays_iterator Upper(sec_t dt) const { return SafeFind<Date7x24Array::tradedays_iterator>(dt); }
  tradedays_iterator Lower(sec_t dt) const { return Upper(dt); };
  dpair<tradedays_iterator> Between(sec_t start, sec_t end) const
  {
    if (start > end)
      throw InvalidTimeOrder(start, end);
    return {Upper(start), Lower(end)};
  };
  month_end_iterator MonthEndUpper(sec_t dt) const;
  month_end_iterator MonthEndLower(sec_t dt) const;
  dpair<month_end_iterator> MonthEndBetween(sec_t start, sec_t end) const
  {
    if (start > end)
      throw InvalidTimeOrder(start, end);
    return {MonthEndUpper(start), MonthEndLower(end)};
  };
  month_begin_iterator MonthBeginUpper(sec_t dt) const;
  month_begin_iterator MonthBeginLower(sec_t dt) const;
  dpair<month_begin_iterator> MonthBeginBetween(sec_t start, sec_t end) const
  {
    if (start > end)
      throw InvalidTimeOrder(start, end);
    return {MonthBeginUpper(start), MonthBeginLower(end)};
  };
  week_end_iterator WeekEndUpper(sec_t dt) const { return WeekDayUpper(dt, 7); }
  week_end_iterator WeekEndLower(sec_t dt) const { return WeekDayLower(dt, 7); }
  dpair<week_end_iterator> WeekEndBetween(sec_t start, sec_t end) const
  {
    if (start > end)
      throw InvalidTimeOrder(start, end);
    return {WeekEndUpper(start), WeekEndLower(end)};
  };
  week_begin_iterator WeekBeginUpper(sec_t dt) const { return WeekDayUpper(dt, 1); }
  week_begin_iterator WeekBeginLower(sec_t dt) const { return WeekDayLower(dt, 1); }
  dpair<week_begin_iterator> WeekBeginBetween(sec_t start, sec_t end) const
  {
    if (start > end)
      throw InvalidTimeOrder(start, end);
    return {WeekBeginUpper(start), WeekBeginLower(end)};
  };
  weekday_iterator WeekDayUpper(sec_t dt, int weekday) const;
  weekday_iterator WeekDayLower(sec_t dt, int weekday) const;
  dpair<weekday_iterator> WeekDayBetween(sec_t start, sec_t end, int weekday) const
  {
    if (start > end)
      throw InvalidTimeOrder(start, end);
    return {WeekDayUpper(start, weekday), WeekDayLower(end, weekday)};
  };

private:
  template <typename Iter, typename... Args>
  Iter SafeFind(sec_t dt, Args... args) const
  {
    if (!is_daily(dt))
      return Iter();
    return Iter{dt, std::forward<Args>(args)...};
  }
};

NS_QMC_END