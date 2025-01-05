#pragma once
#include <string>
#include <tuple>
#include <bitset>
#include "quantdata/datetime.h"

#include "quantcalendar/qmc_globals.h"
#include "quantcalendar/exceptions.h"
#include "ankerl/unordered_dense.h"

NS_QMC_BEGIN

using namespace std::literals::chrono_literals;
using datetime = Datetime<>;
using calendar_date = IsoCalendarDate;
using sec_t = seconds::rep;
using calendar_item = std::tuple<sec_t /*timestamp*/, char /*status*/>;

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
  return seconds(static_cast<sec_t>(__d * 30 * 7 * iseconds_a_day));
}

struct CalendarDataNode
{
  CalendarDataNode(sec_t ts, uint8_t status) : dt_(datetime::precision(ts)), cdate_(dt_.date.isocalendar()), status_(status)
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
  bool IsWeekDay(int weekday) const { return cdate_.weekday == weekday; };
  void SetTrading(bool on) const { state_[0] = on; }
  void SetMonthBegin(bool on) const { state_[1] = on; }
  void SetMonthEnd(bool on) const { state_[2] = on; }
  void SetWeekBegin(bool on) const { state_[3] = on; }
  void SetWeekEnd(bool on) const { state_[4] = on; }
};

class CalendarData
{
public:
  using calendar_data_map = ankerl::unordered_dense::map<sec_t, CalendarDataNode>;
  using const_map_iterator = calendar_data_map::value_container_type::const_iterator;
  class iterator
  {
  protected:
    const_map_iterator it_;
    const const_map_iterator &end_;
    const const_map_iterator &rend_;

  public:
    // iterator traits
    using difference_type = long;
    using value_type = const_map_iterator::value_type;
    using pointer = const_map_iterator::pointer;
    using reference = const_map_iterator::reference;
    using iterator_category = std::bidirectional_iterator_tag;
    iterator(const_map_iterator &&it, const const_map_iterator &end, const const_map_iterator &rend) : it_(std::move(it)), end_(end), rend_(rend) {}
    bool operator==(const iterator &other) const { return it_ == other.it_; }
    bool operator!=(const iterator &other) const { return it_ != other.it_; }
    virtual iterator &operator++() = 0;
    virtual iterator &operator--() = 0;
    reference operator*() const { return *it_; }
    bool is_end() const { return (it_ == end_) || (it_ == rend_); }
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
    weekday_iterator(const_map_iterator &&it, const const_map_iterator &end, const const_map_iterator &rend, int weekday) : iterator(std::move(it), end, rend), weekday_(weekday) {}
    weekday_iterator &operator++() final override;
    weekday_iterator &operator--() final override;

  private:
    int weekday_;
  };

  template <class Iter>
  using iter_range = std::pair<Iter, Iter>;

  void InitData(const std::vector<calendar_item> &calendar_data);
  tradedays_iterator TradedaysUpper(sec_t dt) const;
  tradedays_iterator TradedaysLower(sec_t dt) const;
  iter_range<tradedays_iterator> TradedaysBetween(sec_t start, sec_t end) const;
  month_end_iterator MonthEndUpper(sec_t dt) const;
  month_end_iterator MonthEndLower(sec_t dt) const;
  iter_range<month_end_iterator> MonthEndBetween(sec_t start, sec_t end) const;
  month_begin_iterator MonthBeginUpper(sec_t dt) const;
  month_begin_iterator MonthBeginLower(sec_t dt) const;
  iter_range<month_begin_iterator> MonthBeginBetween(sec_t start, sec_t end) const;
  week_end_iterator WeekEndUpper(sec_t dt) const;
  week_end_iterator WeekEndLower(sec_t dt) const;
  iter_range<week_end_iterator> WeekEndBetween(sec_t start, sec_t end) const;
  week_begin_iterator WeekBeginUpper(sec_t dt) const;
  week_begin_iterator WeekBeginLower(sec_t dt) const;
  iter_range<week_begin_iterator> WeekBeginBetween(sec_t start, sec_t end) const;
  weekday_iterator WeekDayUpper(sec_t dt, int weekday) const;
  weekday_iterator WeekDayLower(sec_t dt, int weekday) const;
  iter_range<weekday_iterator> WeekDayBetween(sec_t start, sec_t end, int weekday) const;
  const CalendarDataNode &At(sec_t dt) const;

private:
  calendar_data_map calendar_data_;
  const_map_iterator end_;
  const_map_iterator rend_;

  template <typename Iter, typename... Args>
  Iter SafeFind(sec_t dt, Args... args) const
  {
    const_map_iterator inner_it = calendar_data_.find(dt);
    if (inner_it == end_ || inner_it == rend_)
      throw OutOfCalendar();
    return Iter{std::move(inner_it), end_, rend_, std::forward<Args>(args)...};
  }
};

NS_QMC_END