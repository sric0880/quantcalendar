#pragma once
#include <ctime>
#include <stdexcept>
#include <string>
#include <vector>
#include <assert.h>
#include <ratio>
#include <optional>
#include "fmt/format.h"

#include "quantcalendar/qmc_globals.h"
#include "quantdata/datetime.h"
#include "quantcalendar/calendar_data.h"
#include "ankerl/unordered_dense.h"

NS_QMC_BEGIN

using session_t = std::pair<sec_t, sec_t>;
using time_point = system_clock::time_point;
using days = duration<int, std::ratio_multiply<std::ratio<24>, hours::period>>;

constexpr seconds UnitToDuration(char u)
{
  switch (u)
  {
  case 's':
    return 1s;
  case 'm':
    return 1min;
  case 'h':
    return 1h;
  case 'd':
    return 1_d;
  case 'w':
    return 1_w;
  case 'M':
    return 1_m;
  default:
    throw std::invalid_argument(fmt::format("interval unit {} is not invalid", u));
  }
}

// 特殊原因提前收盘或者延迟开盘
struct SpecialSessions
{
  std::string name;
  session_t open_close_sessions;
  session_t ordered_sessions;
};

inline bool is_daily(const time_point &tp)
{
  return (tp - time_point_cast<days>(tp)) == time_point::duration::zero();
}

inline sec_t to_daily(const time_point &tp)
{
  return duration_cast<seconds>(time_point_cast<days>(tp).time_since_epoch()).count();
}

inline sec_t to_daily(const datetime& dt)
{
  return duration_cast<seconds>(duration_cast<days>(dt.to_duration())).count();
}

/**
 * 交易日历
 */
class Calendar
{
public:
  virtual ~Calendar() {}
  using tradedays_iterator = CalendarData::tradedays_iterator;
  using month_begin_iterator = CalendarData::month_begin_iterator;
  using month_end_iterator = CalendarData::month_end_iterator;
  using week_begin_iterator = CalendarData::week_begin_iterator;
  using week_end_iterator = CalendarData::week_end_iterator;
  using weekday_iterator = CalendarData::weekday_iterator;
  tradedays_iterator TradedaysUpper(sec_t dt) const { return GetData().TradedaysUpper(dt); }
  tradedays_iterator TradedaysLower(sec_t dt) const { return GetData().TradedaysLower(dt); }
  CalendarData::iter_range<tradedays_iterator> TradedaysBetween(sec_t start, sec_t end) const { return GetData().TradedaysBetween(start, end); }
  month_end_iterator MonthEndUpper(sec_t dt) const { return GetData().MonthEndUpper(dt); }
  month_end_iterator MonthEndLower(sec_t dt) const { return GetData().MonthEndLower(dt); }
  CalendarData::iter_range<month_end_iterator> MonthEndBetween(sec_t start, sec_t end) const { return GetData().MonthEndBetween(start, end); }
  month_begin_iterator MonthBeginUpper(sec_t dt) const { return GetData().MonthBeginUpper(dt); }
  month_begin_iterator MonthBeginLower(sec_t dt) const { return GetData().MonthBeginLower(dt); }
  CalendarData::iter_range<month_begin_iterator> MonthBeginBetween(sec_t start, sec_t end) const { return GetData().MonthBeginBetween(start, end); }
  week_end_iterator WeekEndUpper(sec_t dt) const { return GetData().WeekEndUpper(dt); }
  week_end_iterator WeekEndLower(sec_t dt) const { return GetData().WeekEndLower(dt); }
  CalendarData::iter_range<week_end_iterator> WeekEndBetween(sec_t start, sec_t end) const { return GetData().WeekEndBetween(start, end); }
  week_begin_iterator WeekBeginUpper(sec_t dt) const { return GetData().WeekBeginUpper(dt); }
  week_begin_iterator WeekBeginLower(sec_t dt) const { return GetData().WeekBeginLower(dt); }
  CalendarData::iter_range<week_begin_iterator> WeekBeginBetween(sec_t start, sec_t end) const { return GetData().WeekBeginBetween(start, end); }
  weekday_iterator WeekDayUpper(sec_t dt, int weekday) const { return GetData().WeekDayUpper(dt, weekday); }
  weekday_iterator WeekDayLower(sec_t dt, int weekday) const { return GetData().WeekDayLower(dt, weekday); }
  CalendarData::iter_range<weekday_iterator> WeekDayBetween(sec_t start, sec_t end, int weekday) const { return GetData().WeekDayBetween(start, end, weekday); }
  /**
   * 获取某段时间内所有的K线时间，含start，不含end
   * @param interval(seconds): K线间隔周期
   * @param start: 开始时间
   * @param end: 结束时间
   */
  std::vector<sec_t> GetBartimes(seconds interval, time_point start, time_point end);
  /**
   * 获取某段时间内所有的K线时间，含start
   * @param interval(seconds): K线间隔周期
   * @param start: 开始时间
   * @param count: K线数量
   */
  std::vector<sec_t> GetBartimes(seconds interval, time_point start, int count);
  /**
   * 获取K线时间
   * @param dt: 当前时间
   * @param interval(seconds): K线间隔周期
   */
  inline sec_t GetCurrentBartime(time_point dt, seconds interval);

  // 从配置special_sessions中读取，或者重写该函数
  const SpecialSessions &GetSpecialSessions(time_point dt) const;
  /// @brief 给定时间`dt`, 获取下一次(开盘, 收盘)时间。休息时间不算是收盘，每天只有一次开盘收盘时间。
  /// @param dt 当前时间
  /// @return pair(开盘, 收盘)时间
  const session_t GetOpenCloseDT(sec_t dt) const;
  /// @brief 给定时间`dt`, 获取下一次(开盘, 收盘)。休息时间段也算是收盘
  /// @param dt 当前时间
  /// @return pair(开盘, 收盘)时间
  const session_t GetSessionDT(sec_t dt) const;
  // 返回交易时间段
  const std::vector<session_t> &GetSessions() const
  {
    return sessions_;
  }
  // 返回交易时间段(按开盘时间从小到大排序)
  const void GetOrderedSessions() const;
  // 返回开盘收盘时间
  const void GetOpenCloseTime() const;
  // 判断时间`dt`是否正在交易中, `dt`时间必须是交易所本地时间
  bool IsTrading(time_point dt) const;
  // 判断是否交易日
  bool IsTradingDay(time_point dt) const;
  // 判断是否交易时间段，不判断是否交易，只要在时间段内，都返回True
  bool IsTradingTime(time_point dt) const;

protected:
  /// @brief 
  /// @param sessions 开盘-收盘时间(包括中间的休息时间), 按当天秒数来算 eg. ((32400, 36900), (37800, 41400), (48600, 54000))
  /// @param intervals 支持的K线周期间隔,单位s,只支持分钟和小时 eg. 1min, 5min, 10min 1h 2h...
  /// @param bartime_right K线时间是按`right` 结束时间 或者`left` 开始时间表示，默认结束时间 @todo:  `left`暂未实现
  Calendar(std::vector<session_t> &&sessions,
           std::vector<seconds> &&intervals,
           bool bartime_right = true);


  virtual const CalendarData &GetData() const = 0;
  // 从配置special_sessions中读取，或者重写该函数
  virtual const std::optional<SpecialSessions> GetSpecialSessions(sec_t dt);

private:
  bool bartime_right_;

  const std::vector<int> intervals_;

  ankerl::unordered_dense::map<int, std::vector<int>> bartimes_;

  const std::vector<session_t> sessions_;
  const std::vector<session_t> sorted_sessions_;

  // 本来一天只有一次开盘收盘时间，但是为了兼容特殊日子，开收盘时间依然用vector表示
  std::vector<session_t> open_close_sessions_;

  // 特殊原因提前收盘或者延迟开盘
  ankerl::unordered_dense::map<sec_t, SpecialSessions> special_sessions_;

  void CalcBartimes();
  void GetDailyBartimes(CalendarData::iterator&& it, int count, int offset, std::vector<sec_t>& ret);
  std::pair<time_point, time_point> ApplyOffset(time_point dt);
  std::vector<session_t>& GetSessionsWithBreaks(sec_t dt);
  sec_t CombineDatetime(sec_t tradingday, sec_t time);
  sec_t CombineDatetimeSos(sec_t tradingday, sec_t time);
};

class CalendarAstock : public Calendar
{
public:
  static void InitData(const std::vector<std::tuple<sec_t /*timestamp*/, uint8_t /*status*/>> &data)
  {
    calendar_data.InitData(data);
  }

  static const CalendarAstock &GetInstance(std::string_view symbol = "") { return cal; }

protected:
  const CalendarData &GetData() const override
  {
    return calendar_data;
  }

private:
  static CalendarData calendar_data;
  static CalendarAstock cal;
};

class CalendarCTP : public Calendar
{
};

NS_QMC_END