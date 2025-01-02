#pragma once
#include <ctime>
#include <stdexcept>
#include <regex>
#include <string>
#include <assert.h>
#include "fmt/format.h"

#include "quantcalendar/qmc_globals.h"
#include "quantdata/datetime.h"
#include "quantcalendar/calendar_data.h"
#include "ankerl/unordered_dense.h"

NS_QMC_BEGIN

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
    return 1_M;
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
  // get trade days >= dt
  // virtual c_iter GetTradedaysGTE(sec_t dt) const
  // {
  //   return GetData().GetTradedaysGTE(dt);
  // }
  // // get trade days <= dt
  // virtual cr_iter GetTradedaysLTE(sec_t dt) const
  // {
  //   return GetData().GetTradedaysLTE(dt);
  // }

  // const datetime *GetTradedayNext(const datetime &dt) const
  // {
  //   return GetTradedayNext(dt.to_timestamp());
  // }
  // // equal to GetTradedaysGTE(dt)[0]
  // virtual const datetime *GetTradedayNext(sec_t dt) const
  // {
  //   return GetData().GetTradedayNext(dt);
  // }

  // // equal to GetTradedaysLTE(dt)[-1]
  // const datetime *GetTradedayLast(const datetime &dt) const
  // {
  //   return GetTradedayLast(dt.to_timestamp());
  // }
  // virtual const datetime *GetTradedayLast(sec_t dt) const
  // {
  //   return GetData().GetTradedayLast(dt);
  // }

  // date_range GetTradedaysBetween(const datetime &start_dt, const datetime &end_dt) const
  // {
  //   return GetTradedaysBetween(start_dt.to_timestamp(), end_dt.to_timestamp());
  // }
  // virtual date_range GetTradedaysBetween(sec_t start_dt, sec_t end_dt) const
  // {
  //   return GetData().GetTradedaysBetween(start_dt, end_dt);
  // }
  // /**
  //  * @return
  //  * all month ends >=`start`
  //  */
  // virtual const c_iter GetTradedaysMonthEnd(sec_t start) const
  // {
  // }
  // const c_iter GetTradedaysMonthEnd(const datetime &start) const
  // {
  // }
  // /**
  //  * @return
  //  * `count` month ends >=`start`
  //  */
  // virtual const date_range GetTradedaysMonthEnd(sec_t start, int count) const;
  // const date_range GetTradedaysMonthEnd(const datetime &start, int count) const;
  // /**
  //  * @return
  //  * return all `end` >= monthends >=`start`
  //  */
  // virtual const date_range GetTradedaysMonthEnd(sec_t start, sec_t end) const;
  // const date_range GetTradedaysMonthEnd(const datetime &start, const datetime &end) const;
  // /**
  //  * @return
  //  * all month begins >=`start`
  //  */
  // virtual const c_iter GetTradedaysMonthBegin(sec_t start) const;
  // const c_iter GetTradedaysMonthBegin(const datetime &start) const;
  // /**
  //  * @return
  //  * `count` month begins >=`start`
  //  */
  // virtual const date_range GetTradedaysMonthBegin(sec_t start, int count) const;
  // const date_range GetTradedaysMonthBegin(const datetime &start, int count) const;
  // /**
  //  * @return
  //  * return all `end` >= monthbegins >=`start`
  //  */
  // virtual const date_range GetTradedaysMonthBegin(sec_t start, sec_t end) const;
  // const date_range GetTradedaysMonthBegin(const datetime &start, const datetime &end) const;
  // /**
  //  * @return
  //  * all week ends >=`start`
  //  */
  // virtual const c_iter GetTradedaysWeekEnd(sec_t start) const;
  // const c_iter GetTradedaysWeekEnd(const datetime &start) const;
  // /**
  //  * @return
  //  * `count` week ends >=`start`
  //  */
  // virtual const date_range GetTradedaysWeekEnd(sec_t start, int count) const;
  // const date_range GetTradedaysWeekEnd(const datetime &start, int count) const;
  // /**
  //  * @return
  //  * return all `end` >= weekends >=`start`
  //  */
  // virtual const date_range GetTradedaysWeekEnd(sec_t start, sec_t end) const;
  // const date_range GetTradedaysWeekEnd(const datetime &start, const datetime &end) const;
  // /**
  //  * @return
  //  * all week begins >=`start`
  //  */
  // virtual const c_iter GetTradedaysWeekBegin(sec_t start) const;
  // const c_iter GetTradedaysWeekBegin(const datetime &start) const;
  // /**
  //  * @return
  //  * `count` week begins >=`start`
  //  */
  // virtual const date_range GetTradedaysWeekBegin(sec_t start, int count) const;
  // const date_range GetTradedaysWeekBegin(const datetime &start, int count) const;
  // /**
  //  * @return
  //  * return all `end` >= weekbegins >=`start`
  //  */
  // virtual const date_range GetTradedaysWeekBegin(sec_t start, sec_t end) const;
  // const date_range GetTradedaysWeekBegin(const datetime &start, const datetime &end) const;
  // /**
  //  * @return
  //  * return all trading `weekday` >=`start`, , weekday in [1, 7]
  //  */
  // virtual const c_iter GetTradedaysWeekDay(sec_t start) const;
  // const c_iter GetTradedaysWeekDay(const datetime &start) const;
  // /**
  //  * @return
  //  * return `count` trading `weekday` >=`start`, weekday in [1, 7]
  //  */
  // virtual const date_range GetTradedaysWeekDay(sec_t start, int count) const;
  // const date_range GetTradedaysWeekDay(const datetime &start, int count) const;
  // /**
  //  * @return
  //  * return all `end` >= trading `weekday` >=`start`, weekday in [1, 7]
  //  */
  // virtual const date_range GetTradedaysWeekDay(sec_t start, sec_t end) const;
  // const date_range GetTradedaysWeekDay(const datetime &start, const datetime &end) const;
  /**
   * 获取K线时间
   * @param
   *  interval(seconds): K线间隔周期
   *  start: 开始时间
   *  end: 结束时间
   */
  std::vector<sec_t> GetBartimes(seconds interval, time_point start, time_point end);
  /**
   * 获取K线时间
   * @param
   *  interval(seconds): K线间隔周期
   *  count: K线数量
   */
  std::vector<sec_t> GetBartimes(seconds interval, time_point start, int count);
  /**
   * 获取K线时间
   * @param
   *  interval(seconds): K线间隔周期
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
  Calendar() {};
  // void CalcBarTimestamp();
  // void _calc_bartimestamp_left();
  // void _calc_bartimestamp_right();

  // 开盘-收盘时间(包括中间的休息时间), 按当天秒数来算 eg. ((32400, 36900), (37800, 41400), (48600, 54000))
  std::vector<session_t> sessions_;
  // 特殊原因提前收盘或者延迟开盘
  ankerl::unordered_dense::map<sec_t, SpecialSessions> special_sessions_;
  // 支持的K线周期间隔,单位s,只支持分钟和小时 eg. (60, 300, 600) 表示 1min, 5min, 10min 的K线时间
  std::array<seconds, 0> intervals_;
  // K线时间是按`right` 结束时间 或者`left` 开始时间表示，默认结束时间
  //  @todo:  `left`暂未实现
  bool bartime_right_ = true;

  virtual const CalendarData &GetData() const = 0;

private:
};

class CalendarAstock : public Calendar
{
public:
  static void InitData(const std::vector<std::tuple<sec_t /*timestamp*/, uint8_t /*status*/>> &calendar_data)
  {
    _calendar_data.InitData(calendar_data);
  }
  static const CalendarAstock &Get(std::string_view symbol = "")
  {
    static CalendarAstock cal;
    return cal;
  }

protected:
  std::vector<session_t> sessions_{{34200, 41400}, {46800, 54000}};
  const CalendarData &GetData() const override
  {
    return _calendar_data;
  }

private:
  static CalendarData _calendar_data;
};

class CalendarCTP : public Calendar
{
};

NS_QMC_END