#pragma once
#include <ctime>
#include <memory>
#include <string>
#include <vector>
#include <ratio>
#include <functional> // for reference_wrapper

#include "quantdata/datetime.h"
#include "quantcalendar/calendar_data.h"
#include "ankerl/unordered_dense.h"

NS_QMC_BEGIN

using session_t = std::pair<sec_t, sec_t>;
using time_point = system_clock::time_point;
using days = duration<int, std::ratio_multiply<std::ratio<24>, hours::period>>;

// 特殊原因提前收盘或者延迟开盘
struct SpecialSessions
{
  SpecialSessions(const std::vector<session_t> &a, const std::vector<session_t> &b) : open_close_sessions(a), ordered_sessions(b) {}
  const std::vector<session_t> open_close_sessions;
  const std::vector<session_t> ordered_sessions;
};

inline bool is_daily(const time_point &tp)
{
  return (tp - time_point_cast<days>(tp)) == time_point::duration::zero();
}

inline sec_t to_daily(const time_point &tp)
{
  return duration_cast<seconds>(time_point_cast<days>(tp).time_since_epoch()).count();
}

inline system_clock::duration to_time(const time_point &tp)
{
  return tp - time_point_cast<days>(tp);
}

inline sec_t to_daily(const datetime &dt)
{
  return duration_cast<seconds>(duration_cast<days>(dt.to_duration())).count();
}

inline time_point to_time_point(double ts)
{
  return time_point(time_point::duration(static_cast<typename time_point::rep>(ts * time_point::period::den)));
}

/**
 * 交易日历
 */
class Calendar
{
public:
  Calendar(const Calendar &) = delete;
  Calendar &operator=(const Calendar &) = delete;
  Calendar(Calendar &&rhs) = default;
  Calendar &operator=(Calendar &&rhs) = default;
  using tradedays_iterator = CalendarData::tradedays_iterator;
  using month_begin_iterator = CalendarData::month_begin_iterator;
  using month_end_iterator = CalendarData::month_end_iterator;
  using week_begin_iterator = CalendarData::week_begin_iterator;
  using week_end_iterator = CalendarData::week_end_iterator;
  using weekday_iterator = CalendarData::weekday_iterator;
  tradedays_iterator TradedaysUpper(sec_t dt) const { return data_.get().TradedaysUpper(dt); }
  tradedays_iterator TradedaysLower(sec_t dt) const { return data_.get().TradedaysLower(dt); }
  CalendarData::iter_range<tradedays_iterator> TradedaysBetween(sec_t start, sec_t end) const { return data_.get().TradedaysBetween(start, end); }
  month_end_iterator MonthEndUpper(sec_t dt) const { return data_.get().MonthEndUpper(dt); }
  month_end_iterator MonthEndLower(sec_t dt) const { return data_.get().MonthEndLower(dt); }
  CalendarData::iter_range<month_end_iterator> MonthEndBetween(sec_t start, sec_t end) const { return data_.get().MonthEndBetween(start, end); }
  month_begin_iterator MonthBeginUpper(sec_t dt) const { return data_.get().MonthBeginUpper(dt); }
  month_begin_iterator MonthBeginLower(sec_t dt) const { return data_.get().MonthBeginLower(dt); }
  CalendarData::iter_range<month_begin_iterator> MonthBeginBetween(sec_t start, sec_t end) const { return data_.get().MonthBeginBetween(start, end); }
  week_end_iterator WeekEndUpper(sec_t dt) const { return data_.get().WeekEndUpper(dt); }
  week_end_iterator WeekEndLower(sec_t dt) const { return data_.get().WeekEndLower(dt); }
  CalendarData::iter_range<week_end_iterator> WeekEndBetween(sec_t start, sec_t end) const { return data_.get().WeekEndBetween(start, end); }
  week_begin_iterator WeekBeginUpper(sec_t dt) const { return data_.get().WeekBeginUpper(dt); }
  week_begin_iterator WeekBeginLower(sec_t dt) const { return data_.get().WeekBeginLower(dt); }
  CalendarData::iter_range<week_begin_iterator> WeekBeginBetween(sec_t start, sec_t end) const { return data_.get().WeekBeginBetween(start, end); }
  weekday_iterator WeekDayUpper(sec_t dt, int weekday) const { return data_.get().WeekDayUpper(dt, weekday); }
  weekday_iterator WeekDayLower(sec_t dt, int weekday) const { return data_.get().WeekDayLower(dt, weekday); }
  CalendarData::iter_range<weekday_iterator> WeekDayBetween(sec_t start, sec_t end, int weekday) const { return data_.get().WeekDayBetween(start, end, weekday); }
  void InitSpecialSessions(ankerl::unordered_dense::map<sec_t, std::shared_ptr<SpecialSessions>> &&sessions) noexcept;
  /**
   * 获取某段时间内所有的K线时间，含start，不含end
   * @param interval(seconds): K线间隔周期
   * @param start: 开始时间
   * @param end: 结束时间
   */
  std::vector<sec_t> GetBartimes(seconds interval, time_point start, time_point end) const
  {
    return GetBartimesImpl(interval, start, 0, end);
  }
  /**
   * 获取某段时间内所有的K线时间，含start
   * @param interval(seconds): K线间隔周期
   * @param start: 开始时间
   * @param count: K线数量
   */
  std::vector<sec_t> GetBartimes(seconds interval, time_point start, size_t count) const
  {
    return GetBartimesImpl(interval, start, count, time_point::min());
  }
  /**
   * 获取K线时间
   * @param dt: 当前时间
   * @param interval(seconds): K线间隔周期
   */
  sec_t GetCurrentBartime(time_point dt, seconds interval) const
  {
    return GetBartimesImpl(interval, dt, 1, time_point::min())[0];
  }

  /// @brief 给定时间`dt`, 获取下一次(开盘, 收盘)时间。休息时间不算是收盘，每天只有一次开盘收盘时间。
  /// @param dt 当前时间
  /// @return pair(开盘, 收盘)时间
  session_t GetNextOpenClose(time_point dt) const { return FindNextSession(dt, false); }

  /// @brief 给定时间`dt`, 获取下一次(开盘, 收盘)。休息时间段也算是收盘
  /// @param dt 当前时间
  /// @return pair(开盘, 收盘)时间
  session_t GetNextSession(time_point dt) const { return FindNextSession(dt, true); }
  // 返回交易时间段
  const std::vector<session_t> &GetSessions() const { return sessions_; }
  // 返回交易时间段(按开盘时间从小到大排序)
  const std::vector<session_t> &GetOrderedSessions() const { return sorted_sessions_; }
  // 返回开盘收盘时间
  const session_t &GetOpenCloseTime() const { return open_close_sessions_[0]; }
  // 判断时间`dt`是否正在交易中, `dt`时间必须是交易所本地时间
  bool IsTrading(time_point dt) const;
  // 判断是否交易日
  bool IsTradingDay(time_point dt) const;
  // 判断是否交易时间段，不判断是否交易，只要在时间段内，都返回True
  bool IsTradingTime(time_point dt) const;
  std::string ToString() const;

protected:
  /// @brief
  /// @param sessions 开盘-收盘时间(包括中间的休息时间), 按当天秒数来算 eg. ((32400, 36900), (37800, 41400), (48600, 54000))
  /// @param intervals 支持的K线周期间隔,单位s,只支持分钟和小时 eg. 1min, 5min, 10min 1h 2h...
  /// @param tz 时区
  /// @param offset 有些市场交易时间会跨越凌晨0点, offset表示超过0点的时间差, 越过0点表示下一个交易日
  /// @param bartime_right K线时间是按`right` 结束时间 或者`left` 开始时间表示，默认结束时间 @todo:  `left`暂未实现
  Calendar(const CalendarData &data,
           std::vector<session_t> &&sessions,
           const std::vector<seconds> &intervals,
           std::string_view tz,
           sec_t offset = 0,
           bool bartime_right = true);

private:
  std::reference_wrapper<const CalendarData> data_;
  std::vector<session_t> sessions_;
  std::vector<int> intervals_;
  std::string tz_;
  sec_t offset_;
  bool bartime_right_;
  sec_t offset_minus_day_;

  std::vector<session_t> sorted_sessions_;

  // 本来一天只有一次开盘收盘时间，但是为了兼容特殊日子，开收盘时间依然用vector表示
  std::vector<session_t> open_close_sessions_;

  // 特殊原因提前收盘或者延迟开盘
  ankerl::unordered_dense::map<sec_t, std::shared_ptr<SpecialSessions>> special_sessions_;
  ankerl::unordered_dense::map<int, std::vector<int>> bartimes_;

  void CalcBartimes();
  std::vector<sec_t> GetBartimesImpl(seconds interval, time_point start, size_t count, time_point end) const;
  void GenerateDailyBartimes(CalendarData::iterator &&it, time_point start_dt, size_t count, time_point end, std::vector<sec_t> &ret) const;
  void GenerateMinuteBartimes(CalendarData::iterator &&it, int interval, time_point start_dt, size_t count, time_point end, std::vector<sec_t> &ret) const;
  std::pair<time_point, time_point> ApplyOffset(time_point dt) const;
  const std::vector<session_t> &GetSessionsWithBreaks(sec_t dt) const;
  const std::vector<session_t> &GetSessionsWithoutBreaks(sec_t dt) const;
  sec_t CombineDatetime(sec_t tradingday, sec_t time) const;
  sec_t CombineDatetimeSos(sec_t tradingday, sec_t time) const;
  const std::shared_ptr<SpecialSessions> GetSpecialSessions(sec_t dt) const;
  session_t FindNextSession(time_point dt, bool with_breaks) const;
};

class CalendarAstock : public Calendar
{
public:
  static void InitData(const std::vector<calendar_item> &data)
  {
    calendar_data.InitData(data);
  }

  static const CalendarAstock &GetInstance(const std::string &symbol = "");

private:
  using Calendar::Calendar;
  static CalendarData calendar_data;
};

class CalendarCTP : public Calendar
{
public:
  using session_item = std::tuple<std::string /*product_id*/, std::vector<session_t> /*market time*/>;
  bool HasNight() const;
  static void InitData(const std::vector<calendar_item> &data, std::vector<session_item> &&sessions);
  static const CalendarCTP &GetInstance(const std::string &symbol = "");

private:
  using Calendar::Calendar;
  static CalendarData calendar_data;
  static ankerl::unordered_dense::map<std::string, CalendarCTP> calendar_ctps;
};

NS_QMC_END