#pragma once
#include <ctime>
#include <memory>
#include <string>
#include <vector>
#include <ratio>
#include <functional> // for reference_wrapper
#include <optional>
#include <variant>

#include "quantcalendar/datetime.h"
#include "quantcalendar/dates.h"
#include "ankerl/unordered_dense.h"

NS_QMC_BEGIN

using session_t = std::pair<sec_t, sec_t>;
// cannot use tuple because it cann't be exported to cython
struct CallAuctionSession {
  sec_t start_time;
  sec_t end_time;
  sec_t clearing_price_time;
};
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

enum class RangeClosed
{
  NONE,
  LEFT,
  RIGHT,
  BOTH
};

template <class T1, class T2 = seconds>
struct DurationGreater
{
  constexpr bool operator()(const T1 &lhs, const T2 &rhs) const
  {
    return lhs > rhs;
  }
};
template <class T1, class T2 = seconds>
struct DurationLess
{
  constexpr bool operator()(const T1 &lhs, const T2 &rhs) const
  {
    return lhs < rhs;
  }
};
template <class T1, class T2 = seconds>
struct DurationGreaterEqual
{
  constexpr bool operator()(const T1 &lhs, const T2 &rhs) const
  {
    return lhs >= rhs;
  }
};
template <class T1, class T2 = seconds>
struct DurationLessEqual
{
  constexpr bool operator()(const T1 &lhs, const T2 &rhs) const
  {
    return lhs <= rhs;
  }
};

/**
 * 交易日历
 */
template <class Data>
class Calendar
{
public:
  Calendar(const Calendar &) = delete;
  Calendar &operator=(const Calendar &) = delete;
  Calendar(Calendar &&rhs) = default;
  Calendar &operator=(Calendar &&rhs) = default;
  const Data *tradedays;

  void InitSpecialSessions(ankerl::unordered_dense::map<sec_t, std::shared_ptr<SpecialSessions>> &&sessions) noexcept;
  /**
   * 获取某段时间内所有的K线时间，含start，不含end
   * @param interval(seconds): K线间隔周期
   * @param start: 开始时间
   * @param end: 结束时间
   */
  std::vector<sec_t> GetBartimes(seconds interval, time_point start, time_point end) const
  {
    if (start > end)
      throw InvalidTimeOrder(start.time_since_epoch().count(), end.time_since_epoch().count());
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
   * 获取一日内的所有K线时间点 time of day
   */
  std::vector<int> GetBartimes(seconds interval) const
  {
    return GetBartimes(interval.count());
  }

  /**
   * 获取一日内的所有K线时间点 time of day
   */
  std::vector<int> GetBartimes(int interval) const
  {
    auto iter = bartimes_.find(interval);
    if (iter == bartimes_.end())
    {
      return std::vector<int>();
    }
    return iter->second;
  }

  const auto &GetBartimes() const
  {
    return bartimes_;
  }
  /**
   * 获取K线时间
   * @param dt: 当前时间
   * @param interval(seconds): K线间隔周期
   */
  sec_t GetCurrentBartime(seconds interval, time_point dt) const
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
  const std::vector<int> &GetIntervals() const { return intervals_; }
  const std::string &GetTimezone() const { return tz_; }
  // 判断时间`dt`是否正在交易中（不包括开盘集合竞价，包括收盘集合竞价）, `dt`时间必须是交易所本地时间
  bool IsTrading(time_point dt) const;
  // 判断是否交易日。如果IsTrading返回true，那么IsTradingDay必然返回true，反过来不一定成立。
  // 但是当IsTradingDay返回false，那么IsTrading必然返回false。
  // 比如中国期货白银，周六凌晨1点正在交易，此时IsTradingDay也为true，但是周六实际不是交易日。
  bool IsTradingDay(time_point dt) const;

  // 判断是否交易时间段，不判断是否交易，只要在时间段内，都返回True
  template <class Duration, class GreaterOrEqual = DurationGreaterEqual<Duration>, class LessOrEqual = DurationLessEqual<Duration>>
  bool IsTradingTime(Duration tm) const
  {
    if constexpr (std::is_same_v<Duration, time_point>)
    {
      return IsTradingTime(to_time(tm));
    }
    else
    {
      GreaterOrEqual left_cmp;
      LessOrEqual right_cmp;
      for (auto [start, end] : sorted_sessions_)
      {
        if (start < end)
        {
          if (left_cmp(tm, seconds(start)) && right_cmp(tm, seconds(end)))
            return true;
        }
        else
        {
          if (left_cmp(tm, seconds(start)) || right_cmp(tm, seconds(end)))
            return true;
        }
      }
      return false;
    }
  }

  // 判断是否交易时间段，不判断是否交易，只要在时间段内，都返回True
  template <class Duration>
  bool IsTradingTime(Duration tm, RangeClosed side) const
  {
    if constexpr (std::is_same_v<Duration, time_point>)
    {
      return IsTradingTime(to_time(tm), side);
    }
    else
    {
      switch (side)
      {
      case RangeClosed::NONE:
        return IsTradingTime<Duration, DurationGreater<Duration>, DurationLess<Duration>>(tm);
      case RangeClosed::LEFT:
        return IsTradingTime<Duration, DurationGreaterEqual<Duration>, DurationLess<Duration>>(tm);
      case RangeClosed::RIGHT:
        return IsTradingTime<Duration, DurationGreater<Duration>, DurationLessEqual<Duration>>(tm);
      case RangeClosed::BOTH:
        return IsTradingTime<Duration, DurationGreaterEqual<Duration>, DurationLessEqual<Duration>>(tm);
      default:
        return false;
      }
    }
  }

  /// @brief 给定时间`dt`, 获取当前或下一次(开始, 结束, 定价)开盘集合竞价时间。
  /// @param dt 当前时间
  /// @return tuple(开盘, 收盘, 定价)时间
  std::optional<CallAuctionSession> GetNextOCASession(time_point dt) const { return FindNextCASession(dt, true); }
  /// @brief 给定时间`dt`, 获取当前下一次(开始, 结束, 定价)收盘集合竞价时间
  /// @param dt 当前时间
  /// @return tuple(开盘, 收盘, 定价)时间
  std::optional<CallAuctionSession> GetNextCCASession(time_point dt) const { return FindNextCASession(dt, false); }
  // 返回开盘集合竞价时间段(相对)
  const std::vector<CallAuctionSession> &GetOCASessions() const { return opening_call_auctions_; }
  // 返回收盘集合竞价时间段(相对)
  const std::vector<CallAuctionSession> &GetCCASessions() const { return closing_call_auctions_; }
  // 是否开盘集合竞价
  bool IsOpeningCallAuction(time_point dt, sec_t start_offset = 0, sec_t end_offset = 0) const
  {
    auto s = GetNextOCASession(dt);
    return s.has_value() && dt >= time_point(seconds(s.value().start_time + start_offset)) &&
           dt <= time_point(seconds(s.value().end_time + end_offset));
  }
  // 是否收盘集合竞价
  bool IsClosingCallAuction(time_point dt, sec_t start_offset = 0, sec_t end_offset = 0) const
  {
    auto s = GetNextCCASession(dt);
    return s.has_value() && dt >= time_point(seconds(s.value().start_time + start_offset)) &&
           dt <= time_point(seconds(s.value().end_time + end_offset));
  }
  // 是否集合竞价(从开始时间一直到收盘/开盘价产生)
  bool IsCallAuction(time_point dt) const { return IsOpeningCallAuction(dt) || IsClosingCallAuction(dt); };
  // 是否连续竞价时间
  bool IsContinuousAuction(time_point dt) const { return !IsCallAuction(dt) && IsTrading(dt); };
  // 是否接受订单申报，默认为集合竞价和连续竞价阶段
  bool IsSubmitOrderAllowed(time_point dt) const { return IsTrading(dt) || IsCallAuction(dt); }
  // 是否接受订单撤销，默认集合竞价都不能撤单
  bool IsCancelOrderAllowed(time_point dt) const { return IsContinuousAuction(dt); }

protected:
  /// @brief
  /// @param sessions 开盘-收盘时间(包括中间的休息时间), 按当天秒数来算 eg. ((32400, 36900), (37800, 41400), (48600, 54000))
  /// @param opening_ca_sessions 开盘集合竞价时间
  /// @param closing_ca_sessions 收盘集合竞价时间
  /// @param intervals 支持的K线周期间隔,单位s,只支持分钟和小时 eg. 1min, 5min, 10min 1h 2h...
  /// @param tz 时区
  /// @param offset 有些市场交易时间会跨越凌晨0点, offset表示超过0点的时间差, 越过0点表示下一个交易日
  /// @param bartime_right K线时间是按`right` 结束时间 或者`left` 开始时间表示，默认结束时间 @todo:  `left`暂未实现
  Calendar(const Data &dates_container,
           const std::vector<session_t> &sessions,
           const std::vector<CallAuctionSession> &opening_ca_sessions,
           const std::vector<CallAuctionSession> &closing_ca_sessions,
           const std::vector<seconds> &intervals,
           std::string_view tz,
           sec_t offset = 0,
           bool bartime_right = true);

private:
  std::vector<session_t> sessions_;
  std::vector<int> intervals_;
  std::string tz_;
  sec_t offset_;
  bool bartime_right_;
  sec_t offset_minus_day_;

  std::vector<session_t> sorted_sessions_;
  // 一天可能有多次开盘集合竞价
  std::vector<CallAuctionSession> opening_call_auctions_;
  // 一般只有一次尾盘集合竞价
  std::vector<CallAuctionSession> closing_call_auctions_;

  // 本来一天只有一次开盘收盘时间，但是为了兼容特殊日子，开收盘时间依然用vector表示
  std::vector<session_t> open_close_sessions_;

  // 特殊原因提前收盘或者延迟开盘
  ankerl::unordered_dense::map<sec_t, std::shared_ptr<SpecialSessions>> special_sessions_;
  ankerl::unordered_dense::map<int, std::vector<int>> bartimes_;

  void CalcBartimes();
  std::vector<sec_t> GetBartimesImpl(seconds interval, time_point start, size_t count, time_point end) const;
  void GenerateDailyBartimes(typename Data::iterator &&it, time_point start_dt, size_t count, time_point end, std::vector<sec_t> &ret) const;
  void GenerateMinuteBartimes(typename Data::iterator &&it, int interval, time_point start_dt, size_t count, time_point end, std::vector<sec_t> &ret) const;
  sec_t ToDaily(const time_point &applied_offset_dt) const;
  const std::vector<session_t> &GetSessionsWithBreaks(sec_t dt) const;
  const std::vector<session_t> &GetSessionsWithoutBreaks(sec_t dt) const;
  sec_t CombineDatetime(sec_t tradingday, sec_t time) const;
  sec_t CombineDatetimeSos(sec_t tradingday, sec_t time) const;
  const std::shared_ptr<SpecialSessions> GetSpecialSessions(sec_t dt) const;
  session_t FindNextSession(time_point dt, bool with_breaks) const;
  std::optional<CallAuctionSession> FindNextCASession(time_point dt, bool is_opening) const;
};

class CalendarAstock : public Calendar<DatesArray>
{
public:
  bool IsCancelOrderAllowed(time_point dt) const
  {
    // 集合竞价前5分钟可以撤单09:15--09:20
    return IsContinuousAuction(dt) || IsOpeningCallAuction(dt, 0, -300);
  }
  static void Init(const std::vector<date_status_item> &dates_arr)
  {
    dates_container.Init(dates_arr);
  }

  static const CalendarAstock &GetInstance(const std::string &symbol = "");

private:
  using Calendar<DatesArray>::Calendar;
  static DatesArray dates_container;
};

class CalendarCTP : public Calendar<DatesArray>
{
public:
  using session_item = std::tuple<std::string /*product_id*/, std::vector<session_t> /*market time*/>;
  bool HasNight() const;
  // TODO 需要确认期货收盘集合竞价是否可以撤单
  bool IsCancelOrderAllowed(time_point dt) const { return IsContinuousAuction(dt) || IsOpeningCallAuction(dt); }
  static void Init(const std::vector<date_status_item> &dates_arr, std::vector<session_item> &&sessions);
  static const CalendarCTP &GetInstance(const std::string &symbol = "");

private:
  using Calendar<DatesArray>::Calendar;
  static DatesArray dates_container;
  static ankerl::unordered_dense::map<std::string, CalendarCTP> calendar_ctps;
};

class Time7x24Calendar : public Calendar<Date7x24Array>
{
public:
  static const Time7x24Calendar &GetInstance(const std::string &symbol = "");

private:
  using Calendar<Date7x24Array>::Calendar;
  static Date7x24Array dates_container;
};

using CalendarVar = std::variant<const qmc::CalendarCTP *,
                                 const qmc::CalendarAstock *>;

NS_QMC_END