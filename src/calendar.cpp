#include <string>
#include <algorithm>

#include "quantcalendar/calendar.h"

NS_QMC_BEGIN

#pragma region Calendar
template <class Data>
Calendar<Data>::Calendar(
    const Data &dates_container,
    const std::vector<session_t> &sessions,
    const std::vector<CallAuctionSession> &opening_ca_sessions,
    const std::vector<CallAuctionSession> &closing_ca_sessions,
    const std::vector<seconds> &intervals,
    std::string_view tz,
    sec_t offset,
    bool bartime_right) : tradedays(&dates_container),
                          sessions_(sessions),
                          opening_call_auctions_(opening_ca_sessions),
                          closing_call_auctions_(closing_ca_sessions),
                          intervals_(intervals.size()),
                          tz_(tz),
                          offset_(offset),
                          bartime_right_(bartime_right)
{
  std::transform(intervals.begin(), intervals.end(), intervals_.begin(), [](const seconds &sec)
                 { return static_cast<int>(sec.count()); });
  open_close_sessions_.emplace_back(sessions_[0].first, sessions_[sessions_.size() - 1].second);
  sorted_sessions_ = sessions_;
  std::sort(sorted_sessions_.begin(), sorted_sessions_.end(), [](const session_t &x, const session_t &y)
            { return x.first < y.first; });
  CalcBartimes();
}

template <class Data>
inline sec_t Calendar<Data>::ToDaily(const time_point &applied_offset_dt) const
{
  // 凌晨时刻判断交易日属于前一天
  if (is_daily(applied_offset_dt))
    return to_daily(applied_offset_dt - seconds_a_day);
  return to_daily(applied_offset_dt);
}

template <class Data>
void Calendar<Data>::CalcBartimes()
{
  auto start = sessions_[0].first;
  std::vector<sec_t> times;
  for (auto [sos, eos] : sessions_)
  {
    bool is_open_time = sos == start;
    if (bartime_right_)
    {
      if (!is_open_time)
        ++sos;
      ++eos;
    }
    if (sos >= eos) // 跨天
    {
      for (sec_t i = sos; i < iseconds_a_day; ++i)
        times.push_back(i);
      for (sec_t i = 0; i < eos; ++i)
        times.push_back(i);
    }
    else
    {
      for (sec_t i = sos; i < eos; ++i)
        times.push_back(i);
    }
  }
  auto times_size = times.size();
  for (auto interval : intervals_)
  {
    auto &freq_bartimes = bartimes_[interval];
    for (int i = bartime_right_ ? interval : 0; i < times_size; i += interval)
    {
      freq_bartimes.push_back(times[i]);
    }
    if (bartime_right_ && (freq_bartimes.empty() || freq_bartimes.back() != times.back()))
      freq_bartimes.push_back(times.back());
  }
}

bool __check_insert_bartime(sec_t bt, size_t count, const time_point &end, std::vector<sec_t> &ret)
{
  if (count == 0)
  {
    if (system_clock::from_time_t(bt) < end)
    {
      // if (ret.empty() || bt > ret.back())
      ret.push_back(bt);
    }
    else
      return false;
  }
  else
  {
    // if (ret.empty() || bt > ret.back())
    ret.push_back(bt);
    if (ret.size() >= count)
      return false;
  }
  return true;
}

template <class Data>
void Calendar<Data>::GenerateDailyBartimes(typename Data::iterator &&it, time_point start_dt, size_t count, time_point end, std::vector<sec_t> &ret) const
{
  if (it.is_end())
    throw OutOfCalendar();
  auto close_t = open_close_sessions_[0].second;
  sec_t last_close_bt = 0;
  do
  {
    sec_t bt = (*it).first;
    bt += close_t;
    auto bt_time_point = system_clock::from_time_t(bt);
    if (bartime_right_)
    {
      if (bt_time_point >= start_dt)
      {
        if (!__check_insert_bartime(bt, count, end, ret))
          return;
      }
    }
    else
    {
      if (bt_time_point > start_dt)
      {
        if (last_close_bt > 0)
        {
          if (!__check_insert_bartime(last_close_bt, count, end, ret))
            return;
          last_close_bt = 0;
        }
        if (!__check_insert_bartime(bt, count, end, ret))
          return;
      }
      else
      {
        last_close_bt = bt;
      }
    }
    ++it;
  } while (!it.is_end());
}

template <class Data>
void Calendar<Data>::GenerateMinuteBartimes(typename Data::iterator &&it, int interval, time_point start_dt, size_t count, time_point end, std::vector<sec_t> &ret) const
{
  if (it.is_end())
    throw OutOfCalendar();
  auto &times = bartimes_.at(interval);
  auto start_time = start_dt.time_since_epoch();
  auto start_time_in_sec = duration_cast<seconds>(start_time);
  // 向上取整
  if (bartime_right_ && start_time > start_time_in_sec)
    start_time_in_sec += 1s;
  std::vector<sec_t> bts;
  size_t total_count = 0;
  size_t start = 0;
  do
  {
    sec_t day = (*it).first;
    // merge sessions and bartimes
    auto &sessions = GetSessionsWithBreaks(day);
    for (auto &[sos, eos] : sessions)
    {
      if (sos > eos)
      { // 跨天
        for (auto t : times)
        {
          if (t >= sos)
            bts.push_back(day + t);
          else if (t <= eos)
            bts.push_back(day + iseconds_a_day + t);
        }
      }
      else
      {
        for (auto t : times)
        {
          if (t >= sos && t <= eos)
            bts.push_back(day + t);
        }
      }
    }

    if (start == 0 && total_count == 0)
    {
      if (bartime_right_)
      {
        // 起始K线时间 >= start_dt
        auto bts_it = std::lower_bound(bts.begin(), bts.end(), start_time_in_sec.count());
        start = bts_it - bts.begin();
      }
      else
      {
        // 起始K线时间 <= start_dt
        auto bts_it = std::upper_bound(bts.begin(), bts.end(), start_time_in_sec.count());
        if (bts_it == bts.begin())
        {
          bts.clear();
          --it;
          continue;
        }
        else if (bts_it == bts.end())
        {
          ++it;
          continue;
        }
        else
        {
          --bts_it;
        }
        start = bts_it - bts.begin();
      }
    }
    total_count = bts.size() - start;
    if (count > 0)
    {
      if (total_count >= count)
        break;
    }
    else if (system_clock::from_time_t(bts.back()) >= end)
      break;
    ++it;
  } while (!it.is_end());

  if (count > 0)
    std::copy_n(bts.begin() + start, count, std::back_inserter(ret));
  else
    std::copy_if(bts.begin() + start, bts.end(), std::back_inserter(ret), [&end](auto bt)
                 { return system_clock::from_time_t(bt) < end; });
}

template <class Data>
std::vector<sec_t> Calendar<Data>::GetBartimesImpl(seconds interval, time_point start, size_t count, time_point end) const
{
  int inte = interval.count();
  std::vector<sec_t> ret;
  if (!bartimes_.contains(inte))
  {
    if (interval == 1_d)
    {
      GenerateDailyBartimes(tradedays->Lower(to_daily(start - seconds_a_day)), start, count, end, ret);
    }
    else if (interval == 1_w)
    {
      GenerateDailyBartimes(tradedays->WeekEndLower(to_daily(start - seconds_a_day)), start, count, end, ret);
    }
    else if (interval == 1_m)
    {
      GenerateDailyBartimes(tradedays->MonthEndLower(to_daily(start - seconds_a_day)), start, count, end, ret);
    }
    else
    {
      throw InvalidInterval(inte);
    }
  }
  else
  {
    GenerateMinuteBartimes(tradedays->Upper(ToDaily(start - seconds(offset_))), inte, start, count, end, ret);
  }
  return ret;
}

template <class Data>
session_t Calendar<Data>::FindNextSession(time_point dt, bool with_breaks) const
{
  sec_t start_day = ToDaily(dt - seconds(offset_));
  auto it = tradedays->Upper(start_day);
  if (it.is_end())
    throw OutOfCalendar();
  sec_t next_sos_dt = -1;
  sec_t next_eos_dt = -1;
  do
  {
    sec_t day = (*it).first;
    ++it;
    for (auto &[sos, eos] : with_breaks ? GetSessionsWithBreaks(day) : GetSessionsWithoutBreaks(day))
    {
      if (next_sos_dt == -1 && sos != -1)
      {
        auto session_start = day + sos;
        if (dt < system_clock::from_time_t(session_start))
          next_sos_dt = session_start;
      }
      if (next_eos_dt == -1 && eos != -1)
      {
        auto session_end = eos == offset_ ? day + iseconds_a_day + eos : day + eos;
        if (dt <= system_clock::from_time_t(session_end))
          next_eos_dt = session_end;
      }
    }
    if (next_sos_dt != -1 && next_eos_dt != -1)
      break;
  } while (!it.is_end());
  if (next_sos_dt == -1 || next_eos_dt == -1)
    throw OutOfCalendar();
  return {next_sos_dt, next_eos_dt};
}

template <class Data>
std::optional<CallAuctionSession> Calendar<Data>::FindNextCASession(time_point dt, bool is_opening) const
{
  const auto &cas = is_opening ? opening_call_auctions_ : closing_call_auctions_;
  if (cas.size() == 0)
    return std::nullopt;
  while (1)
  {
    auto oc = GetNextSession(dt);
    auto tp = is_opening ? oc.first : oc.second;
    auto tm = tp % iseconds_a_day;
    for (auto [_1, _2, _3] : cas)
    {
      if (is_opening)
      {
        if (_3 == tm)
          return std::optional<CallAuctionSession>({tp + _1, tp + _2, tp});
      }
      else
      {
        if (_2 == tm)
          return std::optional<CallAuctionSession>({tp + _1, tp, tp + _3});
      }
    }
    dt = time_point(seconds(tp + 1));
  }
}

template <class Data>
bool Calendar<Data>::IsTrading(time_point dt) const
{
  auto [sos_dt, eos_dt] = GetNextSession(dt);
  return eos_dt < sos_dt; // 先收盘 再开盘
}

template <class Data>
bool Calendar<Data>::IsTradingDay(time_point dt) const
{
  dt -= seconds(offset_);
  try
  {
    auto day = to_daily(dt);
    if (is_daily(dt) && offset_ > 0)
      day -= iseconds_a_day;
    auto &node = tradedays->At(day);
    return node.IsTrading();
  }
  catch (std::out_of_range &e)
  {
    throw OutOfCalendar();
  }
}

template <class Data>
void Calendar<Data>::InitSpecialSessions(ankerl::unordered_dense::map<sec_t, std::shared_ptr<SpecialSessions>> &&sessions) noexcept
{
  special_sessions_.swap(sessions);
}

template <class Data>
const std::shared_ptr<SpecialSessions> Calendar<Data>::GetSpecialSessions(sec_t dt) const
{
  return special_sessions_.contains(dt) ? special_sessions_.at(dt) : nullptr;
}

template <class Data>
const std::vector<session_t> &Calendar<Data>::GetSessionsWithBreaks(sec_t dt) const
{
  auto ss = GetSpecialSessions(dt);
  return (ss != nullptr) ? ss->ordered_sessions : sorted_sessions_;
}

template <class Data>
const std::vector<session_t> &Calendar<Data>::GetSessionsWithoutBreaks(sec_t dt) const
{
  auto ss = GetSpecialSessions(dt);
  return (ss != nullptr) ? ss->open_close_sessions : open_close_sessions_;
}

template class Calendar<DatesArray>;
template <>
bool Calendar<Date7x24Array>::IsTrading(time_point dt) const { return true; }
template <>
bool Calendar<Date7x24Array>::IsTradingDay(time_point dt) const { return true; }
template <>
template <class Duration, class GreaterOrEqual, class LessOrEqual>
bool Calendar<Date7x24Array>::IsTradingTime(Duration dt) const { return true; }
template <>
template <class Duration>
bool Calendar<Date7x24Array>::IsTradingTime(Duration tm, RangeClosed side) const { return true; }
template class Calendar<Date7x24Array>;

#pragma endregion

#pragma region CalendarAstock
DatesArray CalendarAstock::dates_container;
const CalendarAstock &CalendarAstock::GetInstance(const std::string &symbol)
{
  static CalendarAstock cal(dates_container,
                            {{34200, 41400}, {46800, 54000}},
                            {{-900, -300, 34200}},
                            {{-180, 54000, 120}},
                            {1min, 5min, 15min, 30min, 1h, 2h},
                            "Asia/Shanghai");
  return cal;
}
#pragma endregion

#pragma region CalendarCTP
DatesArray CalendarCTP::dates_container;
ankerl::unordered_dense::map<std::string, CalendarCTP> CalendarCTP::calendar_ctps;

bool CalendarCTP::HasNight() const
{
  for (auto const &[o, c] : GetSessions())
  {
    if (o >= 21 * 3600) // 夜盘都是晚上9点开始
      return true;
  }
  return false;
}

void uppercase(std::string &str)
{
  std::transform(str.begin(), str.end(), str.begin(), ::toupper);
}

/// @brief To product id and to upper case
/// @param symbol
/// @return upper cased product id
std::string convert_symbol(const std::string &symbol)
{
  auto b = symbol.begin();
  auto e = symbol.end();
  auto it = std::find_if(b, e, [](unsigned char c)
                         { return std::isdigit(c); });
  if (it != e)
  {
    auto ret = symbol.substr(0, std::distance(b, it));
    uppercase(ret);
    return ret;
  }
  else
  {
    std::string ret(symbol);
    uppercase(ret);
    return ret;
  }
}

void CalendarCTP::Init(const std::vector<date_status_item> &dates_arr, std::vector<session_item> &&sessions, bool bartime_right)
{
  dates_container.Init(dates_arr);
  std::vector<seconds> intervals{1min, 3min, 5min, 10min, 15min, 30min, 1h, 2h, 3h, 4h};
  const std::string tz("Asia/Shanghai");
  constexpr sec_t offset = duration_cast<seconds>(2h + 30min).count();
  // common sessions
  calendar_ctps.emplace("", CalendarCTP(dates_container,
                                        {{75600, 9000}, {32400, 54900}},
                                        {{-300, -60, 75600}, {-300, -60, 32400}}, // 开盘前5分钟集合竞价，并且前一分钟不能申报不能撤单，前4分钟可以申报可以撤单
                                        {},                                       // 一般品种无收盘集合竞价
                                        intervals,
                                        tz,
                                        offset,
                                        bartime_right));
  // TODO: 暂时没有收盘集合竞价（有些品种有收盘集合竞价，这里需要区分）
  std::array<std::string, 0> products_has_closing_ca{};
  // custom sessions
  for (auto &[product_id, market_time] : sessions)
  {
    uppercase(product_id);
    sec_t special_offset = 0;
    if (market_time[0].second <= offset) // 跨0点
      special_offset = market_time[0].second;
    std::vector<CallAuctionSession> opening_call_auctions;
    auto first_open_time = market_time[0].first;
    opening_call_auctions.push_back({-300, -60, first_open_time});
    if (first_open_time >= 21 * 3600) // 夜盘都是晚上9点开始
    {
      // 还有日盘开盘集合竞价（TODO: 是否所有品种的日盘开盘都有集合竞价？）
      auto second_open_time = market_time[1].first;
      opening_call_auctions.push_back({-300, -60, second_open_time});
    }
    std::vector<CallAuctionSession> closing_call_auctions;
    auto iter = std::find(products_has_closing_ca.begin(), products_has_closing_ca.end(), product_id);
    if (iter != products_has_closing_ca.end())
    {
      closing_call_auctions.push_back({-300, market_time[market_time.size() - 1].second, 0});
    }

    calendar_ctps.emplace(product_id, CalendarCTP(dates_container,
                                                  market_time,
                                                  opening_call_auctions,
                                                  closing_call_auctions,
                                                  intervals,
                                                  tz,
                                                  special_offset,
                                                  bartime_right));
  }
  // set special sessions
  // 特殊规则：交易日夜盘不开盘。第二天是节假日，夜盘不交易
  std::vector<sec_t> before_holidays;
  std::vector<sec_t> after_holidays;
  const std::pair<sec_t, DateNode> *pre_item = nullptr;
  auto end = dates_container.cend();
  for (auto it = dates_container.cbegin(); it != end; ++it)
  {
    auto &item = *it;
    if (pre_item)
    {
      auto pre_status = pre_item->second.status_;
      auto cur_status = item.second.status_;
      if (pre_status == 1 && cur_status == 3) // 今天节假日，昨天夜盘不交易
      {
        before_holidays.push_back(pre_item->first);
      }
      else if (pre_status == 3 && cur_status == 1) // 昨天节假日，今日上午算开盘
      {
        after_holidays.push_back(item.first);
      }
    }
    pre_item = &item;
  }
  for (auto &[pid, cal] : calendar_ctps)
  {
    if (cal.HasNight())
    {
      // 删除夜盘开盘时间
      auto &open_close = cal.GetOpenCloseTime();
      auto &sorted_sessions = cal.GetOrderedSessions();
      std::vector<session_t> sorted_sessions_without_night;
      std::copy_if(sorted_sessions.begin(),
                   sorted_sessions.end(),
                   std::back_inserter(sorted_sessions_without_night),
                   [](const session_t &s)
                   { return s.first != 21 * 3600; });
      auto before_holiday_session = std::make_shared<SpecialSessions>(std::vector<session_t>{{-1, open_close.second}}, sorted_sessions_without_night);

      // 额外添加一个开盘时间
      auto after_holiday_session = std::make_shared<SpecialSessions>(std::vector<session_t>{{9 * 3600, -1}, open_close}, sorted_sessions);

      ankerl::unordered_dense::map<sec_t, std::shared_ptr<SpecialSessions>> special_sessions;
      for (auto &day : before_holidays)
      {
        special_sessions.emplace(day, before_holiday_session);
      }
      for (auto &day : after_holidays)
      {
        special_sessions.emplace(day, after_holiday_session);
      }
      cal.InitSpecialSessions(std::move(special_sessions));
    }
  }
}

const CalendarCTP &CalendarCTP::GetInstance(const std::string &symbol)
{
  try
  {
    return calendar_ctps.at(convert_symbol(symbol));
  }
  catch (std::out_of_range &e)
  {
    throw CalendarNotFound("CalendarCTP", symbol);
  }
}

#pragma endregion

#pragma region Time7x24Calendar
Date7x24Array Time7x24Calendar::dates_container;
const Time7x24Calendar &Time7x24Calendar::GetInstance(const std::string &symbol)
{
  static Time7x24Calendar cal(dates_container, {{0, 86400}}, {}, {}, {1min, 3min, 5min, 10min, 15min, 30min, 1h, 2h, 3h, 4h}, "UTC", 0, false);
  return cal;
}

#pragma endregion

NS_QMC_END