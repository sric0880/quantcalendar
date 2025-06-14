#include <string>
#include <algorithm>
#include "fmt/format.h"
#include "fmt/ranges.h"
#include "fmt/chrono.h"

#include "quantcalendar/calendar.h"

NS_QMC_BEGIN

#pragma region Calendar
auto calc_bartimestamp_left(int session_start, int session_end, std::vector<std::pair<int, int>> &jumps, int inte)
{
  int jump_idx = 0;
  int jumps_len = jumps.size();
  std::vector<int> ret;
  while (session_start < session_end)
  {
    ret.push_back(session_start);
    session_start += inte;
    while (jump_idx < jumps_len)
    {
      auto &[jtime, cumj] = jumps[jump_idx];
      if (session_start > jtime)
      {
        session_start += cumj;
        jump_idx += 1;
      }
      else
      {
        break;
      }
    }
  }
  std::transform(ret.begin(), ret.end(), ret.begin(), [](int t)
                 { return t < iseconds_a_day ? t : t - iseconds_a_day; });
  return ret;
}

auto calc_bartimestamp_right(int session_start, int session_end, const std::vector<std::pair<int, int>> &jumps, int inte)
{
  int jump_idx = 0;
  int jumps_len = jumps.size();
  std::vector<int> ret;
  while (session_start <= session_end)
  {
    session_start += inte;
    while (jump_idx < jumps_len)
    {
      auto &[jtime, cumj] = jumps[jump_idx];
      if (session_start > jtime)
      {
        session_start += cumj;
        jump_idx += 1;
      }
      else
      {
        break;
      }
    }
    if (session_start <= session_end)
      ret.push_back(session_start);
  }
  if (ret.empty() || ret[ret.size() - 1] < session_end)
    ret.push_back(session_end);
  std::transform(ret.begin(), ret.end(), ret.begin(), [](int t)
                 { return t < iseconds_a_day ? t : t - iseconds_a_day; });
  return ret;
}

template <class Data>
Calendar<Data>::Calendar(
    const Data &dates_container,
    std::vector<session_t> &&sessions,
    const std::vector<seconds> &intervals,
    std::string_view tz,
    sec_t offset,
    bool bartime_right) : tradedays(&dates_container),
                          sessions_(std::move(sessions)),
                          intervals_(intervals.size()),
                          tz_(tz),
                          offset_(offset),
                          bartime_right_(bartime_right)
{
  offset_minus_day_ = offset_ - iseconds_a_day;
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
  // 凌晨时刻判断交易日属于前一天还是后一天
  if (is_daily(applied_offset_dt) && bartime_right_)
    return to_daily(applied_offset_dt - seconds_a_day);
  return to_daily(applied_offset_dt);
}

template <class Data>
void Calendar<Data>::CalcBartimes()
{
  auto start = sessions_[0].first;
  auto end = sessions_[sessions_.size() - 1].second;
  std::vector<std::pair<int, int>> jumps;
  int next_sos;
  int last_eos = -1;
  for (auto &[sos, eos] : sessions_)
  {
    next_sos = sos;
    if (sos < start)
      next_sos += iseconds_a_day;
    if (last_eos != -1)
      jumps.emplace_back(last_eos, next_sos - last_eos);
    last_eos = eos;
    if (eos < start)
      last_eos += iseconds_a_day;
  }
  if (end < start) // 跨越0点
    end += iseconds_a_day;
  if (bartime_right_)
  {
    for (auto inte : intervals_)
      bartimes_.emplace(inte, calc_bartimestamp_right(start, end, jumps, inte));
  }
  else
  {
    for (auto inte : intervals_)
      bartimes_.emplace(inte, calc_bartimestamp_left(start, end, jumps, inte));
  }
}

template <class Data>
void Calendar<Data>::GenerateDailyBartimes(typename Data::iterator &&it, time_point start_dt, size_t count, time_point end, std::vector<sec_t> &ret) const
{
  if (it.is_end())
    throw OutOfCalendar();
  auto close_t = open_close_sessions_[0].second;
  do
  {
    sec_t bt = (*it).first;
    ++it;
    bt = CombineDatetime(bt, close_t);
    auto bt_time_point = system_clock::from_time_t(bt);
    if (start_dt <= bt_time_point)
    {
      if (count == 0)
      {
        if (bt_time_point < end)
          ret.push_back(bt + offset_);
        else
          return;
      }
      else
      {
        ret.push_back(bt + offset_);
        if (ret.size() >= count)
          return;
      }
    }
  } while (!it.is_end());
}

template <class Data>
void Calendar<Data>::GenerateMinuteBartimes(typename Data::iterator &&it, int interval, time_point start_dt, size_t count, time_point end, std::vector<sec_t> &ret) const
{
  if (it.is_end())
    throw OutOfCalendar();
  auto &times = bartimes_.at(interval);
  do
  {
    sec_t day = (*it).first;
    ++it;
    auto &sessions = GetSessionsWithBreaks(day);
    std::vector<sec_t> bts;
    for (auto t : times)
    {
      bts.push_back(CombineDatetime(day, t));
    }
    for (auto &[sos, eos] : sessions)
    {
      auto session_end = CombineDatetime(day, eos);
      if (start_dt > system_clock::from_time_t(session_end))
        continue;
      auto session_start = CombineDatetimeSos(day, sos);
      for (auto &bt : bts)
      {
        auto bt_time_point = system_clock::from_time_t(bt);
        if (bt_time_point >= start_dt && bt <= session_end && bt >= session_start)
        {
          if (count == 0)
          {
            if (bt_time_point < end)
              ret.push_back(bt + offset_);
            else
              return;
          }
          else
          {
            ret.push_back(bt + offset_);
            if (ret.size() >= count)
              return;
          }
        }
      }
    }
  } while (!it.is_end());
}

template <class Data>
std::vector<sec_t> Calendar<Data>::GetBartimesImpl(seconds interval, time_point start, size_t count, time_point end) const
{
  int inte = interval.count();
  seconds offset(offset_);
  start -= offset;
  end -= offset;
  sec_t start_day = ToDaily(start);
  std::vector<sec_t> ret;
  if (!bartimes_.contains(inte))
  {
    if (interval == 1_d)
    {
      GenerateDailyBartimes(tradedays->Upper(start_day), start, count, end, ret);
    }
    else if (interval == 1_w)
    {
      GenerateDailyBartimes(tradedays->WeekEndUpper(start_day), start, count, end, ret);
    }
    else if (interval == 1_m)
    {
      GenerateDailyBartimes(tradedays->MonthEndUpper(start_day), start, count, end, ret);
    }
    else
    {
      throw InvalidInterval(inte);
    }
  }
  else
  {
    GenerateMinuteBartimes(tradedays->Upper(start_day), inte, start, count, end, ret);
  }
  return ret;
}

template <class Data>
session_t Calendar<Data>::FindNextSession(time_point dt, bool with_breaks) const
{
  dt -= seconds(offset_);
  sec_t start_day = ToDaily(dt);
  sec_t next_sos_dt = -1;
  sec_t next_eos_dt = -1;
  auto it = tradedays->Upper(start_day);
  if (it.is_end())
    throw OutOfCalendar();
  do
  {
    sec_t day = (*it).first;
    ++it;
    for (auto &[sos, eos] : with_breaks ? GetSessionsWithBreaks(day) : GetSessionsWithoutBreaks(day))
    {
      if (next_sos_dt == -1 && sos != -1)
      {
        auto session_start = CombineDatetimeSos(day, sos);
        if (dt < system_clock::from_time_t(session_start))
          next_sos_dt = session_start;
      }
      if (next_eos_dt == -1 && eos != -1)
      {
        auto session_end = CombineDatetime(day, eos);
        if (dt <= system_clock::from_time_t(session_end))
          next_eos_dt = session_end;
      }
    }
    if (next_sos_dt != -1 && next_eos_dt != -1)
      break;
  } while (!it.is_end());
  if (next_sos_dt == -1 || next_eos_dt == -1)
    throw OutOfCalendar();
  return {next_sos_dt + offset_, next_eos_dt + offset_};
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
    auto &node = tradedays->At(to_daily(dt));
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

template <class Data>
sec_t Calendar<Data>::CombineDatetime(sec_t tradingday, sec_t time) const
{
  int offset;
  if (time > offset_)
    offset = offset_;
  else if (time == offset_)
    offset = bartime_right_ ? offset_minus_day_ : offset_;
  else
    offset = offset_minus_day_;
  return tradingday + time - offset;
}

template <class Data>
sec_t Calendar<Data>::CombineDatetimeSos(sec_t tradingday, sec_t time) const
{
  // 开盘不可能跨越0点
  if (time == sessions_[0].first)
    return tradingday + time - offset_;
  else
    return CombineDatetime(tradingday, time);
}

inline std::string time_fmt(sec_t sec)
{
  return fmt::format("{:%H:%M:%S}", seconds(sec));
}

template <class Data>
std::string Calendar<Data>::ToString() const
{
  std::vector<std::string> sessions;
  int i = 1;
  for (auto &[_sos, _eos] : sessions_)
  {
    if (_eos <= _sos)
      sessions.emplace_back(fmt::format("\t{}) {}-{}(+1 days)", i, time_fmt(_sos), time_fmt(_eos)));
    else
      sessions.emplace_back(fmt::format("\t{}) {}-{}", i, time_fmt(_sos), time_fmt(_eos)));
    ++i;
  }

  std::vector<std::string> bartimestamps;
  for (auto &[inte, bts] : bartimes_)
  {
    int k = inte;
    k /= 60;
    char unit = 'm';
    if (k >= 60)
    {
      k /= 60;
      unit = 'H';
    }
    size_t bts_size = bts.size();
    std::vector<std::string> bt_strs(bts_size);
    std::transform(bts.begin(), bts.end(), bt_strs.begin(), [](const int &sec)
                   { return time_fmt(sec); });
    if (bts_size > 8)
      bartimestamps.emplace_back(fmt::format("\t{}{})\t[{}]", k, unit, fmt::format("{}, {}, {}, {},...{}, {}, {}, {}", bt_strs[0], bt_strs[1], bt_strs[2], bt_strs[3], bt_strs[bts_size - 4], bt_strs[bts_size - 3], bt_strs[bts_size - 2], bt_strs[bts_size - 1])));
    else
      bartimestamps.emplace_back(fmt::format("\t{}{})\t[{}]", k, unit, fmt::join(bt_strs, ", ")));
  }
  return fmt::format("时区: {}\n交易时间段:\n {}\nK线时间点划分:\n {}\n", tz_, fmt::join(sessions, "\n"), fmt::join(bartimestamps, "\n"));
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
  static CalendarAstock cal(dates_container, {{34200, 41400}, {46800, 54000}}, {1min, 5min, 15min, 30min, 1h, 2h}, "Asia/Shanghai");
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

void CalendarCTP::Init(const std::vector<date_status_item> &dates_arr, std::vector<session_item> &&sessions)
{
  dates_container.Init(dates_arr);
  std::vector<seconds> intervals{1min, 3min, 5min, 10min, 15min, 30min, 1h, 2h, 3h, 4h};
  const std::string tz("Asia/Shanghai");
  constexpr const sec_t offset = duration_cast<seconds>(2h + 30min).count();
  // common sessions
  calendar_ctps.emplace("", CalendarCTP(dates_container, {{75600, 9000}, {32400, 54900}}, intervals, tz, offset));
  // custom sessions
  for (auto &[product_id, market_time] : sessions)
  {
    uppercase(product_id);
    sec_t special_offset = 0;
    if (market_time[0].second <= offset)
      special_offset = market_time[0].second;
    calendar_ctps.emplace(product_id, CalendarCTP(dates_container, std::move(market_time), intervals, tz, special_offset));
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
  static Time7x24Calendar cal(dates_container, {{0, 86400}}, {1min, 3min, 5min, 10min, 15min, 30min, 1h, 2h, 3h, 4h}, "UTC");
  return cal;
}

#pragma endregion

NS_QMC_END