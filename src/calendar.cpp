#include <string>
#include <algorithm>

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
    ret.push_back(session_start < iseconds_a_day ? session_start : session_start - iseconds_a_day);
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
      ret.push_back(session_start < iseconds_a_day ? session_start : session_start - iseconds_a_day);
  }
  if (ret.empty() || ret[ret.size() - 1] < session_end)
    ret.push_back(session_end < iseconds_a_day ? session_end : session_end - iseconds_a_day);
  return ret;
}

Calendar::Calendar(
    const CalendarData &data,
    std::vector<session_t> &&sessions,
    std::vector<seconds> &&intervals,
    std::string_view tz,
    sec_t offset,
    bool bartime_right) : data_(data),
                          sessions_(std::move(sessions)),
                          intervals_(intervals.size()),
                          tz_(tz),
                          offset_(offset),
                          bartime_right_(bartime_right)
{
  offset_minus_day_ = offset_ - iseconds_a_day;
  std::transform(intervals.begin(), intervals.end(), std::back_inserter(intervals_), [](seconds &sec)
                 { return sec.count(); });
  open_close_sessions_.emplace_back(sessions_[0].first, sessions_[sessions_.size() - 1].second);
  sorted_sessions_ = sessions_;
  std::sort(sorted_sessions_.begin(), sorted_sessions_.end(), [](const session_t &x, const session_t &y)
            { return x.first < y.first; });
  CalcBartimes();
}

std::pair<time_point, time_point> Calendar::ApplyOffset(time_point dt) const
{
  dt -= seconds(offset_);
  auto trading_day = dt;
  // 凌晨时刻判断交易日属于前一天还是后一天
  if (is_daily(dt) && bartime_right_)
    trading_day -= seconds_a_day;
  return {dt, trading_day};
}

void Calendar::CalcBartimes()
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

inline std::vector<sec_t> Calendar::GetBartimes(seconds interval, time_point start, time_point end) const
{
  return GetBartimesImpl(interval, start, 0, end);
}

inline std::vector<sec_t> Calendar::GetBartimes(seconds interval, time_point start, size_t count) const
{
  return GetBartimesImpl(interval, start, count, time_point::min());
}

void Calendar::GenerateDailyBartimes(CalendarData::iterator &&it, time_point start_dt, size_t count, time_point end, std::vector<sec_t> &ret) const
{
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

void Calendar::GenerateMinuteBartimes(CalendarData::iterator &&it, int interval, time_point start_dt, size_t count, time_point end, std::vector<sec_t> &ret) const
{
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

std::vector<sec_t> Calendar::GetBartimesImpl(seconds interval, time_point start, size_t count, time_point end) const
{
  // @TODO: end 没有apply offset
  int inte = interval.count();
  auto &&[start_dt, start_day] = ApplyOffset(start);
  std::vector<sec_t> ret;
  if (!bartimes_.contains(inte))
  {
    if (interval == 1_d)
    {
      GenerateDailyBartimes(TradedaysUpper(to_daily(start_day)), start_dt, count, end, ret);
    }
    else if (interval == 1_w)
    {
      GenerateDailyBartimes(WeekEndUpper(to_daily(start_day)), start_dt, count, end, ret);
    }
    else if (interval == 1_m)
    {
      GenerateDailyBartimes(MonthEndUpper(to_daily(start_day)), start_dt, count, end, ret);
    }
    else
    {
      throw InvalidArgumentInterval(inte);
    }
  }
  else
  {
    GenerateMinuteBartimes(TradedaysUpper(to_daily(start_day)), inte, start_dt, count, end, ret);
  }
  if (ret.empty())
  {
    throw OutOfCalendar();
  }
  return ret;
}

inline sec_t Calendar::GetCurrentBartime(time_point dt, seconds interval) const
{
  return GetBartimesImpl(interval, dt, 1, time_point::min())[0];
}

session_t Calendar::FindNextSession(time_point dt, bool with_breaks) const
{
  auto &&[start_dt, start_day] = ApplyOffset(dt);
  sec_t next_sos_dt, next_eos_dt = -1;
  auto it = TradedaysUpper(to_daily(start_day));
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

inline session_t Calendar::GetNextOpenClose(time_point dt) const
{
  return FindNextSession(dt, false);
}

inline session_t Calendar::GetNextSession(time_point dt) const
{
  return FindNextSession(dt, true);
}

bool Calendar::IsTrading(time_point dt) const
{
  auto [sos_dt, eos_dt] = GetNextSession(dt);
  return eos_dt < sos_dt; // 先收盘 再开盘
}

bool Calendar::IsTradingDay(time_point dt) const
{
  auto start_day = ApplyOffset(dt).second;
  auto &node = data_.At(to_daily(start_day));
  return node.IsTrading();
}

bool Calendar::IsTradingTime(time_point dt) const
{
  auto tm = to_time(dt);
  for (auto [start, end] : sorted_sessions_)
  {
    if (start < end)
    {
      if (tm >= seconds(start) and tm <= seconds(end))
        return true;
    }
    else
    {
      if (tm >= seconds(start) or tm <= seconds(end))
        return true;
    }
  }
  return false;
}

void Calendar::InitSpecialSessions(ankerl::unordered_dense::map<sec_t, SpecialSessions> &&sessions)
{
  special_sessions_.swap(sessions);
}

const SpecialSessions *Calendar::GetSpecialSessions(sec_t dt) const
{
  return special_sessions_.contains(dt) ? &special_sessions_.at(dt) : nullptr;
}

const std::vector<session_t> &Calendar::GetSessionsWithBreaks(sec_t dt) const
{
  auto ss = GetSpecialSessions(dt);
  return (ss != nullptr) ? ss->ordered_sessions : sorted_sessions_;
}

const std::vector<session_t> &Calendar::GetSessionsWithoutBreaks(sec_t dt) const
{
  auto ss = GetSpecialSessions(dt);
  return (ss != nullptr) ? ss->open_close_sessions : open_close_sessions_;
}

sec_t Calendar::CombineDatetime(sec_t tradingday, sec_t time) const
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

sec_t Calendar::CombineDatetimeSos(sec_t tradingday, sec_t time) const
{
  // 开盘不可能跨越0点
  if (time == sessions_[0].first)
    return tradingday + time - offset_;
  else
    return CombineDatetime(tradingday, time);
}

#pragma endregion

#pragma region CalendarAstock
CalendarData CalendarAstock::calendar_data;
const CalendarAstock &CalendarAstock::GetInstance(std::string symbol)
{
  static CalendarAstock cal(calendar_data, {{34200, 41400}, {46800, 54000}}, {1min, 5min, 15min, 30min, 1h, 2h}, "Asia/Shanghai");
  return cal;
}
#pragma endregion

NS_QMC_END