#include <string>
#include <algorithm>

#include "quantcalendar/calendar.h"

NS_QMC_BEGIN

constexpr const int iseconds_a_day = 86400;
constexpr const seconds seconds_a_day(86400);

// void seconds_to_time(int seconds, std::tm &out)
// {
//   if (seconds == 86400)
//   {
//     out.tm_hour = 0;
//     out.tm_minute = 0;
//     out.tm_second = 0;
//   }
//   else
//   {
//     int value = seconds % 3600;
//     int seconds -= value * 3600;
//     out.tm_hour = value;
//     value = seconds % 60;
//     seconds -= value * 60;
//     out.tm_minute = value;
//     out.tm_second = seconds;
//   }
// }
// inline int time_to_seconds(std::tm &tm)
// {
//   return tm.tm_hour * 3600 + tm.tm_minute * 60 + tm.tm_second;
// }

// inline void bartime_seconds_to_time(sec, std::tm &out)
// {
//   if (sec < 86400)
//     return seconds_to_time(sec, out);
//   else
//     return seconds_to_time(sec - 86400, out);
// }

// static const std::regex interval_pattern("([\\d]+)([smhdwM])");
// int IntervalToSeconds(std::string_view interval)
// {
//   std::smatch matches;
//   if (std::regex_search(interval, matches, interval_pattern))
//   {
//     int num = std::atoi(matches[1].str().c_str());
//     char unit = matches[2].str().at(0);
//     return num * UnitToSeconds(unit);
//   }
//   else
//   {
//     raise ValueError(f "{interval_str} is invalid.");
//   }
// }

#pragma region Calendar
inline bool check_bartime_append_condition(sec_t bt, sec_t end, int offset, std::vector<sec_t> &ret)
{
  if (bt >= end)
    return true;
  ret.push_back(bt + offset);
  return false;
}

inline bool check_bartime_append_condition(sec_t bt, int count, int offset, std::vector<sec_t> &ret)
{
  ret.push_back(bt + offset);
  return ret.size() >= count;
}

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
  if (ret.empty() || ret[ret.size() -1] < session_end)
    ret.push_back(session_end);
  return ret;
}

Calendar::Calendar(
    std::vector<session_t> &&sessions,
    std::vector<seconds> &&intervals,
    bool bartime_right) : sessions_(std::move(sessions)),
                          intervals_(intervals.size()),
                          bartime_right_(bartime_right)
{
  std::transform(intervals.begin(), intervals.end(), intervals_.begin(), [](seconds& sec) {return sec.count();});
  open_close_sessions_.emplace_back(sessions_[0].first, sessions_[sessions_.size() - 1].second);
  std::copy(sessions_.begin(), sessions_.end(), sorted_sessions_.begin());
  std::sort(sorted_sessions_.begin(), sorted_sessions_.end(), [](session_t& x, session_t& y) {return x.first < y.first;});
  CalcBartimes();
}

std::pair<time_point, time_point> Calendar::ApplyOffset(time_point dt)
{
  dt -= GetData().offset_;
  auto trading_day = dt;
  // 凌晨时刻判断交易日属于前一天还是后一天
  if (is_daily(dt) && bartime_right_)
    trading_day -= seconds_a_day;
  return { dt, trading_day };
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

std::vector<sec_t> Calendar::GetBartimes(seconds interval, time_point start, time_point end)
{
}

void Calendar::GetDailyBartimes(CalendarData::iterator&& it, int count, int offset, std::vector<sec_t>& ret)
{
  auto close_t = open_close_sessions_[0].second;
  auto ddt = dt.time_since_epoch();
  do
  {
    sec_t bt = (*it).first;
    ++it;
    bt = CombineDatetime(bt, close_t);
    if (ddt <= seconds(bt))
    {
      if (check_bartime_append_condition(bt, count, offset, ret))
        return;
    }
  } while (!it.is_end());
}

std::vector<sec_t> Calendar::GetBartimes(seconds interval, time_point start, int count)
{
  int offset = GetData().offset_.count();
  int inte = interval.count();
  auto &&[dt, start_day] = ApplyOffset(start);
  std::vector<sec_t> ret;
  if (!bartimes_.contains(inte))
  {
    if (interval == 1_d)
    {
      GetDailyBartimes(TradedaysUpper(to_daily(start_day)), count, offset, ret);
    }
    else if (interval == 1_w)
    {
      GetDailyBartimes(WeekEndUpper(to_daily(start_day)), count, offset, ret);
    }
    else if (interval == 1_m)
    {
      GetDailyBartimes(MonthEndUpper(to_daily(start_day)), count, offset, ret);
    }
    else
    {
      throw InvalidArgumentInterval(inte);
    }
  }
  else
  {
    auto &times = bartimes_[inte];
    auto it = TradedaysUpper(to_daily(start_day));
    auto ddt = dt.time_since_epoch();
    do
    {
      sec_t bt = (*it).first;
      ++it;
      auto &sessions = GetSessionsWithBreaks(bt);
      std::vector<sec_t> bts;
      std::transform(times.begin(), times.end(), bts.begin(), [](int& x) {return CombineDatetime(bt, x); });
      for (auto& [sos, eos] : sessions)
      {
        eos = CombineDatetime(bt, eos);
        if (ddt > seconds(eos))
          continue;
        sos = CombineDatetimeSos(bt, sos);
        for (auto& bt in bts)
        {
          if (seconds(bt) >= ddt && bt <= eos && bt >= sos)
          {
            if (check_bartime_append_condition(bt, count, offset, ret))
              return ret;
          }
        }
      }
    } while (!it.is_end());
  }
  if (ret.empty())
  {
    throw OutOfCalendar();
  }
  return ret;
}

inline sec_t Calendar::GetCurrentBartime(time_point dt, seconds interval)
{
}
/*
获取K线时间

  Params :
  interval(seconds) : K线间隔周期
  */
auto get_current_bartime(dt : datetime, int interval) : return get_bartimes(interval, dt, count = 1)[0]

                                                        const SpecialSessions
                                                        & Calendar::GetSpecialSessions(time_point dt) const
{
}

const session_t Calendar::GetOpenCloseDT(sec_t dt) const
{
}

const session_t Calendar::GetSessionDT(sec_t dt) const
{
}
const void Calendar::GetOrderedSessions() const
{
}
const void Calendar::GetOpenCloseTime() const
{
}
bool Calendar::IsTrading(time_point dt) const
{
}
bool Calendar::IsTradingDay(time_point dt) const
{
}
bool Calendar::IsTradingTime(time_point dt) const
{
}

const std::optional<SpecialSessions> Calendar::GetSpecialSessions(sec_t dt)
{
  return special_sessions_.find(dt);
}

std::vector<session_t>& Calendar::GetSessionsWithBreaks(sec_t dt)
{
  auto s = GetSpecialSessions(dt);
  return s.
if s is not None :
  return s.ordered_sessions
else :
  return sorted_sessions_
}

sec_t Calendar::CombineDatetime(sec_t tradingday, sec_t time)
{

}
sec_t Calendar::CombineDatetimeSos(sec_t tradingday, sec_t time)
{

}

#pragma endregion

#pragma region CalendarAstock
CalendarData CalendarAstock::calendar_data("Asia/Shanghai");
CalendarAstock CalendarAstock::cal(std::vector<session_t>{{34200, 41400}, {46800, 54000}});
#pragma endregion

NS_QMC_END