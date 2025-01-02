#include <regex>
#include <string>

#include "quantcalendar/calendar.h"
#include "calendar.h"

NS_QMC_BEGIN

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
bool check_append_conditions(bt, offset, vector<int> &ret, int count = 0)
{
  if (end is not None && bt >= end)
    return True;
  ret.push_back(bt + offset);
  if (count > 0 && ret.size() >= count)
    return True;
  return False;
}

void _to_offset_dt(dt)
{
  auto dt = dt - this->offset auto trading_day = dt;
  if (dt.time() == time.min and this._bartime_side_right)
  {
    trading_day -= day_offset
  }
  return {dt, trading_day};
}

auto calc_bartimestamp_left(start, end, const vector<std::pair<time_piont, time_piont>> &jumps, inte)
{
  int jump_idx = 0;
  int jumps_len = jumps.size();
  vector<int> ret;
  while (start < end)
  {
    ret.push_back(start);
    start += inte;
    while (jump_idx < jumps_len)
    {
      auto &[jtime, cumj] = jumps[jump_idx];
      if (start > jtime)
      {
        start += cumj;
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

auto calc_bartimestamp_right(start, end, const vector<std::pair<time_piont, time_piont>> &jumps, inte)
{
  int jump_idx = 0;
  int jumps_len = jumps.size();
  vector<int> ret;
  while (start <= end)
  {
    start += inte;
    while (jump_idx < jumps_len)
    {
      auto &[jtime, cumj] = jumps[jump_idx];
      if (start > jtime)
      {
        start += cumj;
        jump_idx += 1;
      }
      else
      {
        break;
      }
    }
    if (start <= end)
      ret.push_back(start)
  }
  if (ret.empty() or *ret.rend() < end)
    ret.push_back(end);
  return ret;
}

Calendar::Calendar(
    std::vector<session_t> &&sessions,
    std::vector<seconds> &&intervals,
    bool bartime_right) : sessions_(std::move(sessions)),
                          intervals_(std::move(intervals)),
                          bartime_right_(bartime_right)
{
  CalcBartimes();
}

void Calendar::CalcBartimes()
{
  auto &[start, end] = sessions_[0];
  vector<std::pair<time_piont, time_piont>> jumps;
  last_eos = None;
  for (auto &[sos, eos] : sessions_)
  {
    if (sos < start)
      sos += 86400;
    if (eos < start)
      eos += 86400;
    if (last_eos is not None)
      jumps.append((last_eos, sos - last_eos));
    last_eos = eos;
  }
  if (end < start)
    end += 86400;
  ret = {};
  if (bartime_right_)
  {
    for (auto inte : intervals_)
      ret[inte] = calc_bartimestamp_right(start, end, jumps, inte);
  }
  else
  {
    for (auto inte : intervals_)
      ret[inte] = calc_bartimestamp_left(start, end, jumps, inte);
  }
  return ret;
}

std::vector<sec_t> Calendar::GetBartimes(seconds interval, time_point start, time_point end)
{
}

std::vector<sec_t> Calendar::GetBartimes(seconds interval, time_point start, int count)
{
  vector<time_point> ret;
  times = self._bartimestamp.get(interval, None);
  auto &&[dt, start_day] = self._to_offset_dt(start);
  if (times is None)
  {
    if (interval == DAILY)
    {
      for (auto &day : self.get_tradedays_gte(start_day))
      {
        auto close_time = self._combine_date_time(
            day, self._open_close_sessions[0][-1]);
        if (dt <= close_time)
        {
          if (check_append_conditions(close_time))
            return ret;
        }
      }
    }
    else if (interval == WEEKLY)
    {
      for (auto close_time : self._get_bartimes(dt, start_day, _check_next_week))
      {
        if (check_append_conditions(close_time))
          return ret;
      }
    }
    else if (interval == MONTHLY)
    {
      for (auto &close_time : self._get_bartimes(dt, start_day, _check_next_month))
      {
        if (check_append_conditions(close_time))
          return ret;
      }
    }
    else
    {
      throw ValueError(f "bartime {interval} not supported");
    }
  }
  else
  {
    for (auto &day : self.get_tradedays_gte(start_day))
    {
      auto &sessions = self._get_sessions_with_breaks(day);
      bts = list(map(lambda x : self._combine_date_time(day, x), times));
      for (auto &[sos, eos] : sessions)
      {
        eos = self._combine_date_time(day, eos);
        if (dt > eos)
          continue;
        sos = self._combine_date_time_sos(day, sos);
        for (auto &bt in bts)
        {
          if (bt >= dt && bt <= eos && bt >= sos)
          {
            if (check_append_conditions(bt))
              return ret;
          }
        }
      }
    }
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

#pragma endregion

#pragma region CalendarAstock
CalendarData CalendarAstock::calendar_data("Asia/Shanghai");
CalendarAstock CalendarAstock::cal(std::vector<session_t>{{34200, 41400}, {46800, 54000}});
#pragma endregion

NS_QMC_END