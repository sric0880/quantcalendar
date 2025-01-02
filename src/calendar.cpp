#include <regex>
#include <string>

#include "quantcalendar/calendar.h"

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

//std::vector<sec_t> Calendar::GetBartimes(seconds interval, time_point start, time_point end)
//{
//}
//
//std::vector<sec_t> Calendar::GetBartimes(seconds interval, time_point start, int count)
//{
//}
//
//inline sec_t Calendar::GetCurrentBartime(time_point dt, seconds interval)
//{
//}
//
//const SpecialSessions &Calendar::GetSpecialSessions(time_point dt) const
//{
//}
//
//const session_t Calendar::GetOpenCloseDT(sec_t dt) const
//{
//}
//
//const session_t Calendar::GetSessionDT(sec_t dt) const
//{
//}
//const void Calendar::GetOrderedSessions() const
//{
//}
//const void Calendar::GetOpenCloseTime() const
//{
//}
//bool Calendar::IsTrading(time_point dt) const
//{
//}
//bool Calendar::IsTradingDay(time_point dt) const
//{
//}
//bool Calendar::IsTradingTime(time_point dt) const
//{
//}

#pragma endregion

#pragma region CalendarAstock
CalendarData CalendarAstock::_calendar_data("Asia/Shanghai");
#pragma endregion

NS_QMC_END