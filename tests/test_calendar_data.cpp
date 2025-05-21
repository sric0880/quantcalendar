#include <iostream>
#include <vector>
#include <algorithm>
#include <assert.h>
#include <cstdlib>

#include "quantcalendar/dates.h"
#include "quantcalendar/calendar.h"
#include "calendar_data.h"

int main(int argv, char *args[])
{
  qmc::DatesArray dates_arr;
  dates_arr.Init(cn_stock);
  auto iter = dates_arr.Upper(qmc::Datetime(2023, 6, 30).to_timestamp());
  auto iter2 = dates_arr.Lower(qmc::Datetime(2023, 6, 30).to_timestamp());
  assert((*iter).second.IsTrading());
  assert((*iter).second.dt_ == qmc::Datetime(2023, 6, 30));
  assert((*iter).second.dt_ == (*iter2).second.dt_);

  auto iter3 = dates_arr.Upper(qmc::Datetime(2023, 6, 30, 12).to_timestamp());
  assert(iter3.is_end());

  // Test the end of calendar(maybe changed when the calenar is updated)
  auto iter4 = dates_arr.Upper(qmc::Datetime(2025, 12, 31).to_timestamp());
  ++iter4;
  assert(iter4.is_end());

  auto iter5 = dates_arr.Lower(qmc::Datetime(1990, 12, 19).to_timestamp());
  --iter5;
  assert(iter5.is_end());

  qmc::CalendarAstock::Init(cn_stock);
  const qmc::CalendarAstock &astock_cal = qmc::CalendarAstock::GetInstance();
  std::cout << astock_cal.ToString() << std::endl;

  auto cpy_cn_future_sessions = cn_future_sessions;
  qmc::CalendarCTP::Init(cn_future, std::move(cpy_cn_future_sessions));
  auto const &ctp_cal = qmc::CalendarCTP::GetInstance("ag2405");
  std::cout << ctp_cal.ToString() << std::endl;

  auto const &ctp_cal1 = qmc::CalendarCTP::GetInstance("IH");
  std::cout << ctp_cal1.ToString() << std::endl;

  auto const &cal7x24 = qmc::Time7x24Calendar::GetInstance();
  std::cout << cal7x24.ToString() << std::endl;
}