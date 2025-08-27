#include <iostream>
#include <vector>
#include <algorithm>
#include <assert.h>
#include <cstdlib>

#include "quantcalendar/dates.h"
#include "quantcalendar/calendar.h"
#include "quantcalendar/datetime.h"

extern const std::vector<qmc::date_status_item> cn_stock;

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

  const qmc::CalendarAstock &astock_cal = qmc::CalendarAstock::GetInstance();
  auto const &ctp_cal = qmc::CalendarCTP::GetInstance("ag2405");
  auto const &ctp_cal1 = qmc::CalendarCTP::GetInstance("IH");
  auto const &cal7x24 = qmc::Time7x24Calendar::GetInstance();

  auto tp = qmc::fromisoformat("2025-08-27 14:55:00");
  auto bartime = ctp_cal1.GetCurrentBartime(std::chrono::seconds(300), tp);
  auto bartime1 = ctp_cal1.GetCurrentBartime(std::chrono::seconds(300), tp+system_clock::duration(1));
  assert(bartime1 - bartime == 300);
}