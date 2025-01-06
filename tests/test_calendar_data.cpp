#include <iostream>
#include <vector>
#include <algorithm>
#include <assert.h>
#include <cstdlib>

#include "quantcalendar/calendar_data.h"
#include "quantcalendar/calendar.h"
#include "quantdata/mongodb.h"

const char *host = "localhost";

int main(int argv, char *args[])
{
  qmc::CalendarData calendar_data;
  MongoConnect(host);
  std::atexit(MongoClose);
  auto cursor = MongoGetTradeCal("quantcalendar", "cn_stock");
  auto results = MongoFetchArrays<qmc::sec_t, char>(std::move(cursor), [](const document::view &view)
                                                    { return std::tuple{duration_cast<seconds>(view["_id"].get_date().value).count(), static_cast<char>(view["status"].get_int32().value)}; });
  calendar_data.InitData(results);
  auto iter = calendar_data.TradedaysUpper(Datetime(2023, 6, 30).to_timestamp());
  auto iter2 = calendar_data.TradedaysLower(Datetime(2023, 6, 30).to_timestamp());
  assert((*iter).second.IsTrading());
  assert((*iter).second.dt_ == Datetime(2023, 6, 30));
  assert((*iter).second.dt_ == (*iter2).second.dt_);

  auto iter3 = calendar_data.TradedaysUpper(Datetime(2023, 6, 30, 12).to_timestamp());
  assert(iter3.is_end());

  auto iter4 = calendar_data.TradedaysUpper(Datetime(2024, 12, 31).to_timestamp());
  ++iter4;
  assert(iter4.is_end());

  auto iter5 = calendar_data.TradedaysLower(Datetime(1990, 12, 19).to_timestamp());
  --iter5;
  assert(iter5.is_end());

  qmc::CalendarAstock::InitData(results);
  const qmc::CalendarAstock &astock_cal = qmc::CalendarAstock::GetInstance();
}