#include <iostream>
#include <vector>
#include <algorithm>
#include <assert.h>

#include "quantcalendar/calendar_data.h"
#include "quantdata/mongodb.h"

const char *host = "localhost";

int main(int argv, char *args[])
{
  qmc::CalendarData calendar_data("Asia/Shanghai");
  MongoConnect(host);
  auto cursor = MongoGetTradeCal("quantcalendar", "cn_stock");
  auto results = MongoFetchArrays<qmc::sec_t, uint8_t>(std::move(cursor), [](const document::view &view)
                                                       { return std::tuple{duration_cast<seconds>(view["_id"].get_date().value).count(), static_cast<uint8_t>(view["status"].get_int32().value)}; });
  calendar_data.InitData(results);
  auto iter = calendar_data.TradedaysUpper(Datetime(2023, 6, 30).to_timestamp());
  auto iter2 = calendar_data.TradedaysLower(Datetime(2023, 6, 30).to_timestamp());
  assert((*iter).second.IsTrading());
  assert((*iter).second.dt_ == Datetime(2023, 6, 30));
  assert((*iter).second.dt_ == (*iter2).second.dt_);

  try
  {
    auto iter3 = calendar_data.TradedaysUpper(Datetime(2023, 6, 30, 12).to_timestamp());
    assert(false);
  }
  catch (qmc::OutOfCalendar &e)
  {
  }
  auto iter4 = calendar_data.TradedaysUpper(Datetime(2024, 12, 31).to_timestamp());
  try
  {
    ++iter4;
    assert(false);
  }
  catch (qmc::OutOfCalendar &e)
  {
  }
  auto iter5 = calendar_data.TradedaysLower(Datetime(1990, 12, 19).to_timestamp());
  try
  {
    --iter5;
    assert(false);
  }
  catch (qmc::OutOfCalendar& e)
  {
  }

  MongoClose();
}