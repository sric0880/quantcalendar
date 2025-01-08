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
  std::cout << astock_cal.ToString() << std::endl;

  auto cursor1 = MongoGetData("quantcalendar", "cn_future_sessions");
  auto sessions = MongoFetchArrays<std::string, std::vector<qmc::session_t>>(std::move(cursor1),
                                                                             [](const document::view &view)
                                                                             {
                                                                               std::vector<qmc::session_t> market_time;
                                                                               for (auto &pair : view["market_time"].get_array().value)
                                                                               {
                                                                                 market_time.emplace_back(pair[0].get_int32().value, pair[1].get_int32().value);
                                                                               }
                                                                               return std::tuple{view["_id"].get_string().value, market_time};
                                                                             });
  auto cursor2 = MongoGetData("quantcalendar", "cn_future");
  auto results2 = MongoFetchArrays<qmc::sec_t, char>(std::move(cursor2), [](const document::view &view)
                                                     { return std::tuple{duration_cast<seconds>(view["_id"].get_date().value).count(), static_cast<char>(view["status"].get_int32().value)}; });
  qmc::CalendarCTP::InitData(results2, std::move(sessions));
  auto const &ctp_cal = qmc::CalendarCTP::GetInstance("ag2405");
  std::cout << ctp_cal.ToString() << std::endl;

  auto const &ctp_cal1 = qmc::CalendarCTP::GetInstance("IH");
  std::cout << ctp_cal1.ToString() << std::endl;
}