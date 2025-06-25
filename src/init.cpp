#include <vector>
#include "quantcalendar/dates.h"
#include "quantcalendar/calendar.h"

extern const std::vector<qmc::date_status_item> cn_stock;
extern const std::vector<qmc::date_status_item> cn_future;
extern const std::vector<qmc::CalendarCTP::session_item> cn_future_sessions;

NS_QMC_BEGIN
class __CalendarInit__ {
  public:
  __CalendarInit__() {
    CalendarAstock::Init(cn_stock);
    auto _cn_future_sessions = cn_future_sessions;
    CalendarCTP::Init(cn_future, std::move(_cn_future_sessions));
  }
  static __CalendarInit__ init;
};

__CalendarInit__ __CalendarInit__::init;

NS_QMC_END