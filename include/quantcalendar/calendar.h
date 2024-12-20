#include <ctime>
#include <stdexcept>
#include <regex>
#include <span>
#include <string>
#include <assert.h>
#include "fmt/format.h"

#include "quantcalendar/qmc_globals.h"
#include "quantdata/datetime.h"
#include "ankerl/unordered_dense.h"

using std::time_t;

NS_QMC_BEGIN

using datetime = Datetime<>;
using sec_t = datetime::precision::rep;
using cr_iter = std::vector<datetime>::const_reverse_iterator;
using c_iter = std::vector<datetime>::const_iterator;

#define I1H 3600
#define I2H 7200
#define I3H 10800
#define I4H 14400
#define DAILY 86400
#define WEEKLY 7 * 86400
#define MONTHLY 30 * 86400

constexpr int
UnitToSeconds(char u)
{
  switch (u)
  {
  case 's':
    return 1;
  case 'm':
    return 60;
  case 'h':
    return I1H;
  case 'd':
    return DAILY;
  case 'w':
    return WEEKLY;
  case 'M':
    return MONTHLY;
  default:
    throw std::invalid_argument(fmt::format("interval unit {} is not invalid", u));
  }
}

/**
 * 交易日历
 */
class Calendar
{
public:
  // get trade days >= dt
  virtual inline c_iter GetTradedaysGTE(const datetime& dt) const = 0;
  virtual inline c_iter GetTradedaysGTE(sec_t dt) const = 0;
  // get trade days <= dt
  virtual inline cr_iter GetTradedaysLTE(const datetime& dt) const = 0;
  virtual inline cr_iter GetTradedaysLTE(sec_t dt) const = 0;
  // equal to GetTradedaysGTE(dt)[0]
  virtual inline const datetime* GetTradedayNext(const datetime& dt) const = 0;
  virtual inline const datetime* GetTradedayNext(sec_t dt) const = 0;
  // equal to GetTradedaysLTE(dt)[-1]
  virtual inline const datetime* GetTradedayLast(const datetime& dt) const = 0;
  virtual inline const datetime* GetTradedayLast(sec_t dt) const = 0;
  virtual std::pair<c_iter, c_iter> GetTradedaysBetween(const datetime& start_dt, const datetime& end_dt) const = 0;
  virtual std::pair<c_iter, c_iter> GetTradedaysBetween(sec_t start_dt, sec_t end_dt) const = 0;

  /**
   * 获取K线时间
   * @param
   *  interval(int): K线间隔周期
   */
  time_t GetCurrentBartime(time_t dt, int interval);
  void GetBartimes(int interval, time_t start, time_t end, int count);

  /**
   * @return
   * all month ends >=`start`
   */
  virtual const std::vector<time_t> GetTradedaysMonthEnd(time_t start) const = 0;
  /**
   * @return
   * `count` month ends >=`start`
   */
  virtual const std::vector<time_t> GetTradedaysMonthEnd(time_t start, int count) const = 0;
  /**
   * @return
   * return all `end` >= monthends >=`start`
   */
  virtual const std::vector<time_t> GetTradedaysMonthEnd(time_t start, time_t end) const = 0;
  /**
   * @return
   * all month begins >=`start`
   */
  virtual const std::vector<time_t> GetTradedaysMonthBegin(time_t start) const = 0;
  /**
   * @return
   * `count` month begins >=`start`
   */
  virtual const std::vector<time_t> GetTradedaysMonthBegin(time_t start, int count) const = 0;
  /**
   * @return
   * return all `end` >= monthbegins >=`start`
   */
  virtual const std::vector<time_t> GetTradedaysMonthBegin(time_t start, time_t end) const = 0;
  /**
   * @return
   * all week ends >=`start`
   */
  virtual const std::vector<time_t> GetTradedaysWeekEnd(time_t start) const = 0;
  /**
   * @return
   * `count` week ends >=`start`
   */
  virtual const std::vector<time_t> GetTradedaysWeekEnd(time_t start, int count) const = 0;
  /**
   * @return
   * return all `end` >= weekends >=`start`
   */
  virtual const std::vector<time_t> GetTradedaysWeekEnd(time_t start, time_t end) const = 0;
  /**
   * @return
   * all week begins >=`start`
   */
  virtual const std::vector<time_t> GetTradedaysWeekBegin(time_t start) const = 0;
  /**
   * @return
   * `count` week begins >=`start`
   */
  virtual const std::vector<time_t> GetTradedaysWeekBegin(time_t start, int count) const = 0;
  /**
   * @return
   * return all `end` >= weekbegins >=`start`
   */
  virtual const std::vector<time_t> GetTradedaysWeekBegin(time_t start, time_t end) const = 0;
  /**
   * @return
   * return all trading `weekday` >=`start`, , weekday in [1, 7]
   */
  virtual const std::vector<time_t> GetTradedaysWeekDay(time_t start) const = 0;
  /**
   * @return
   * return `count` trading `weekday` >=`start`, weekday in [1, 7]
   */
  virtual const std::vector<time_t> GetTradedaysWeekDay(time_t start, int count) const = 0;
  /**
   * @return
   * return all `end` >= trading `weekday` >=`start`, weekday in [1, 7]
   */
  virtual const std::vector<time_t> GetTradedaysWeekDay(time_t start, time_t end) const = 0;
  // 从配置special_sessions中读取，或者重写该函数
  const void GetSpecialSessions(time_t dt) const;
  // 给定时间`dt`, 获取下一次(开盘, 收盘)。休息时间段也算是收盘
  const std::tuple<time_t, time_t> GetSessionDatetime(time_t dt) const;
  // 返回交易时间段
  const void GetSessions() const;
  // 返回交易时间段(按开盘时间从小到大排序)
  const void GetOrderedSessions() const;
  // 返回开盘收盘时间
  const void GetOpenCloseTime() const;
  // 判断时间`dt`是否正在交易中, `dt`时间必须是交易所本地时间
  bool IsTrading(time_t dt) const;
  // 判断是否交易日
  bool IsTradingDay(time_t dt) const;
  // 判断是否交易时间段，不判断是否交易，只要在时间段内，都返回True
  bool IsTradingTime(time_t dt) const;

private:
  void
  CalcBarTimestamp();
  void _calc_bartimestamp_left();
  void _calc_bartimestamp_right();
};

class DBCalendar : public Calendar
{
public:
  DBCalendar(const std::vector<std::pair<sec_t /*timestamp*/, uint8_t /*status*/>> &calendar_data);
  virtual inline c_iter GetTradedaysGTE(const datetime &dt) const override;
  virtual inline c_iter GetTradedaysGTE(sec_t dt) const override;
  virtual inline cr_iter GetTradedaysLTE(const datetime &dt) const override;
  virtual inline cr_iter GetTradedaysLTE(sec_t dt) const override;
  virtual inline const datetime *GetTradedayNext(const datetime &dt) const override;
  virtual inline const datetime *GetTradedayNext(sec_t dt) const override;
  virtual inline const datetime *GetTradedayLast(const datetime &dt) const override;
  virtual inline const datetime *GetTradedayLast(sec_t dt) const override;
  virtual std::pair<c_iter, c_iter> GetTradedaysBetween(const datetime &start_dt, const datetime &end_dt) const override;
  virtual std::pair<c_iter, c_iter> GetTradedaysBetween(sec_t start_dt, sec_t end_dt) const override;

private:
  std::vector<datetime> tradedays_;
  ankerl::unordered_dense::map<sec_t, int> tradedays_indexers_;
};

void SetMongoCalendarDBName(std::string_view dbname);
// 将15m, 1h, 4h之类的k线间隔转化成秒
int IntervalToSeconds(std::string_view interval);

NS_QMC_END