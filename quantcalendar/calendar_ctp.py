import copy
import re
from datetime import datetime, timedelta
from enum import Enum
from functools import cache
from zoneinfo import ZoneInfo

import quantdata as qd

from .calendar import DB_NAME_CALENDAR, MongoDBCalendar
from .common import IntervalType
from .next_bartime_utils import *

__all__ = ["CalendarCTP"]


class ProductType(Enum):
    Commodity = 0  # 大宗商品
    StockIndex = 1  # 股指
    Bonds = 2  # 债券期货


def _get_product_type(product_id):
    if product_id in ("T", "TS", "TF", "TL"):
        return ProductType.Bonds
    elif product_id in ("IC", "IH", "IF", "IM"):
        return ProductType.StockIndex
    else:
        return ProductType.Commodity


_microsecond = datetime.timedelta(microseconds=1)


class CalendarCTP(MongoDBCalendar):
    """
    中国期货日历
    """

    COLLECTION_NAME = "cn_future"
    COLLECTION_NAME_SESSIONS = "cn_future_session_details"
    session = (75600, 54900)
    session_details = ((32400, 54900), (75600, 9000))
    tz = ZoneInfo("Asia/Shanghai")
    offset = timedelta(hours=2, minutes=30)

    def __init__(self, mongo_client):
        super().__init__(mongo_client)
        self.product_id = None
        self.product_type = None
        products = qd.mongo_get_data(
            mongo_client[DB_NAME_CALENDAR], self.COLLECTION_NAME_SESSIONS
        )
        for prod in products:
            product_id = prod["_id"].upper()
            self._sub_calendars[product_id] = cal = copy.copy(self)
            cal.product_id = product_id
            cal.product_type = _get_product_type(product_id)
            cal.session_details = prod["market_time"]
            # 收盘时间 15:00 15:15
            close_tm = None
            for _, c in cal.session_details:
                if c >= 54000 and c <= 54900:  #
                    close_tm = c
                    break
            cal.session = (75600, close_tm)

    def get(self, symbol: str = None):
        return super().get(_convert_symbol(symbol))

    def _tradeday_is_trade(self, dt: datetime) -> bool:
        """
        特殊规则：交易日 也有不交易的时段。比如期货第二天是节假日，夜盘不交易
        """
        # 15:15:00 收盘
        tomorrow = (
            dt
            + timedelta(hours=8, minutes=44, seconds=59, microseconds=999)
            + self.offset
        )

        next_trading_state = self._trade_status[tomorrow.date().isoformat()]
        return next_trading_state != 3

    def is_trading_day(self, dt):
        today = dt - self.offset
        trading_state = self._trade_status[today.date().isoformat()]
        return trading_state == 1

    def __str__(self):
        product_type = self.product_type.name if self.product_type else ""
        return f"品种: {self.product_id}\n类型: {product_type}" + super().__str__()

    def get_next_bartime(
        self, dt: datetime, interval: int, interval_type: IntervalType
    ):
        """获取K线的结束时间"""
        product_type = self.product_type
        ticktime = dt
        if is_opening_time(ticktime, self.session_details):
            # 开盘时间要加一个微小的时间差，以便算入当前的K线
            ticktime += _microsecond
        if interval_type == IntervalType.SECOND:
            return get_next_second_bartime(ticktime, interval)

        elif interval_type == IntervalType.MINUTE:
            if product_type == ProductType.Commodity:
                if interval == 30:
                    special_times = (36000, 38700, 40500, 49500, 51300, 53100, 54000)
                    _ticktime = self._get_next_special_time(ticktime, special_times)
                    if _ticktime is not None:
                        return _ticktime
                return get_next_minute_bartime(ticktime, interval)
            elif product_type == ProductType.StockIndex:
                return get_next_minute_bartime(ticktime, interval)
            elif product_type == ProductType.Bonds:
                special_times = (54000, 54900)
                _ticktime = self._get_next_special_time(ticktime, special_times)
                return (
                    get_next_minute_bartime(ticktime, interval)
                    if _ticktime is None
                    else _ticktime
                )

        elif interval_type == IntervalType.HOUR:
            if product_type == ProductType.Commodity:
                special_times = _ctpHourTimes(self.session_details, interval)
                crossdaytime = _ctpCrossTimes(self.session_details, interval)
                return self._get_next_special_time(
                    ticktime, special_times, crossdaytime=crossdaytime
                )
            elif product_type == ProductType.StockIndex:
                if interval == 1:
                    special_times = (34200, 37800, 41400, 50400, 54000)
                elif interval == 2:
                    special_times = (34200, 41400, 54000)
                return self._get_next_special_time(ticktime, special_times)
            elif product_type == ProductType.Bonds:
                if interval == 1:
                    special_times = (34200, 37800, 41400, 50400, 54000, 54900)
                elif interval == 2:
                    special_times = (34200, 41400, 54000, 54900)
                return self._get_next_special_time(ticktime, special_times)

        elif interval_type == IntervalType.DAILY:
            if interval > 1:
                raise ValueError("Day interval only support 1 day")
            if product_type == ProductType.Commodity:
                special_times = (-1, 54000, 140400)
            elif product_type == ProductType.StockIndex:
                special_times = (34200, 54000)
            elif product_type == ProductType.Bonds:
                special_times = (34200, 54900)
            ticktime = self._get_next_special_time(ticktime, special_times)
            ticktime = self._time_until_openday(ticktime)
            return ticktime

        elif interval_type == IntervalType.WEEKLY:
            if interval > 1:
                raise ValueError("Week interval only support 1 week")
            if product_type == ProductType.Commodity:
                special_times = (-1, 54000, 140400)
            elif product_type == ProductType.StockIndex:
                special_times = (34200, 54000)
            elif product_type == ProductType.Bonds:
                special_times = (34200, 54900)
            ticktime = self._get_next_special_time(ticktime, special_times)
            ticktime = self._time_until_openday(ticktime)
            ticktime = self._to_weekend(ticktime)
            return ticktime
        return None

    def _get_next_special_time(self, ticktime, special_times, crossdaytime=0):
        seconds = (
            ticktime.hour * 3600
            + ticktime.minute * 60
            + ticktime.second
            + ticktime.microsecond * 0.000001
        )
        if seconds > special_times[0] and seconds <= special_times[-1]:
            for t in special_times:
                if seconds <= t:
                    _t = t
                    if t >= 86400:
                        _t = t - 86400
                        ticktime = ticktime + datetime.timedelta(days=1)
                    if t == crossdaytime:
                        ticktime = self._time_until_openday(ticktime)
                    h = int(_t / 3600)
                    m = int((_t - h * 3600) / 60)
                    return ticktime.replace(hour=h, minute=m, second=0, microsecond=0)
        return None

    def _to_weekend(self, ticktime):
        _weekday = ticktime.isocalendar()[2]
        for i in [5, 4, 3, 2, 1]:
            if i == _weekday:
                return ticktime
            else:
                _ticktime = ticktime + datetime.timedelta(days=(i - _weekday))
                if self.is_trading_day(_ticktime):
                    return _ticktime

    def _time_until_openday(self, ticktime):
        count = 0
        while not self.is_trading_day(ticktime):
            ticktime = ticktime + datetime.timedelta(days=1)
            count += 1
            if count > 15:
                ticktime = ticktime - datetime.timedelta(days=15)
                print(
                    f"bartime_generator: 往后推15天也没有交易日，交易日历没更新，请及时更新"
                )
                break
        return ticktime


@cache
def _convert_symbol(symbol: str):
    if symbol:
        ret = re.match(r"([a-zA-Z]{1,2})([\d]{3,4})", symbol)  # 国内期货
        if ret:
            return ret.group(1).upper()
        else:
            return symbol.upper()
    return symbol


def _ctpHourTimes(timeperiod, interval):
    if len(timeperiod) == 3:  # 无夜盘品种
        if interval == 1:
            return (32400, 36000, 40500, 51300, 54000)  # 10:00 11:15 14:15 15:00
        elif interval == 2:
            return (32400, 40500, 54000)  # 11:15 15:00
        elif interval == 3:
            return (32400, 51300, 54000)  # 14:15 15:00
        elif interval == 4:
            return (32400, 54000)  # 15:00
    elif timeperiod[-1][1] == 82800:  # 23:00
        if interval == 1:
            return (
                32400,
                36000,
                40500,
                51300,
                54000,
                79200,
                82800,
            )  # 10:00 11:15 14:15 15:00 22:00 23:00
        elif interval == 2:
            return (32400, 40500, 54000, 82800)  # 11:15 15:00 23:00
        elif interval == 3:
            return (32400, 36000, 54000, 122400)  # 10:00 15:00 (21:00-10:00可能跨天)
        elif interval == 4:
            return (32400, 40500, 54000, 126900)  # 11:15 15:00 (21:00-11:15可能跨天)
    elif timeperiod[-1][1] == 3600:  # 01:00
        if interval == 1:
            return (
                -1,
                3600,
                36000,
                40500,
                51300,
                54000,
                79200,
                82800,
                86400,
            )  # 10:00 11:15 14:15 15:00 22:00 23:00, 00:00 01:00
        elif interval == 2:
            return (-1, 3600, 40500, 54000, 82800, 90000)  # 11:15 15:00 23:00 01:00
        elif interval == 3:
            return (-1, 40500, 54000, 86400)  # 11:15 15:00 00:00 (00:00-11:15可能跨天)
        elif interval == 4:
            return (-1, 3600, 54000, 90000)  # 01:00 15:00
    elif timeperiod[-1][1] == 9000:  # 02:30
        if interval == 1:
            return (
                -1,
                3600,
                7200,
                34200,
                38700,
                49500,
                53100,
                54000,
                79200,
                82800,
                86400,
            )  # 09:30 10:45 13:45 14:45 15:00 22:00 23:00, 00:00 01:00 02:00 (02:00-09:30可能跨天)
        elif interval == 2:
            return (
                -1,
                3600,
                34200,
                49500,
                54000,
                82800,
                90000,
            )  # 09:30 13:45 15:00 23:00 01:00 (01:00-09:30可能跨天)
        elif interval == 3:
            return (
                -1,
                34200,
                53100,
                54000,
                86400,
            )  # 09:30 14:45 15:00 00:00 (00:00-09:30可能跨天)
        elif interval == 4:
            return (
                -1,
                3600,
                49500,
                54000,
                90000,
            )  # 01:00 13:45 15:00 (01:00-13:45可能跨天)


def _ctpCrossTimes(timeperiod, interval):  # 可能跨天的时间段
    if timeperiod[-1][1] == 82800:  # 23:00
        if interval == 3:
            return 122400
        elif interval == 4:
            return 126900
    elif timeperiod[-1][1] == 3600:  # 01:00
        if interval == 3:
            return 40500  # (00:00-11:15可能跨天)
    elif timeperiod[-1][1] == 9000:  # 02:30
        if interval == 4:
            return 49500
        else:
            return 34200
    return 0


if __name__ == "__main__":
    import quantdata as qd

    with qd.mongo_connect("127.0.0.1") as conn:
        print("connect mongodb")
        cal = CalendarCTP(conn)
        print(cal)

        print(cal.get("ag"))
        print(cal.get("t"))
        print(cal.get("ih"))
        print(cal.get("c"))
        print(cal.get("ec"))
        print(cal.get("bc"))

    print("disconnect mongodb")
