import re
from datetime import datetime, time, timedelta
from enum import Enum
from functools import cache
from zoneinfo import ZoneInfo

import quantdata as qd

from .calendar import (
    DB_NAME_CALENDAR,
    I1H,
    I2H,
    I3H,
    I4H,
    MongoDBCalendar,
    SpecialSession,
)

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


class CalendarCTP(MongoDBCalendar):
    """
    中国期货日历
    """

    COLLECTION_NAME = "cn_future"
    COLLECTION_NAME_SESSIONS = "cn_future_sessions"
    sessions = ((75600, 9000), (32400, 54900))
    tz = ZoneInfo("Asia/Shanghai")
    # 为防止边界条件，多加1微妙
    offset = timedelta(hours=2, minutes=30, microseconds=1)
    # 1m - 3m - 5m - 10m - 15m - 30m - 1H - 2H - 3H - 4H
    intervals = (60, 180, 300, 600, 900, 1800, I1H, I2H, I3H, I4H)

    def __init__(self, mongo_client):
        super().__init__(mongo_client)
        # 特殊规则：交易日夜盘不开盘。第二天是节假日，夜盘不交易
        last_status = None
        lastdt = None
        for tradedt, status in self._trade_status.items():
            if last_status is not None:
                if last_status == 1:
                    if status == 3:  # 今天节假日，昨天夜盘不交易
                        self.special_sessions[lastdt] = _specialses_before_holiday(self)
                elif last_status == 3:
                    if status == 1:  # 昨天节假日，今日上午算开盘
                        self.special_sessions[tradedt] = _specialses_after_holiday(self)
            last_status = status
            lastdt = tradedt

        self.product_id = None
        self.product_type = None
        products = qd.mongo_get_data(
            mongo_client[DB_NAME_CALENDAR], self.COLLECTION_NAME_SESSIONS
        )
        for prod in products:
            product_id = prod["_id"].upper()
            cal = self.add(
                product_id,
                sessions=prod["market_time"],
                product_id=product_id,
                product_type=_get_product_type(product_id),
            )
            # 针对每个不同品种计算特殊规则：交易日夜盘不开盘。第二天是节假日，夜盘不交易
            if cal.product_type == ProductType.Commodity:
                for tradedt, special_ses in self.special_sessions.items():
                    if special_ses.name == 1:
                        cal.special_sessions[tradedt] = _specialses_before_holiday(cal)
                    else:
                        cal.special_sessions[tradedt] = _specialses_after_holiday(cal)

    def init(self):
        super().init()
        self.special_sessions = {}
        self._sorted_session_time_without_night = tuple(
            s for s in self._sorted_session_time if s[0] != time(21)
        )

    def get(self, symbol: str = None):
        return super().get(_convert_symbol(symbol))

    def __str__(self):
        product_type = self.product_type.name if self.product_type else ""
        return f"品种: {self.product_id}\n类型: {product_type}" + super().__str__()


@cache
def _convert_symbol(symbol: str):
    if symbol:
        ret = re.match(r"([a-zA-Z]{1,2})([\d]{3,4})", symbol)  # 国内期货
        if ret:
            return ret.group(1).upper()
        else:
            return symbol.upper()
    return symbol


def _specialses_before_holiday(cal):
    return SpecialSession(
        name=1,
        open_close_sessions=((None, cal._session_time[-1][-1]),),  # 删除夜盘开盘时间
        ordered_sessions=cal._sorted_session_time_without_night,
    )


def _specialses_after_holiday(cal):
    return SpecialSession(
        name=2,
        open_close_sessions=(
            (time(9), None),  # 额外添加一个开盘时间
            cal._open_close_sessions[0],
        ),
        ordered_sessions=cal._sorted_session_time,
    )


if __name__ == "__main__":
    import quantdata as qd

    with qd.mongo_connect("127.0.0.1") as conn:
        print("connect mongodb")
        cal = CalendarCTP(conn)
        print(cal)

        print(cal.get("ag"))
        # print(cal.get("t"))
        # print(cal.get("ih"))
        # print(cal.get("c"))
        # print(cal.get("ec"))
        # print(cal.get("bc"))
        # print(cal.get("ag").get_bartimes(30 * 86400, datetime(2024, 9, 13), count=20))

    print("disconnect mongodb")
