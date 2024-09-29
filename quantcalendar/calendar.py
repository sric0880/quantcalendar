from abc import ABC, abstractmethod
from datetime import datetime, time, timedelta
from typing import Iterable, List, Tuple

import quantdata as qd

from .common import IntervalType
from .next_bartime_utils import get_next_bartime

__all__ = ["Calendar", "MongoDBCalendar", "DB_NAME_CALENDAR"]


DB_NAME_CALENDAR = "quantcalendar"


class Calendar(ABC):
    """
    交易日历
    """

    session = ()
    """ (开盘, 收盘)时间, 按当天秒数来算 eg. (34200,54000) """
    session_details = ()
    """ 开盘-收盘时间(包括中间的休息时间) eg. ((32400, 36900), (37800, 41400), (48600, 54000), (75600, 82800))"""
    tz = None
    """ 时区"""
    offset = timedelta()
    """ 有些市场交易时间会跨越凌晨0点, offset表示超过0点的时间差"""

    def __init__(self):
        # 每个交易品种对应不同的交易日历
        self._sub_calendars = {}

    def get(self, symbol: str = None):
        """根据不同的证券品种获取不同的日历"""
        if not symbol or not self._sub_calendars:
            return self
        else:
            return self._sub_calendars.get(symbol, self)

    @abstractmethod
    def get_tradedays_gte(self, dt: datetime) -> List[datetime]:
        """get trade days >= dt"""
        pass

    @abstractmethod
    def get_tradedays_lte(self, dt: datetime) -> List[datetime]:
        """get trade days <= dt"""
        pass

    def get_next_bartime(
        self, dt: datetime, interval: int, interval_type: IntervalType
    ):
        """获取K线的结束时间"""
        sos, eos = self.session
        return get_next_bartime(
            dt,
            interval,
            interval_type,
            opentime=timedelta(seconds=sos),
            closetime=seconds_to_time(eos),
        )

    def get_session_dt(self, dt: datetime) -> Tuple[datetime, datetime]:
        """
        给定时间dt, 获取下一次(开盘, 收盘)时间。
        """
        dt = dt - self.offset
        sos, eos = self.get_session_time()
        next_sos_dt = next_eos_dt = None
        for day in self.get_tradedays_gte(dt):
            if next_sos_dt is None:
                sos_dt = datetime.combine(day.date(), sos) - self.offset
                if dt < sos_dt:
                    next_sos_dt = sos_dt
            if next_eos_dt is None:
                eos_dt = datetime.combine(day.date(), eos) - self.offset
                if dt < eos_dt:
                    next_eos_dt = eos_dt
            if next_sos_dt is not None and next_eos_dt is not None:
                break
        return (next_sos_dt + self.offset, next_eos_dt + self.offset)

    def get_session_detail_dt(self, dt: datetime) -> Tuple[datetime, datetime]:
        """
        给定时间dt, 获取下一次(开盘, 收盘)时间。休息时间段也算是收盘
        """
        dt = dt - self.offset
        lst = self.get_session_detail_time()
        next_sos_dt = next_eos_dt = None
        for day in self.get_tradedays_gte(dt):
            for sos, eos in lst:
                if next_sos_dt is None:
                    offset = (
                        self.offset
                        if time_to_seconds(sos) > self.offset.total_seconds()
                        else self.offset - timedelta(days=1)
                    )
                    sos_dt = datetime.combine(day.date(), sos) - offset
                    if dt < sos_dt and self._tradeday_is_trade(sos_dt):
                        next_sos_dt = sos_dt
                if next_eos_dt is None:
                    offset = (
                        self.offset
                        if time_to_seconds(eos) > self.offset.total_seconds()
                        else self.offset - timedelta(days=1)
                    )
                    eos_dt = datetime.combine(day.date(), eos) - offset
                    if dt <= eos_dt and self._tradeday_is_trade(eos_dt):
                        next_eos_dt = eos_dt
            if next_sos_dt is not None and next_eos_dt is not None:
                break
        if next_sos_dt is None or next_eos_dt is None:
            raise RuntimeError("日历数据到期了，请更新日历")
        return (next_sos_dt + self.offset, next_eos_dt + self.offset)

    def _tradeday_is_trade(self, dt: datetime) -> bool:
        """
        特殊规则：交易日 也有不交易的时段。比如期货第二天是节假日，夜盘不交易
        """
        return True

    def get_session_time(self) -> Tuple[time, time]:
        """
        Return:

            eg. (time(9,30,0), time(15,0,0))
        """
        sos, eos = self.session
        return seconds_to_time(sos), seconds_to_time(eos)

    def get_session_detail_time(self) -> Iterable[Tuple[time, time]]:
        """
        Return:

            eg: [(time(9,30,0), time(11,30,0)), (time(13,0,0), time(15,0,0)), ...]
        """
        return [
            (seconds_to_time(sos), seconds_to_time(eos))
            for sos, eos in self.session_details
        ]

    def is_trading(self, dt: datetime):
        """
        判断当前时间是否正在交易中, dt时间必须是交易所本地时间
        """
        if dt.tzinfo is not None:
            dt = dt.replace(tzinfo=None)
        sos_dt, eos_dt = self.get_session_detail_dt(dt)
        return eos_dt < sos_dt  # 先收盘 再开盘


class Time7x24Calendar(Calendar):
    """
    7 x 24小时不间断交易，比如数字货币
    开盘和收盘时间都是凌晨0点

    如果需要以开盘或者收盘设置定时任务，只需以其一为锚点
    """

    session = (0, 86400)
    session_details = ((0, 86400),)

    def get_tradedays_gte(self, dt: datetime) -> List[datetime]:
        """get trade days >= dt 取一年时间"""
        start_dt = datetime.combine(dt.date(), time(0, 0, 0), tzinfo=dt.tzinfo)
        return [start_dt + timedelta(days=i) for i in range(365)]

    def get_tradedays_lte(self, dt: datetime) -> List[datetime]:
        """get trade days <= dt 取一年时间"""
        start_dt = datetime.combine(dt.date(), time(0, 0, 0), tzinfo=dt.tzinfo)
        return [start_dt + timedelta(days=i) for i in range(-364, 1, 1)]

    def get_next_bartime(
        self, dt: datetime, interval: int, interval_type: IntervalType
    ):
        """获取K线的结束时间"""
        return get_next_bartime(dt, interval, interval_type)

    def is_trading(self, dt: datetime):
        return True


class MongoDBCalendar(Calendar):
    COLLECTION_NAME = ""

    def __init__(self, mongo_client):
        """加载 tradecal 和 session"""
        super().__init__()
        days = qd.mongo_get_data(mongo_client[DB_NAME_CALENDAR], self.COLLECTION_NAME)
        self._tradedays: list = []
        # 为了加速`get_tradedays_gte`和`get_tradedays_lte`的执行
        self._tradedays_indexers: dict = {}
        self._trade_status = {}

        for day in days:
            dt = day["_id"]
            status = day["status"]
            strdt = dt.date().isoformat()
            self._trade_status[strdt] = status
            _index = len(self._tradedays)
            if status == 1:
                self._tradedays.append(dt)
                self._tradedays_indexers[strdt] = (_index, _index)
            else:
                self._tradedays_indexers[strdt] = (_index - 1, _index)

    def get_tradedays_gte(self, dt: datetime) -> List[datetime]:
        """get trade days >= dt"""
        indexers = self._tradedays_indexers.get(dt.date().isoformat(), None)
        if indexers is None:
            return []
        else:
            return self._tradedays[indexers[1] :]

    def get_tradedays_lte(self, dt: datetime) -> List[datetime]:
        """get trade days <= dt"""
        indexers = self._tradedays_indexers.get(dt.date().isoformat(), None)
        if indexers is None:
            return []
        else:
            return self._tradedays[: indexers[0] + 1]


def seconds_to_time(seconds):
    if seconds == 86400:
        return time(0, 0, 0)
    result = []
    for count in (3600, 60, 1):
        value = seconds // count
        seconds -= value * count
        result.append(value)
    return time(*result)


def time_to_seconds(tm: time):
    return tm.hour * 3600 + tm.minute * 60 + tm.second
