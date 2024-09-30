import copy
from abc import ABC, abstractmethod
from datetime import datetime, time, timedelta
from typing import Iterable, List, Tuple

import quantdata as qd

from .next_bartime_utils import get_next_bartime

DB_NAME_CALENDAR = "quantcalendar"

I1H = 3600
I2H = 7200
I3H = 10800
I4H = 14400
DAILY = 86400
WEEKLY = 7 * 86400
MONTHLY = 30 * 86400

supproted_bartime = (DAILY, WEEKLY, MONTHLY)


class Calendar(ABC):
    """
    交易日历
    """

    session_details = ()
    """ 开盘-收盘时间(包括中间的休息时间), 按当天秒数来算 eg. ((32400, 36900), (37800, 41400), (48600, 54000))"""
    special_session_details = {}
    """ 特殊"""
    tz = None
    """ 时区"""
    offset = timedelta()
    """ 有些市场交易时间会跨越凌晨0点, offset表示超过0点的时间差, 越过0点表示下一个交易日"""
    intervals = ()
    """ 支持的K线周期间隔（单位s）,只支持分钟和小时 eg. (60, 300, 600) 表示 1min, 5min, 10min 的K线时间"""
    bartime_side = "right"
    """ K线时间是按`right` 结束时间 或者`left` 开始时间表示，默认结束时间"""

    def __init__(self):
        # 每个交易品种对应不同的交易日历
        self._sub_calendars = {}
        self.init()

    def init(self):
        start = self.session_details[0][0]
        end = self.session_details[-1][1]
        self._session_time = (seconds_to_time(start), seconds_to_time(end))
        self._session_detail_time = [
            (seconds_to_time(sos), seconds_to_time(eos))
            for sos, eos in self.session_details
        ]
        self._sorted_session_detail_time = sorted(
            self._session_detail_time, key=lambda x: x[0]
        )
        self._sorted_special_session = {}
        #############################
        # 计算K线时间
        self._bartimestamp = {}
        jumps = []
        last_eos = None
        for sos, eos in self.session_details:
            if sos < start:
                sos += 86400
            if eos < start:
                eos += 86400
            if last_eos is not None:
                jumps.append((last_eos, sos - last_eos))
            last_eos = eos
        if end < start:
            end += 86400
        if self.bartime_side == "right":
            for inte in self.intervals:
                self._bartimestamp[inte] = self._calc_bartimestamp_right(
                    start, end, jumps, inte
                )
        else:
            for inte in self.intervals:
                self._bartimestamp[inte] = self._calc_bartimestamp_left(
                    start, end, jumps, inte
                )
        ##############################

    def add(self, symbol: str, **kwargs):
        """添加不同证券品种的日历"""
        self._sub_calendars[symbol] = cal = copy.copy(self)
        for k, v in kwargs.items():
            setattr(cal, k, v)
        cal.init()
        return cal

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

    def _calc_bartimestamp_left(self, start, end, jumps, inte):
        jump_idx = 0
        jumps_len = len(jumps)
        ret = []
        while start < end:
            ret.append(start)
            start += inte
            while jump_idx < jumps_len:
                jtime, cumj = jumps[jump_idx]
                if start > jtime:
                    start += cumj
                    jump_idx += 1
                else:
                    break
        return list(map(lambda x: x if x <= 86400 else (x - 86400), ret))

    def _calc_bartimestamp_right(self, start, end, jumps, inte):
        jump_idx = 0
        jumps_len = len(jumps)
        ret = []
        while start <= end:
            start += inte
            while jump_idx < jumps_len:
                jtime, cumj = jumps[jump_idx]
                if start > jtime:
                    start += cumj
                    jump_idx += 1
                else:
                    break
            if start <= end:
                ret.append(start)
        if not ret or ret[-1] < end:
            ret.append(end)
        return list(map(lambda x: x if x <= 86400 else (x - 86400), ret))

    def get_current_bartime(self, dt: datetime, interval: int):
        """获取K线时间

        Params:
            interval: 秒 K线间隔周期
        """
        barts = self._bartimestamp.get(interval, None)
        if barts is None:
            if interval == DAILY:
                pass
            elif interval == WEEKLY:
                pass
            elif interval == MONTHLY:
                pass
            else:
                raise ValueError(f"bartime {interval} not supported")
        else:
            pass

    def _get_sessions_with_breaks(
        self, dt: datetime
    ) -> List[Tuple[datetime, datetime]]:
        special_session = self._sorted_special_session.get(dt.isoformat(), None)
        if special_session is not None:
            return special_session

    def _get_sessions_without_breaks(
        self, dt: datetime
    ) -> List[Tuple[datetime, datetime]]:
        special_session = self.special_session_details.get(dt.isoformat(), None)
        if special_session is not None:
            pass

    def get_session_dt(
        self, dt: datetime, include_breaks=False
    ) -> Tuple[datetime, datetime]:
        """
        给定时间`dt`, 获取下一次(开盘, 收盘)时间。

        Params:
            include_breaks:
                True: 休息时间段也算是收盘; False: 所有session段走完才算是收盘
        """
        dt = dt - self.offset
        next_sos_dt = next_eos_dt = None
        for day in self.get_tradedays_gte(dt):
            sessions = self._get_sessions_by_date(day, include_breaks=False)
            for sos, eos in sessions:
                if next_sos_dt is None and sos is not None and dt < sos:
                    next_sos_dt = sos
                if next_eos_dt is None and eos is not None and dt < eos:
                    next_eos_dt = eos
                if next_sos_dt is not None and next_eos_dt is not None:
                    break
            # if next_sos_dt is None:
            #     sos_dt = datetime.combine(day.date(), sos) - self.offset
            #     if dt < sos_dt:
            #         next_sos_dt = sos_dt
            # if next_eos_dt is None:
            #     eos_dt = datetime.combine(day.date(), eos) - self.offset
            #     if dt < eos_dt:
            #         next_eos_dt = eos_dt
            # if next_sos_dt is not None and next_eos_dt is not None:
            #     break
        if next_sos_dt is None or next_eos_dt is None:
            raise RuntimeError("日历数据到期了，请更新日历")
        return (next_sos_dt + self.offset, next_eos_dt + self.offset)

    # def get_session_detail_dt(self, dt: datetime) -> Tuple[datetime, datetime]:
    #     """
    #     给定时间dt, 获取下一次(开盘, 收盘)时间。休息时间段也算是收盘
    #     """
    #     dt = dt - self.offset
    #     next_sos_dt = next_eos_dt = None
    #     for day in self.get_tradedays_gte(dt):
    #         for sos, eos in self._sorted_session_detail_time:
    #             if next_sos_dt is None:
    #                 offset = (
    #                     self.offset
    #                     if time_to_seconds(sos) > self.offset.total_seconds()
    #                     else self.offset - timedelta(days=1)
    #                 )
    #                 sos_dt = datetime.combine(day.date(), sos) - offset
    #                 if dt < sos_dt and self._tradeday_is_trade(sos_dt):
    #                     next_sos_dt = sos_dt
    #             if next_eos_dt is None:
    #                 offset = (
    #                     self.offset
    #                     if time_to_seconds(eos) > self.offset.total_seconds()
    #                     else self.offset - timedelta(days=1)
    #                 )
    #                 eos_dt = datetime.combine(day.date(), eos) - offset
    #                 if dt <= eos_dt and self._tradeday_is_trade(eos_dt):
    #                     next_eos_dt = eos_dt
    #         if next_sos_dt is not None and next_eos_dt is not None:
    #             break
    #     if next_sos_dt is None or next_eos_dt is None:
    #         raise RuntimeError("日历数据到期了，请更新日历")
    #     return (next_sos_dt + self.offset, next_eos_dt + self.offset)

    def _tradeday_is_trade(self, dt: datetime) -> bool:
        """
        特殊规则：交易日 也有不交易的时段。比如期货第二天是节假日，夜盘不交易
        """
        return True

    def get_session_detail_time(self) -> Iterable[Tuple[time, time]]:
        """
        Return:

            eg: [(time(9,30,0), time(11,30,0)), (time(13,0,0), time(15,0,0)), ...]
        """
        return self._session_detail_time

    def is_trading(self, dt: datetime):
        """
        判断当前时间是否正在交易中, dt时间必须是交易所本地时间
        """
        if dt.tzinfo is not None:
            dt = dt.replace(tzinfo=None)
        sos_dt, eos_dt = self.get_session_detail_dt(dt)
        return eos_dt < sos_dt  # 先收盘 再开盘

    string_format = """
时区: {tz}
交易时间段:
{session_details}
K线时间点划分:
{bartimestamp}
"""

    def __str__(self):
        session_details = []
        i = 1
        for _sos, _eos in self._session_detail_time:
            session_details.append(f"\t{i}) {_sos}-{_eos}")
            i += 1

        bartimestamps = []
        for k, v in self._bartimestamp.items():
            v = list(map(lambda x: seconds_to_time(x).isoformat(), v))
            if len(v) > 8:
                v = f"[{v[0]},{v[1]},{v[2]},{v[3]},...{v[-4]},{v[-3]},{v[-2]},{v[-1]}]"
            k //= 60
            unit = "m"
            if k >= 60:
                k //= 60
                unit = "H"
            bartimestamps.append(f"\t{k}{unit})\t{v}")

        return self.string_format.format(
            tz=self.tz,
            session_details="\n".join(session_details),
            bartimestamp="\n".join(bartimestamps),
        )


class Time7x24Calendar(Calendar):
    """
    7 x 24小时不间断交易，比如数字货币
    开盘和收盘时间都是凌晨0点

    如果需要以开盘或者收盘设置定时任务，只需以其一为锚点
    """

    session_details = ((0, 86400),)

    def get_tradedays_gte(self, dt: datetime) -> List[datetime]:
        """get trade days >= dt 取一年时间"""
        start_dt = datetime.combine(dt.date(), time(0, 0, 0), tzinfo=dt.tzinfo)
        return [start_dt + timedelta(days=i) for i in range(365)]

    def get_tradedays_lte(self, dt: datetime) -> List[datetime]:
        """get trade days <= dt 取一年时间"""
        start_dt = datetime.combine(dt.date(), time(0, 0, 0), tzinfo=dt.tzinfo)
        return [start_dt + timedelta(days=i) for i in range(-364, 1, 1)]

    def get_next_bartime(self, dt: datetime, interval: int):
        """获取K线的结束时间"""
        return get_next_bartime(dt, interval)

    def is_trading(self, dt: datetime):
        return True


class MongoDBCalendar(Calendar):
    COLLECTION_NAME = ""

    def __init__(self, mongo_client):
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
