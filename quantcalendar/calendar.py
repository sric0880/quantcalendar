import copy
from abc import ABC, abstractmethod
from collections import namedtuple
from datetime import datetime, time, timedelta
from typing import Iterable, List, Mapping, Tuple

import quantdata as qd

DB_NAME_CALENDAR = "quantcalendar"

I1H = 3600
I2H = 7200
I3H = 10800
I4H = 14400
DAILY = 86400
WEEKLY = 7 * 86400
MONTHLY = 30 * 86400

supproted_bartime = (DAILY, WEEKLY, MONTHLY)


class OutOfCalendar(Exception):
    def __init__(self) -> None:
        super().__init__("日历越界，请更新日历")


SpecialSessions = namedtuple(
    "SpecialSessions", ["name", "open_close_sessions", "ordered_sessions"]
)

day_offset = timedelta(days=1)
zero_offset = timedelta()


class Calendar(ABC):
    """
    交易日历
    """

    sessions = ()
    """ 开盘-收盘时间(包括中间的休息时间), 按当天秒数来算 eg. ((32400, 36900), (37800, 41400), (48600, 54000))"""
    special_sessions: Mapping[str, SpecialSessions] = {}
    """ 特殊原因提前收盘或者延迟开盘, Key为datetime.date().isoformat()"""
    tz = None
    """ 时区"""
    offset = zero_offset
    """ 有些市场交易时间会跨越凌晨0点, offset表示超过0点的时间差, 越过0点表示下一个交易日"""
    intervals = ()
    """ 支持的K线周期间隔,单位s,只支持分钟和小时 eg. (60, 300, 600) 表示 1min, 5min, 10min 的K线时间"""
    bartime_side = "right"
    """ K线时间是按`right` 结束时间 或者`left` 开始时间表示，默认结束时间

        TODO: `left`暂未实现
    """

    def __init__(self):
        # 每个交易品种对应不同的交易日历
        self._sub_calendars = {}
        # 记录每天交易状态 1-表示交易日
        self._trade_status = {}
        self._offset_seconds = self.offset.total_seconds()
        self._offset_minus_day = self.offset - day_offset
        self._bartime_side_right = self.bartime_side == "right"
        self.init()

    def init(self):
        self._session_time = [
            (seconds_to_time(sos), seconds_to_time(eos)) for sos, eos in self.sessions
        ]
        self._open_close_sessions = (
            (self._session_time[0][0], self._session_time[-1][1]),
        )
        self._sorted_session_time = sorted(self._session_time, key=lambda x: x[0])
        self._bartimestamp = self._calc_bartimestamp(self.sessions)

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

    def _calc_bartimestamp(self, sessions):
        start = sessions[0][0]
        end = sessions[-1][1]
        jumps = []
        last_eos = None
        for sos, eos in sessions:
            if sos < start:
                sos += 86400
            if eos < start:
                eos += 86400
            if last_eos is not None:
                jumps.append((last_eos, sos - last_eos))
            last_eos = eos
        if end < start:
            end += 86400
        ret = {}
        if self._bartime_side_right:
            for inte in self.intervals:
                ret[inte] = self._calc_bartimestamp_right(start, end, jumps, inte)
        else:
            for inte in self.intervals:
                ret[inte] = self._calc_bartimestamp_left(start, end, jumps, inte)
        return ret

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
        return list(map(self.__bartime_seconds_to_time, ret))

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
        return list(map(self.__bartime_seconds_to_time, ret))

    def __bartime_seconds_to_time(self, sec):
        return seconds_to_time(sec) if sec < 86400 else seconds_to_time(sec - 86400)

    def get_current_bartime(self, dt: datetime, interval: int):
        """获取K线时间

        Params:
            interval(seconds): K线间隔周期
        """
        return self.get_bartimes(interval, dt, count=1)[0]

    def _check_add_bartimes(self, lst, bt, range_end, count):
        if range_end is not None and bt >= range_end:
            return True
        lst.append(bt + self.offset)
        if count > 0 and len(lst) >= count:
            return True
        return False

    def get_bartimes(
        self, interval: int, range_start: datetime, range_end: datetime = None, count=0
    ) -> List[datetime]:
        """
        获取某段时间内所有的K线时间，含range_start，不含range_end，或者取前count个。
        range_end和count二者必须设置其一

        TODO: 还需要实现 bartime_side==left

        Params:
            interval(seconds): K线间隔周期
        """
        ret = []
        times = self._bartimestamp.get(interval, None)
        dt, start_day = self._to_offset_dt(range_start)
        if times is None:
            if interval == DAILY:
                for day in self.get_tradedays_gte(start_day):
                    close_time = self._combine_date_time(
                        day, self._open_close_sessions[0][-1]
                    )
                    if dt <= close_time:
                        if self._check_add_bartimes(ret, close_time, range_end, count):
                            return ret

            elif interval == WEEKLY:
                for close_time in self._get_bartimes(dt, start_day, _check_next_week):
                    if self._check_add_bartimes(ret, close_time, range_end, count):
                        return ret

            elif interval == MONTHLY:
                for close_time in self._get_bartimes(dt, start_day, _check_next_month):
                    if self._check_add_bartimes(ret, close_time, range_end, count):
                        return ret
            else:
                raise ValueError(f"bartime {interval} not supported")
        else:
            for day in self.get_tradedays_gte(start_day):
                sessions = self._get_sessions_with_breaks(day)
                bts = list(map(lambda x: self._combine_date_time(day, x), times))
                for sos, eos in sessions:
                    eos = self._combine_date_time(day, eos)
                    if dt > eos:
                        continue
                    sos = self._combine_date_time_sos(day, sos)
                    for bt in bts:
                        if bt >= dt and bt <= eos and bt >= sos:
                            if self._check_add_bartimes(ret, bt, range_end, count):
                                return ret
        if not ret:
            raise OutOfCalendar()
        return ret

    def _get_bartimes(self, dt, start_day, check_func):
        last_day = None
        close_time = None
        for day in self.get_tradedays_gte(start_day):
            if last_day is not None:
                if check_func(day, last_day):
                    yield close_time
            close_time = self._combine_date_time(day, self._open_close_sessions[0][-1])
            if dt > close_time:
                continue
            last_day = day

    def get_special_sessions(self, dt: datetime):
        """从配置special_sessions中读取，或者重写该函数"""
        return self.special_sessions.get(dt.date().isoformat())

    def _get_sessions_with_breaks(
        self, dt: datetime
    ) -> Iterable[Tuple[datetime, datetime]]:
        s = self.get_special_sessions(dt)
        if s is not None:
            return s.ordered_sessions
        else:
            return self._sorted_session_time

    def _get_sessions_without_breaks(
        self, dt: datetime
    ) -> Iterable[Tuple[datetime, datetime]]:
        s = self.get_special_sessions(dt)
        if s is not None:
            # 一天可能有多次开收盘时间
            return s.open_close_sessions
        else:
            # 正常一天只有一次开收盘时间
            return self._open_close_sessions

    def _to_offset_dt(self, dt: datetime):
        trading_day = dt = dt - self.offset
        if dt.time() == time.min and self._bartime_side_right:
            trading_day -= day_offset
        return dt, trading_day

    def _combine_date_time_sos(self, trading_day: datetime, tm: time):
        if tm == self._session_time[0][0]:
            # 开盘不可能跨越0点
            return datetime.combine(trading_day.date(), tm) - self.offset
        else:
            return self._combine_date_time(trading_day, tm)

    def _combine_date_time(self, trading_day: datetime, tm: time):
        time_sec = time_to_seconds(tm)
        if time_sec > self._offset_seconds:
            offset = self.offset
        elif time_sec == self._offset_seconds:
            if self._bartime_side_right:
                offset = self._offset_minus_day
            else:
                offset = self.offset
        else:
            offset = self._offset_minus_day
        return datetime.combine(trading_day.date(), tm) - offset

    def _find_next_session(self, dt: datetime, with_breaks: bool):
        dt, start_day = self._to_offset_dt(dt)
        next_sos_dt = next_eos_dt = None
        for day in self.get_tradedays_gte(start_day):
            if with_breaks:
                sessions = self._get_sessions_with_breaks(day)
            else:
                sessions = self._get_sessions_without_breaks(day)
            for sos, eos in sessions:
                if next_sos_dt is None and sos is not None:
                    sos = self._combine_date_time_sos(day, sos)
                    if dt < sos:
                        next_sos_dt = sos
                if next_eos_dt is None and eos is not None:
                    eos = self._combine_date_time(day, eos)
                    if dt <= eos:
                        next_eos_dt = eos
            if next_sos_dt is not None and next_eos_dt is not None:
                break
        if next_sos_dt is None or next_eos_dt is None:
            raise OutOfCalendar()
        return (next_sos_dt + self.offset, next_eos_dt + self.offset)

    def get_open_close_dt(self, dt: datetime) -> Tuple[datetime, datetime]:
        """给定时间`dt`, 获取下一次(开盘, 收盘)。"""
        return self._find_next_session(dt, False)

    def get_session_dt(self, dt: datetime) -> Tuple[datetime, datetime]:
        """
        给定时间`dt`, 获取下一次(开盘, 收盘)。休息时间段也算是收盘
        """
        return self._find_next_session(dt, True)

    def get_sessions(self):
        """返回交易时间段"""
        return self._session_time

    def get_open_close_time(self):
        """返回开盘收盘时间"""
        return self._open_close_sessions[0]

    def is_trading(self, dt: datetime):
        """
        判断时间`dt`是否正在交易中, `dt`时间必须是交易所本地时间
        """
        if dt.tzinfo is not None:
            dt = dt.replace(tzinfo=None)
        sos_dt, eos_dt = self.get_session_dt(dt)
        return eos_dt < sos_dt  # 先收盘 再开盘

    def is_trading_day(self, dt: datetime):
        """
        判断是否交易日
        """
        _, today = self._to_offset_dt(dt)
        return self._trade_status[today.date().isoformat()] == 1

    string_format = """
时区: {tz}
交易时间段:
{sessions}
K线时间点划分:
{bartimestamp}
"""

    def __str__(self):
        sessions = []
        i = 1
        for _sos, _eos in self._session_time:
            if _eos <= _sos:
                _eos = f"{_eos}(+1 days)"
            sessions.append(f"\t{i}) {_sos}-{_eos}")
            i += 1

        bartimestamps = []
        for k, v in self._bartimestamp.items():
            v = list(map(lambda x: x.isoformat(), v))
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
            sessions="\n".join(sessions),
            bartimestamp="\n".join(bartimestamps),
        )


class MongoDBCalendar(Calendar):
    COLLECTION_NAME = ""

    def __init__(self, mongo_client):
        super().__init__()
        days = qd.mongo_get_data(mongo_client[DB_NAME_CALENDAR], self.COLLECTION_NAME)
        self._tradedays: list = []
        # 为了加速`get_tradedays_gte`和`get_tradedays_lte`的执行
        self._tradedays_indexers: dict = {}

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


def _check_next_month(day, last_day):
    return day.month != last_day.month


def _check_next_week(day, last_day):
    return day.isocalendar().week != last_day.isocalendar().week


def seconds_to_time(seconds):
    if seconds == 86400:
        return time.min
    result = []
    for count in (3600, 60, 1):
        value = seconds // count
        seconds -= value * count
        result.append(value)
    return time(*result)


def time_to_seconds(tm: time):
    return tm.hour * 3600 + tm.minute * 60 + tm.second
