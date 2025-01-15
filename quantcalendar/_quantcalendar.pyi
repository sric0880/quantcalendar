# fmt: off
class PyCalendar:
    def get_tradedays_gte(self, dt: int, count: int = 2**32-1) -> list[int]:
        """
        get `count` trade days which >= `dt`
        """

    def get_tradedays_lte(self, dt: int, count: int = 2**32-1) -> list[int]:
        """
        get `count` trade days which <= `dt`
        """

    def get_tradedays_between(self, start: int, end: int) -> list[int]:
        """
        get trade days which are between [`start`, `end`]
        """

    def get_tradedays_count(self, start: int, end: int) -> int:
        """
        get the count of trade days that are between [`start`, `end`]
        """

    def get_tradeday_next(self, dt: int) -> int:
        """get next trade day, include `dt`"""

    def get_tradeday_last(self, dt: int) -> int:
        """get last trade day, include `dt`"""

    def get_month_ends_gte(self, dt: int, count: int = 2**32-1) -> list[int]:
        """
        get `count` trade month end days which >= `dt`
        """

    def get_month_ends_lte(self, dt: int, count: int = 2**32-1) -> list[int]:
        """
        get `count` trade month end days which <= `dt`
        """

    def get_month_ends_between(self, start: int, end: int) -> list[int]:
        """
        get month ends which are between [`start`, `end`]
        """

    def get_month_end_next(self, dt: int)  -> int:
        """get next month end day, include `dt`"""

    def get_month_end_last(self, dt: int)  -> int:
        """get last month end day, include `dt`"""

    def get_month_begins_gte(self, dt: int, count: int = 2**32-1) -> list[int]:
        """
        get `count` trade month begin days which >= `dt`
        """

    def get_month_begins_lte(self, dt: int, count: int = 2**32-1) -> list[int]:
        """
        get `count` trade month begin days which <= `dt`
        """

    def get_month_begins_between(self, start: int, end: int = None) -> list[int]:
        """
        get month begins which are between [`start`, `end`]
        """

    def get_month_begin_next(self, dt: int)  -> int:
        """get next month begin day, include `dt`"""

    def get_month_begin_last(self, dt: int)  -> int:
        """get last month begin day, include `dt`"""

    def get_week_ends_gte(self, dt: int, count: int = 2**32-1) -> list[int]:
        """
        get `count` trade week end days which >= `dt`
        """

    def get_week_ends_lte(self, dt: int, count: int = 2**32-1) -> list[int]:
        """
        get `count` trade week end days which <= `dt`
        """

    def get_week_ends_between(self, start: int, end: int = None) -> list[int]:
        """
        get week ends which are between [`start`, `end`]
        """

    def get_week_end_next(self, dt: int)  -> int:
        """get next week end day, include `dt`"""

    def get_week_end_last(self, dt: int)  -> int:
        """get last week end day, include `dt`"""

    def get_week_begins_gte(self, dt: int, count: int = 2**32-1) -> list[int]:
        """
        get `count` trade week begin days which >= `dt`
        """

    def get_week_begins_lte(self, dt: int, count: int = 2**32-1) -> list[int]:
        """
        get `count` trade week begin days which <= `dt`
        """

    def get_week_begins_between(self, start: int, end: int = None) -> list[int]:
        """
        get week begins which are between [`start`, `end`]
        """

    def get_week_begin_next(self, dt: int)  -> int:
        """get next week begin day, include `dt`"""

    def get_week_begin_last(self, dt: int)  -> int:
        """get last week begin day, include `dt`"""

    def get_week_days_gte(self, weekday: int, dt: int, count: int = 2**32-1) -> list[int]:
        """
        get `count` trade week days of `weekday` which >= `dt`

        Params:
         - weekday: monday to sunday [1, 7]
        """

    def get_week_days_lte(self, weekday: int, dt: int, count: int = 2**32-1) -> list[int]:
        """
        get `count` trade week days of `weekday` which <= `dt`

        Params:
         - weekday: monday to sunday [1, 7]
        """

    def get_week_days_between(self, weekday: int, start: int, end: int = None) -> list[int]:
        """
        get week days of `weekday` which are between [`start`, `end`]

        Params:
         - weekday: monday to sunday [1, 7]
        """

    def get_week_day_next(self, weekday: int, dt: int)  -> int:
        """get next week day of `weekday`, include `dt`

        Params:
         - weekday: monday to sunday [1, 7]
        """

    def get_week_day_last(self, weekday: int, dt: int)  -> int:
        """get last week day of `weekday`, include `dt`

        Params:
         - weekday: monday to sunday [1, 7]
         """

    def get_bartime_next(self, interval: int, dt: float) -> int:
        """ get current bartime, include `dt`

        Params:
         * dt: timestamp
         * interval: interval seconds of bars
        """

    def get_bartimes_gte(self, interval: int, start: float, count: int=2**32-1) -> list[int]:
        """ get `count` bartimes that are >= `start`

        Params:
         * interval: interval seconds of bars
         * start: start timestamp
         * count: max count to get
        """

    def get_bartimes_between(self, interval: int, start: float, end: float) -> list[int]:
        """ get bartimes that are all in [`start`, `end`)

        Params:
         * interval: interval seconds of bars
         * start: start timestamp
         * end: end timestamp
        """

    def get_next_open_close(self, dt: float) -> tuple[int, int]:
        """ 给定时间`dt`, 获取下一次(开盘, 收盘)时间。休息时间不算是收盘，每天只有一次开盘收盘时间。

        Params:
         - dt 当前时间

        Return:
         - pair(开盘, 收盘)时间
        """

    def get_next_session(self, dt: float) -> tuple[int, int]:
        """ 给定时间`dt`, 获取下一次(开盘, 收盘)。休息时间段也算是收盘

        Params:
         - dt: 当前时间
        Return:
         - pair(开盘, 收盘)时间
        """

    def get_sessions(self) -> list[tuple[int, int]]:
        """ 返回交易时间段 """

    def get_ordered_sessions(self) -> list[tuple[int, int]]:
        """ 返回交易时间段(按开盘时间从小到大排序) """

    def get_open_close_time(self) -> tuple[int, int]:
        """ 返回开盘收盘时间 """

    def get_timezone(self) -> str:
        """ 返回时区 """

    def get_intervals(self) -> list[int]:
        """ 返回K线间隔 """

    def is_trading(self, dt: float) -> bool:
        """ 判断时间`dt`是否正在交易中, `dt`时间必须是交易所本地时间
        """

    def is_trading_day(self, dt: float) -> bool:
        """
        判断是否交易日。如果`is_trading`返回true，那么`is_trading_day`必然返回true，反过来不一定成立。
        但是当`is_trading_day`返回false，那么`is_trading`必然返回false。
        比如中国期货白银，周六凌晨1点正在交易，此时`is_trading_day`也为true，但是周六实际不是交易日。
        """

    def is_trading_time(self, dt: float) -> bool:
        """ 判断是否交易时间段，不判断是否交易，只要在时间段内，都返回True
        """

class PyCalendar_DatesArray(PyCalendar):
    """实际上并没有继承PyCalendar"""
    pass

class PyCalendar_Date7x24Array(PyCalendar):
    """实际上并没有继承PyCalendar"""
    pass

class PyCalendarAstock(PyCalendar_DatesArray):
    @staticmethod
    def Init(dates_arr):
        """"""

class PyCalendarCTP(PyCalendar_DatesArray):
    def __init__(self, symbol: str):
        """"""

    def has_night(self) -> bool:
        """是否又夜盘交易"""

    @staticmethod
    def Init(dates_arr, sessions):
        """"""

class PyTime7x24Calendar(PyCalendar_Date7x24Array):
    """7 x 24小时不间断交易，比如数字货币
    开盘和收盘时间都是凌晨0点

    如果需要以开盘或者收盘设置定时任务，只需以其一为锚点
    """
