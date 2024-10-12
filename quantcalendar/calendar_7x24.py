from datetime import datetime, time, timedelta
from typing import List

from .calendar import I1H, I2H, I3H, I4H, Calendar


class Time7x24Calendar(Calendar):
    """
    7 x 24小时不间断交易，比如数字货币
    开盘和收盘时间都是凌晨0点

    如果需要以开盘或者收盘设置定时任务，只需以其一为锚点
    """

    sessions = ((0, 86400),)
    # 1m - 3m - 5m - 10m - 15m - 30m - 1H - 2H - 3H - 4H
    intervals = (60, 180, 300, 600, 900, 1800, I1H, I2H, I3H, I4H)
    # bartime_side = "left"

    def get_tradedays_gte(self, dt: datetime):
        """get trade days >= dt, generate from today to days after"""
        day = datetime.combine(dt.date(), time(0, 0, 0), tzinfo=dt.tzinfo)
        while True:
            yield day
            day += timedelta(days=1)

    def get_tradedays_lte(self, dt: datetime):
        """get trade days <= dt, generate from today to days before"""
        day = datetime.combine(dt.date(), time(0, 0, 0), tzinfo=dt.tzinfo)
        while True:
            yield day
            day -= timedelta(days=1)

    def get_tradedays_next(self, dt: datetime) -> datetime:
        """equal to get_tradedays_gte(dt)[0]"""
        return datetime.combine(dt.date(), time(0, 0, 0), tzinfo=dt.tzinfo)

    def get_tradedays_last(self, dt: datetime) -> datetime:
        """equal to get_tradedays_lte(dt)[-1]"""
        return datetime.combine(dt.date(), time(0, 0, 0), tzinfo=dt.tzinfo)

    def get_tradedays_month_end(self, dt: datetime, count: int) -> List[datetime]:
        if count <= 0:
            raise OverflowError("infinite generated days, count must be great than 0")
        return super().get_tradedays_month_end(dt, count)

    def get_tradedays_month_begin(self, dt: datetime, count: int) -> List[datetime]:
        if count <= 0:
            raise OverflowError("infinite generated days, count must be great than 0")
        return super().get_tradedays_month_begin(dt, count)

    def get_tradedays_week_end(self, dt: datetime, count: int) -> List[datetime]:
        if count <= 0:
            raise OverflowError("infinite generated days, count must be great than 0")
        return super().get_tradedays_week_end(dt, count)

    def get_tradedays_week_begin(self, dt: datetime, count: int) -> List[datetime]:
        if count <= 0:
            raise OverflowError("infinite generated days, count must be great than 0")
        return super().get_tradedays_week_begin(dt, count)

    def get_tradedays_week_day(
        self, weekday: int, dt: datetime, count: int
    ) -> List[datetime]:
        if count <= 0:
            raise OverflowError("infinite generated days, count must be great than 0")
        return super().get_tradedays_week_day(weekday, dt, count)

    def get_tradedays_between(
        self, start_dt: datetime, end_dt: datetime
    ) -> List[datetime]:
        """get start_dt <= trade days <= end_dt"""
        ret = []
        start_dt = datetime.combine(
            start_dt.date(), time(0, 0, 0), tzinfo=start_dt.tzinfo
        )
        end_dt = datetime.combine(end_dt.date(), time(0, 0, 0), tzinfo=end_dt.tzinfo)
        next_day = start_dt
        while next_day <= end_dt:
            ret.append(next_day)
            next_day += timedelta(days=1)
        return ret

    def is_trading(self, dt: datetime):
        return True


if __name__ == "__main__":
    cal = Time7x24Calendar()
    print(cal)
