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

    def get_tradedays_gte(self, dt: datetime) -> List[datetime]:
        """get trade days >= dt 取一年时间"""
        start_dt = datetime.combine(dt.date(), time(0, 0, 0), tzinfo=dt.tzinfo)
        return [start_dt + timedelta(days=i) for i in range(365)]

    def get_tradedays_lte(self, dt: datetime) -> List[datetime]:
        """get trade days <= dt 取一年时间"""
        start_dt = datetime.combine(dt.date(), time(0, 0, 0), tzinfo=dt.tzinfo)
        return [start_dt + timedelta(days=i) for i in range(-364, 1, 1)]

    def is_trading(self, dt: datetime):
        return True


if __name__ == "__main__":
    cal = Time7x24Calendar()
    print(cal)
