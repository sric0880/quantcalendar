import datetime

import pandas as pd
from akshare.futures.futures_zh_sina import (futures_zh_daily_sina,
                                             futures_zh_minute_sina)

from quantcalendar.common import IntervalType


def _filter_dt(dt):
    # 新浪数据有点问题
    day_of_week = dt.day_of_week
    t = dt.time()
    if day_of_week == 0 and t < datetime.time(9):
        return False
    elif dt.day_of_week == 5 and t > datetime.time(2, 30):
        return False
    return True


def download_and_save_answers(symbols):
    for symbol, close_time in symbols:
        for interval in ("1", "5", "15", "30", "60"):
            df = futures_zh_minute_sina(symbol, interval)
            df["datetime"] = pd.to_datetime(df["datetime"])
            dt = df.loc[df["datetime"].apply(_filter_dt), "datetime"]
            dt.reset_index(drop=True, inplace=True)
            st = dt.iloc[0]
            ed = dt.iloc[-1]
            print(f"{symbol}\t{interval}min\tfrom {st} to {ed}")
            origin = pd.date_range(st, ed, periods=200)
            answer = dt[dt.searchsorted(origin, side="left")]
            int_type = IntervalType.MINUTE
            if interval == "60":
                interval = 1
                int_type = IntervalType.HOUR
            answer.index = origin
            # print(answer)
            answer.to_pickle(
                f"tests/next_bartime_answers/{symbol}_{interval}{int_type.value}.pickle"
            )
        df = futures_zh_daily_sina(symbol)
        df["date"] = pd.to_datetime(df["date"])
        dt = df["date"].apply(lambda x: x + close_time)
        st = dt.iloc[0]
        ed = dt.iloc[-1]
        print(f"{symbol}\tdaily\tfrom {st} to {ed}")
        origin = pd.date_range(st, ed, periods=200)
        answer = dt[dt.searchsorted(origin, side="left")]
        answer.index = origin
        # print(answer)
        answer.to_pickle(f"tests/next_bartime_answers/{symbol}_1D.pickle")


if __name__ == "__main__":
    symbols = [
        ("AG2412", datetime.timedelta(hours=15)),
        ("T2409", datetime.timedelta(hours=15, minutes=15)),
        ("IH2409", datetime.timedelta(hours=15)),
        ("C2411", datetime.timedelta(hours=15)),
        ("EC2412", datetime.timedelta(hours=15)),
        ("BC2411", datetime.timedelta(hours=15)),
    ]

    # - 1m
    #   - 3m
    #   - 5m
    #   - 15m
    #   - 30m
    #   - 1h
    #   # - 2h
    #   - 4h
    #   # - 6h
    #   - 12h
    #   - 1d
    #   # - 1w
    #   # - 1M
    #   # - 3M
    download_and_save_answers(symbols)
