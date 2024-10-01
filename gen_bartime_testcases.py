import datetime

import pandas as pd
from akshare.futures.futures_zh_sina import (
    futures_zh_daily_sina,
    futures_zh_minute_sina,
)


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
            if interval == "60":
                int_type = f"1H"
            else:
                int_type = f"{interval}m"
            answer.index = origin
            # print(answer)
            answer.to_pickle(f"tests/next_bartime_answers/{symbol}_{int_type}.pickle")
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
        ("T2412", datetime.timedelta(hours=15, minutes=15)),
        ("IH2409", datetime.timedelta(hours=15)),
        ("C2411", datetime.timedelta(hours=15)),
        ("EC2412", datetime.timedelta(hours=15)),
        ("CU2411", datetime.timedelta(hours=15)),
    ]

    download_and_save_answers(symbols)

    # K线缺失导致计算错误，需要纠正
    file = "tests/next_bartime_answers/AG2412_15m.pickle"
    series = pd.read_pickle(file)
    # 2024-08-30 23:48:32.562814070: 2024-08-31 00:15:00 != 2024-08-31 00:00:00
    series["2024-08-30 23:48:32.562814070"] = pd.Timestamp("2024-08-31 00:00:00")
    # print(series["2024-08-30 23:48:32.562814070"])
    series.to_pickle(file)

    file = "tests/next_bartime_answers/C2411_1m.pickle"
    series = pd.read_pickle(file)
    # 2024-09-25 14:42:50.954773869: 2024-09-25 14:44:00 != 2024-09-25 14:43:00
    series["2024-09-25 14:42:50.954773869"] = pd.Timestamp("2024-09-25 14:43:00")
    # print(series["2024-09-25 14:42:50.954773869"])
    series.to_pickle(file)

    file = "tests/next_bartime_answers/CU2411_1H.pickle"
    series = pd.read_pickle(file)
    # 2024-06-28 23:04:49.447236180: 2024-06-29 01:00:00 != 2024-06-29 00:00:00
    series["2024-06-28 23:04:49.447236180"] = pd.Timestamp("2024-06-29 00:00:00")
    # print(series["2024-06-28 23:04:49.447236180"])
    series.to_pickle(file)

    file = "tests/next_bartime_answers/EC2412_1m.pickle"
    series = pd.read_pickle(file)
    # 2024-09-25 14:40:42.211055276: 2024-09-25 14:44:00 != 2024-09-25 14:41:00
    series["2024-09-25 14:40:42.211055276"] = pd.Timestamp("2024-09-25 14:41:00")
    # print(series["2024-09-25 14:40:42.211055276"])
    series.to_pickle(file)
