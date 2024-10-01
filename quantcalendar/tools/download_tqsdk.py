from contextlib import closing
from datetime import datetime

import pandas as pd
from chinese_calendar import get_holiday_detail

from quantcalendar import calendar_ctp


def download(tq_username, tq_psw, end_dt):
    from tqsdk import TqApi, TqAuth

    api = TqApi(auth=TqAuth(tq_username, tq_psw))
    with closing(api):
        yield _download_trade_cal(api, end_dt)
        yield _download_markettime(api)


def _download_trade_cal(api, end_dt):
    df = api.get_trading_calendar(start_dt=datetime(2012, 1, 1), end_dt=end_dt)
    print("期货交易日历下载完成")
    df = df.rename(columns={"date": "_id", "trading": "status"})
    df["_id"] = pd.to_datetime(df["_id"])
    # print(df)
    # print(df.info())
    dates = pd.date_range(df["_id"].iloc[0].date(), df["_id"].iloc[-1].date())

    status = dates.map(_get_trading_day_detail)
    # 节假日连着的周末全部标记为节假日
    status = _weekends_to_holidays(status.to_list())
    newdf = pd.DataFrame({"_id": dates, "status": status})
    newdf["status"] = newdf["status"].astype("int8")
    # 特例：除夕当天上班，但是不开市
    newdf.loc[df["_id"] == pd.Timestamp(year=2024, month=2, day=9), "status"] = 3
    _trading_days = newdf.loc[df["status"] == 1]
    _trading_days2 = df.loc[df["status"] == True]
    diff_days = set(_trading_days["_id"]) - set(_trading_days2["_id"])
    assert (
        diff_days == set()
    ), f"{diff_days}不是交易日, 但chinese_calendar包计算是交易日"
    # print(newdf)
    # print(newdf.info())
    return calendar_ctp.CalendarCTP.COLLECTION_NAME, newdf.to_dict(orient="records")


def _download_markettime(api):
    """
    每个品种的开盘收盘时间
    """
    quotes = api.query_quotes(
        ins_class="FUTURE",
        exchange_id=["CFFEX", "SHFE", "DCE", "CZCE", "INE", "GFEX"],
        expired=False,
    )
    # print(quotes)
    df = api.query_symbol_info(quotes)
    # print(df)
    print("期货基本资料下载完成")
    products = {}
    for row in df.itertuples():
        open_period = []
        open_period.extend(_time_period_from_str(row.trading_time_night))
        open_period.extend(_time_period_from_str(row.trading_time_day))
        if row.product_id in products:
            sum = 0
            for start, end in products[row.product_id]:
                sum += start
                sum += end
            sum_new = 0
            for start, end in open_period:
                sum_new += start
                sum_new += end
            assert sum == sum_new
        else:
            products[row.product_id] = open_period
    return calendar_ctp.CalendarCTP.COLLECTION_NAME_SESSIONS, [
        {"_id": product_id, "market_time": period}
        for product_id, period in products.items()
    ]


def _time_period_from_str(list_of_period):
    if not list_of_period:
        return []
    open_period = []
    for period in list_of_period:
        start = period[0].split(":")
        end = period[1].split(":")
        s_hour = int(start[0])
        s_min = int(start[1])
        s_sec = int(start[2])
        e_hour = int(end[0])
        e_min = int(end[1])
        e_sec = int(end[2])
        if e_hour > 23:
            e_hour = e_hour - 24
        s_total_seconds = s_hour * 3600 + s_min * 60 + s_sec
        e_total_seconds = e_hour * 3600 + e_min * 60 + e_sec
        open_period.append((s_total_seconds, e_total_seconds))
    return open_period


def _is_weekend(dt):
    wd = dt.weekday()
    return wd == 5 or wd == 6


def _get_trading_day_detail(dt):
    holiday, holiday_name = get_holiday_detail(dt)
    if holiday:
        if holiday_name is not None:
            return 3  # holiday
        else:
            return 2  # weekend
    else:
        if _is_weekend(dt):
            return 2  # weekend
        else:
            return 1  # workday


def _weekends_to_holidays(status):
    all_3 = []
    for i, s in enumerate(status):
        if s == 3:
            all_3.append(i)

    for i in all_3:
        for j in range(i - 1, -1, -1):
            if status[j] == 2:
                status[j] = 3
            else:
                break
        for j in range(i + 1, len(status)):
            if status[j] == 2:
                status[j] = 3
            else:
                break
    return status
