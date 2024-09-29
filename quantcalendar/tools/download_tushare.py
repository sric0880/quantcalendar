import pandas as pd

from quantcalendar import calendar_astock


def download(token: str):
    import tushare as ts

    pro = ts.pro_api(token)
    df = pro.trade_cal(exchange="SSE", fields=["cal_date", "is_open"])
    print("A股交易日历下载完成")
    df = df.iloc[::-1]
    df = df.rename(
        columns={"cal_date": "_id", "is_open": "status"}
    )  # mongodb要求必须有_id
    df["_id"] = pd.to_datetime(df["_id"])
    df = df.astype({"status": "int8"})
    # print(df)
    # print(df.info())
    return calendar_astock.CalendarAstock.COLLECTION_NAME, df.to_dict(orient="records")
