from datetime import date, datetime, time

import quantdata as qd
import quantcalendar as qc


def mongo_client():
    conn = qd.mongo_connect("127.0.0.1")
    print("connect mongodb")
    try:
        days = qd.mongo_get_data(conn["quantcalendar"], "cn_stock")
        data = [(int(day["_id"].timestamp()), day["status"]) for day in days]
        cal = qc.CalendarAstock()
        cal.InitData(data)
        assert cal.get_tradedays_gte(datetime(2023, 6, 30))[0] == datetime(2023, 6, 30)
    finally:
        qd.mongo_close(conn)
        print("disconnect mongodb")


mongo_client()
