from datetime import date, datetime, time, timezone

import pandas as pd
import pytest
import quantdata as qd

from quantcalendar import CalendarAstock


@pytest.fixture(scope="module")
def mongo_client():
    conn = qd.mongo_connect("127.0.0.1", tz_aware=True)  # utc
    print("connect mongodb")
    days = qd.mongo_get_data(conn["quantcalendar"], "cn_stock")
    data = [(int(day["_id"].timestamp()), day["status"]) for day in days]
    CalendarAstock.InitData(data)
    yield
    qd.mongo_close(conn)
    print("disconnect mongodb")


def ts(dt: datetime):
    dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


# fmt: off
def test_get_tradedays(mongo_client):
    cal = CalendarAstock()
    assert cal.get_tradedays_gte(ts(datetime(2023, 6, 30)))[0] == ts(datetime(2023, 6, 30))
    assert cal.get_tradedays_lte(ts(datetime(2024, 9, 17)))[-1] == ts(datetime(2024, 9, 13))
    assert cal.get_tradedays_lte(ts(datetime(2024, 9, 14)))[-1] == ts(datetime(2024, 9, 13))
    assert cal.get_tradedays_lte(ts(datetime(2024, 9, 13)))[-1] == ts(datetime(2024, 9, 13))
    # assert cal.get_tradedays_between(datetime(2024, 9, 13), datetime(2024, 9, 17)) == [datetime(2024, 9, 13)]
    # assert cal.get_tradedays_between(datetime(2024, 9, 13), datetime(2024, 9, 18)) == [datetime(2024, 9, 13), datetime(2024, 9, 18)]
    # month_ends = [ datetime(2024, 1, 31), datetime(2024, 2, 29), datetime(2024, 3, 29), ]
    # month_begins = [ datetime(2024, 1, 2), datetime(2024, 2, 1), datetime(2024, 3, 1), ]
    # week_ends = [ datetime(2024, 1, 5), datetime(2024, 1, 12), datetime(2024, 1, 19), ]
    # week_begins = [ datetime(2024, 1, 2), datetime(2024, 1, 8), datetime(2024, 1, 15), ]
    # week_days = [ datetime(2024, 2, 7), datetime(2024, 2, 21), datetime(2024, 2, 28), ]
    # assert cal.get_tradedays_month_end(datetime(2024, 1, 1))
    # assert cal.get_tradedays_month_end(datetime(2024, 1, 1), datetime(2024, 3, 31)) == month_ends
    # assert cal.get_tradedays_month_end(datetime(2024, 1, 1), count=3) == month_ends
    # assert cal.get_tradedays_month_end(datetime(2024, 1, 31), count=3) == month_ends
    # assert cal.get_tradedays_month_begin(datetime(2024, 1, 1), datetime(2024, 3, 1)) == month_begins
    # assert cal.get_tradedays_month_begin(datetime(2024, 1, 1), count=3) == month_begins
    # assert cal.get_tradedays_month_begin(datetime(2024, 1, 2), count=3) == month_begins
    # assert cal.get_tradedays_month_begin(datetime(2023, 12, 31), count=3) == month_begins
    # assert cal.get_tradedays_week_end(datetime(2024, 1, 1), datetime(2024, 1, 21)) == week_ends
    # assert cal.get_tradedays_week_end(datetime(2024, 1, 1), count=3) == week_ends
    # assert cal.get_tradedays_week_end(datetime(2024, 1, 5), count=3) == week_ends
    # assert cal.get_tradedays_week_end(datetime(2024, 1, 7))[0] == datetime(2024, 1, 12)
    # assert cal.get_tradedays_week_begin(datetime(2024, 1, 1), datetime(2024, 1, 15)) == week_begins
    # assert cal.get_tradedays_week_begin(datetime(2024, 1, 1), count=3) == week_begins
    # assert cal.get_tradedays_week_begin(datetime(2023, 12, 26), count=3) == week_begins
    # assert cal.get_tradedays_week_begin(datetime(2024, 6, 11))[0] == datetime(2024, 6, 11)
    # assert cal.get_tradedays_week_day(3, datetime(2024, 2, 7), datetime(2024, 2, 28)) == week_days
    # assert cal.get_tradedays_week_day(3, datetime(2024, 2, 7), count=3) == week_days


# def test_calendar_astock(mongo_client):
#     cal = CalendarAstock(mongo_client)
#     assert cal.is_trading(datetime(2024, 9, 20, 9, 0)) == False
#     assert cal.is_trading(datetime(2024, 9, 20, 9, 30)) == True
#     assert cal.is_trading(datetime(2024, 9, 20, 11, 30)) == True
#     assert cal.is_trading(datetime(2024, 9, 20, 12, 0)) == False
#     assert cal.is_trading(datetime(2024, 9, 20, 13, 0)) == True
#     assert cal.is_trading(datetime(2024, 9, 20, 15, 0)) == True
#     assert cal.is_trading(datetime(2024, 9, 20, 15, 1)) == False
#     assert cal.is_trading(datetime(2024, 9, 17, 10, 0)) == False
#     assert cal.get_open_close_dt(datetime(2024, 9, 13)) == (datetime(2024, 9, 13, 9, 30), datetime(2024, 9, 13, 15))
#     assert cal.get_open_close_dt(datetime(2024, 9, 14)) == (datetime(2024, 9, 18, 9, 30), datetime(2024, 9, 18, 15))
#     assert cal.get_open_close_dt(datetime(2024, 9, 18, 10)) == (datetime(2024, 9, 19, 9, 30), datetime(2024, 9, 18, 15))
#     # test bartime
#     bartime_testcases = [
#         (datetime(2024, 9, 20, 15), datetime(2024, 9, 20, 15), 60),
#         (datetime(2024, 9, 20, 15, 0, 1), datetime(2024, 9, 23, 9, 31), 60),
#         (datetime(2024, 9, 23, 9, 30), datetime(2024, 9, 23, 9, 35), 300),
#         (datetime(2024, 9, 20, 8, 30), datetime(2024, 9, 20, 10, 30), I1H),
#         (datetime(2024, 9, 20, 15), datetime(2024, 9, 20, 15), I1H),
#         (datetime(2024, 9, 20, 15), datetime(2024, 9, 20, 15), I2H),
#         (datetime(2024, 10, 1), datetime(2024, 10, 8, 15), DAILY),
#         (datetime(2024, 10, 6), datetime(2024, 10, 11, 15), WEEKLY),
#         (datetime(2024, 10, 11), datetime(2024, 10, 31, 15), MONTHLY),
#     ]
#     for query, answer, interval in bartime_testcases:
#         assert cal.get_current_bartime(query, interval) == answer
