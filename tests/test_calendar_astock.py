import pytest
import quantdata as qd
from common import tp, ts

from quantcalendar import CalendarAstock, bar_unit


@pytest.fixture(scope="module", autouse=True)
def mongo_client():
    conn = qd.mongo_connect("127.0.0.1", tz_aware=True)  # utc
    print("connect mongodb")
    days = qd.mongo_get_data(conn["quantcalendar"], "cn_stock")
    data = [(int(day["_id"].timestamp()), day["status"]) for day in days]
    CalendarAstock.InitData(data)
    qd.mongo_close(conn)
    print("disconnect mongodb")


# fmt: off
def test_get_tradedays():
    cal = CalendarAstock()
    assert cal.get_tradedays_gte(ts(2023, 6, 30))[0] == ts(2023, 6, 30)
    assert cal.get_tradeday_next(ts(2023, 6, 30)) == ts(2023, 6, 30)
    assert cal.get_tradedays_lte(ts(2024, 9, 17))[-1] == ts(2024, 9, 13)
    assert cal.get_tradedays_lte(ts(2024, 9, 14))[-1] == ts(2024, 9, 13)
    assert cal.get_tradedays_lte(ts(2024, 9, 13))[-1] == ts(2024, 9, 13)
    assert cal.get_tradeday_next(ts(2025,12,1)) is None
    assert cal.get_tradeday_last(ts(2024, 9, 13)) == ts(2024, 9, 13)
    assert cal.get_tradeday_last(ts(1990,1,1)) is None
    assert cal.get_tradedays_between(ts(2024, 9, 13), ts(2024, 9, 17)) == [ts(2024, 9, 13)]
    assert cal.get_tradedays_between(ts(2024, 9, 13), ts(2024, 9, 18)) == [ts(2024, 9, 13), ts(2024, 9, 18)]
    month_ends = [ ts(2024, 1, 31), ts(2024, 2, 29), ts(2024, 3, 29), ]
    month_begins = [ ts(2024, 1, 2), ts(2024, 2, 1), ts(2024, 3, 1), ]
    week_ends = [ ts(2024, 1, 5), ts(2024, 1, 12), ts(2024, 1, 19), ]
    week_begins = [ ts(2024, 1, 2), ts(2024, 1, 8), ts(2024, 1, 15), ]
    week_days = [ ts(2024, 2, 7), ts(2024, 2, 21), ts(2024, 2, 28), ]
    assert cal.get_month_ends_gte(ts(2024, 1, 1))
    assert cal.get_month_ends_between(ts(2024, 1, 1), ts(2024, 3, 31)) == month_ends
    assert cal.get_month_ends_gte(ts(2024, 1, 1), count=3) == month_ends
    assert cal.get_month_ends_gte(ts(2024, 1, 31), count=3) == month_ends
    assert cal.get_month_begins_between(ts(2024, 1, 1), ts(2024, 3, 1)) == month_begins
    assert cal.get_month_begins_gte(ts(2024, 1, 1), count=3) == month_begins
    assert cal.get_month_begins_gte(ts(2024, 1, 2), count=3) == month_begins
    assert cal.get_month_begins_gte(ts(2023, 12, 31), count=3) == month_begins
    assert cal.get_week_ends_between(ts(2024, 1, 1), ts(2024, 1, 21)) == week_ends
    assert cal.get_week_ends_gte(ts(2024, 1, 1), count=3) == week_ends
    assert cal.get_week_ends_gte(ts(2024, 1, 5), count=3) == week_ends
    assert cal.get_week_ends_gte(ts(2024, 1, 7))[0] == ts(2024, 1, 12)
    assert cal.get_week_end_next(ts(2024, 1, 7)) == ts(2024, 1, 12)
    assert cal.get_week_begins_between(ts(2024, 1, 1), ts(2024, 1, 15)) == week_begins
    assert cal.get_week_begins_gte(ts(2024, 1, 1), count=3) == week_begins
    assert cal.get_week_begins_gte(ts(2023, 12, 26), count=3) == week_begins
    assert cal.get_week_begins_gte(ts(2024, 6, 11))[0] == ts(2024, 6, 11)
    assert cal.get_week_begin_next(ts(2024, 6, 11)) == ts(2024, 6, 11)
    assert cal.get_week_begin_last(ts(2024, 6, 11)) == ts(2024, 6, 11)
    assert cal.get_week_days_between(3, ts(2024, 2, 7), ts(2024, 2, 28)) == week_days
    assert cal.get_week_days_gte(3, ts(2024, 2, 7), count=3) == week_days


def test_bartimes():
    cal = CalendarAstock()
    assert cal.is_trading(tp(2024, 9, 20, 9, 0)) == False
    assert cal.is_trading(tp(2024, 9, 20, 9, 30)) == True
    assert cal.is_trading(tp(2024, 9, 20, 11, 30)) == True
    assert cal.is_trading(tp(2024, 9, 20, 12, 0)) == False
    assert cal.is_trading(tp(2024, 9, 20, 13, 0)) == True
    assert cal.is_trading(tp(2024, 9, 20, 15, 0)) == True
    assert cal.is_trading(tp(2024, 9, 20, 15, 1)) == False
    assert cal.is_trading(tp(2024, 9, 17, 10, 0)) == False
    assert cal.get_next_open_close(tp(2024, 9, 13)) == (ts(2024, 9, 13, 9, 30), ts(2024, 9, 13, 15))
    assert cal.get_next_open_close(tp(2024, 9, 14)) == (ts(2024, 9, 18, 9, 30), ts(2024, 9, 18, 15))
    assert cal.get_next_open_close(tp(2024, 9, 18, 10)) == (ts(2024, 9, 19, 9, 30), ts(2024, 9, 18, 15))
    bartime_testcases = [
        (tp(2024, 9, 20, 15), ts(2024, 9, 20, 15), 60),
        (tp(2024, 9, 20, 15, 0, 1), ts(2024, 9, 23, 9, 31), 60),
        (tp(2024, 9, 23, 9, 30), ts(2024, 9, 23, 9, 35), 300),
        (tp(2024, 9, 20, 8, 30), ts(2024, 9, 20, 10, 30), 1*bar_unit.hour),
        (tp(2024, 9, 20, 15), ts(2024, 9, 20, 15), 1*bar_unit.hour),
        (tp(2024, 9, 20, 15), ts(2024, 9, 20, 15), 2*bar_unit.hour),
        (tp(2024, 10, 1), ts(2024, 10, 8, 15), bar_unit.day),
        (tp(2024, 10, 6), ts(2024, 10, 11, 15), bar_unit.week),
        (tp(2024, 10, 11), ts(2024, 10, 31, 15), bar_unit.mon),
    ]
    for query, answer, interval in bartime_testcases:
        assert cal.get_bartime_next(query, interval) == answer
