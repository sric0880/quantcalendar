import numpy as np
import pytest
import quantdata as qd
from datetime_helper import to_seconds

from quantcalendar import CalendarAstock, bar_unit, timestamp_s


@pytest.fixture(scope="module", autouse=True)
def mongo_client():
    with qd.open_mongodb(host="127.0.0.1"):
        days = qd.mongo_get_data("quantcalendar", "cn_stock")
        dates_arr = [(timestamp_s(day["_id"]), day["status"]) for day in days]
        CalendarAstock.Init(dates_arr)


# fmt: off
def test_tradedays(to_datetime64):
    cal = CalendarAstock()
    assert cal.get_timezone() == "Asia/Shanghai"
    assert cal.get_intervals() == [1*bar_unit.min, 5*bar_unit.min, 15*bar_unit.min, 30*bar_unit.min, bar_unit.hour, 2*bar_unit.hour]
    assert cal.get_tradedays_gte(to_datetime64(2023, 6, 30))[0] == to_seconds(2023, 6, 30)
    assert cal.get_tradeday_next(to_datetime64(2023, 6, 30)) == to_seconds(2023, 6, 30)
    assert cal.get_tradedays_lte(to_datetime64(2024, 9, 17))[-1] == to_seconds(2024, 9, 13)
    assert cal.get_tradedays_lte(to_datetime64(2024, 9, 14))[-1] == to_seconds(2024, 9, 13)
    assert cal.get_tradedays_lte(to_datetime64(2024, 9, 13))[-1] == to_seconds(2024, 9, 13)
    assert cal.get_tradeday_last(to_datetime64(2024, 9, 13)) == to_seconds(2024, 9, 13)
    assert cal.get_tradedays_between(to_datetime64(2024, 9, 13), to_datetime64(2024, 9, 17)) == [to_seconds(2024, 9, 13)]
    assert cal.get_tradedays_between(to_datetime64(2024, 9, 13), to_datetime64(2024, 9, 18)) == [to_seconds(2024, 9, 13), to_seconds(2024, 9, 18)]
    assert cal.get_tradedays_count(to_datetime64(2024, 9, 13), to_datetime64(2024, 9, 18)) == 2
    month_ends = [ to_seconds(2024, 1, 31), to_seconds(2024, 2, 29), to_seconds(2024, 3, 29), ]
    month_begins = [ to_seconds(2024, 1, 2), to_seconds(2024, 2, 1), to_seconds(2024, 3, 1), ]
    week_ends = [ to_seconds(2024, 1, 5), to_seconds(2024, 1, 12), to_seconds(2024, 1, 19), ]
    week_begins = [ to_seconds(2024, 1, 2), to_seconds(2024, 1, 8), to_seconds(2024, 1, 15), ]
    week_days = [ to_seconds(2024, 2, 7), to_seconds(2024, 2, 21), to_seconds(2024, 2, 28), ]
    assert cal.get_month_ends_gte(to_datetime64(2024, 1, 1))
    assert cal.get_month_ends_between(to_datetime64(2024, 1, 1), to_datetime64(2024, 3, 31)) == month_ends
    assert cal.get_month_ends_gte(to_datetime64(2024, 1, 1), count=3) == month_ends
    assert cal.get_month_ends_gte(to_datetime64(2024, 1, 31), count=3) == month_ends
    assert cal.get_month_begins_between(to_datetime64(2024, 1, 1), to_datetime64(2024, 3, 1)) == month_begins
    assert cal.get_month_begins_gte(to_datetime64(2024, 1, 1), count=3) == month_begins
    assert cal.get_month_begins_gte(to_datetime64(2024, 1, 2), count=3) == month_begins
    assert cal.get_month_begins_gte(to_datetime64(2023, 12, 31), count=3) == month_begins
    assert cal.get_week_ends_between(to_datetime64(2024, 1, 1), to_datetime64(2024, 1, 21)) == week_ends
    assert cal.get_week_ends_gte(to_datetime64(2024, 1, 1), count=3) == week_ends
    assert cal.get_week_ends_gte(to_datetime64(2024, 1, 5), count=3) == week_ends
    assert cal.get_week_ends_gte(to_datetime64(2024, 1, 7))[0] == to_seconds(2024, 1, 12)
    assert cal.get_week_end_next(to_datetime64(2024, 1, 7)) == to_seconds(2024, 1, 12)
    assert cal.get_week_begins_between(to_datetime64(2024, 1, 1), to_datetime64(2024, 1, 15)) == week_begins
    assert cal.get_week_begins_gte(to_datetime64(2024, 1, 1), count=3) == week_begins
    assert cal.get_week_begins_gte(to_datetime64(2023, 12, 26), count=3) == week_begins
    assert cal.get_week_begins_gte(to_datetime64(2024, 6, 11))[0] == to_seconds(2024, 6, 11)
    assert cal.get_week_begin_next(to_datetime64(2024, 6, 11)) == to_seconds(2024, 6, 11)
    assert cal.get_week_begin_last(to_datetime64(2024, 6, 11)) == to_seconds(2024, 6, 11)
    assert cal.get_week_days_between(3, to_datetime64(2024, 2, 7), to_datetime64(2024, 2, 28)) == week_days
    assert cal.get_week_days_gte(3, to_datetime64(2024, 2, 7), count=3) == week_days


def test_trading_time(to_datetime64):
    cal = CalendarAstock()
    assert cal.is_trading(np.datetime64("2007-01-04T14:59:59.999999")) == True
    assert cal.is_trading(to_datetime64(2024, 9, 20, 9, 0)) == False
    assert cal.is_trading(to_datetime64(2024, 9, 20, 9, 30)) == True
    assert cal.is_trading(to_datetime64(2024, 9, 20, 11, 30)) == True
    assert cal.is_trading(to_datetime64(2024, 9, 20, 12, 0)) == False
    assert cal.is_trading(to_datetime64(2024, 9, 20, 13, 0)) == True
    assert cal.is_trading(to_datetime64(2024, 9, 20, 15, 0)) == True
    assert cal.is_trading(to_datetime64(2024, 9, 20, 15, 1)) == False
    assert cal.is_trading(to_datetime64(2024, 9, 17, 10, 0)) == False
    assert cal.get_next_open_close(to_datetime64(2024, 9, 13)) == (to_seconds(2024, 9, 13, 9, 30), to_seconds(2024, 9, 13, 15))
    assert cal.get_next_open_close(to_datetime64(2024, 9, 14)) == (to_seconds(2024, 9, 18, 9, 30), to_seconds(2024, 9, 18, 15))
    assert cal.get_next_open_close(to_datetime64(2024, 9, 18, 10)) == (to_seconds(2024, 9, 19, 9, 30), to_seconds(2024, 9, 18, 15))


def test_next_bartime(to_datetime64):
    cal = CalendarAstock()
    bartime_testcases = [
        (to_datetime64(2024, 9, 20, 15), to_seconds(2024, 9, 20, 15), 60),
        (to_datetime64(2024, 9, 20, 15, 0, 1), to_seconds(2024, 9, 23, 9, 31), 60),
        (to_datetime64(2024, 9, 23, 9, 30), to_seconds(2024, 9, 23, 9, 35), 300),
        (to_datetime64(2024, 9, 20, 8, 30), to_seconds(2024, 9, 20, 10, 30), 1*bar_unit.hour),
        (to_datetime64(2024, 9, 20, 15), to_seconds(2024, 9, 20, 15), 1*bar_unit.hour),
        (to_datetime64(2024, 9, 20, 15), to_seconds(2024, 9, 20, 15), 2*bar_unit.hour),
        (to_datetime64(2024, 10, 1), to_seconds(2024, 10, 8, 15), bar_unit.day),
        (to_datetime64(2024, 10, 6), to_seconds(2024, 10, 11, 15), bar_unit.week),
        (to_datetime64(2024, 10, 11), to_seconds(2024, 10, 31, 15), bar_unit.mon),
    ]
    for query, answer, interval in bartime_testcases:
        assert cal.get_bartime_next(interval, query) == answer


def test_get_bartimes(to_datetime64):
    cal = CalendarAstock()
    bartimes = cal.get_bartimes_gte(bar_unit.mon, to_datetime64(2024, 9, 13), count=2)
    assert bartimes[0] == to_seconds(2024, 9, 30, 15)
    assert bartimes[1] == to_seconds(2024, 10, 31, 15)

    # 2024年9月30这周只有一天交易日，是周初，也是周末
    bartimes = cal.get_bartimes_gte(bar_unit.week, to_datetime64(2024, 9, 30), count=2)
    assert bartimes[0] == to_seconds(2024, 9, 30, 15)
    assert bartimes[1] == to_seconds(2024, 10, 11, 15)
    bartimes = cal.get_bartimes_gte(bar_unit.week, to_datetime64(2024, 10, 1), count=2)
    assert bartimes[0] == to_seconds(2024, 10, 11, 15)
    assert bartimes[1] == to_seconds(2024, 10, 18, 15)

    bartimes = cal.get_bartimes_gte(bar_unit.day, to_datetime64(2024, 9, 30), count=30)
    assert len(bartimes) == 30
    assert bartimes[0] == to_seconds(2024, 9, 30, 15)
    assert bartimes[1] == to_seconds(2024, 10, 8, 15)

    bartimes = cal.get_bartimes_between(30*bar_unit.min, to_datetime64(2024, 9, 13), to_datetime64(2024, 9, 14))
    assert len(bartimes) == 8
    assert bartimes[0] == to_seconds(2024, 9, 13, 10)
    assert bartimes[1] == to_seconds(2024, 9, 13, 10, 30)
    assert bartimes[2] == to_seconds(2024, 9, 13, 11)
    assert bartimes[3] == to_seconds(2024, 9, 13, 11, 30)
    assert bartimes[4] == to_seconds(2024, 9, 13, 13, 30)
    assert bartimes[5] == to_seconds(2024, 9, 13, 14)
    assert bartimes[6] == to_seconds(2024, 9, 13, 14, 30)
    assert bartimes[7] == to_seconds(2024, 9, 13, 15)

    bartimes = cal.get_bartimes_between(2*bar_unit.hour, to_datetime64(2024, 9, 13, 9, 30), to_datetime64(2024, 9, 14))
    assert len(bartimes) == 2
    assert bartimes[0] == to_seconds(2024, 9, 13, 11, 30)
    assert bartimes[1] == to_seconds(2024, 9, 13, 15)

    assert cal.get_bartimes_between(bar_unit.hour, to_datetime64(2024, 9, 13, 10, 31), to_datetime64(2024, 9, 13, 11, 30)) == []


def test_exceptions(to_datetime64):
    cal = CalendarAstock()
    # invalid arguments: interval
    with pytest.raises(ValueError):
        cal.get_bartimes_gte(5 * bar_unit.hour, to_datetime64(2024, 9, 13), 2)
    with pytest.raises(ValueError):
        cal.get_bartimes_between(20 * bar_unit.min, to_datetime64(2024, 9, 13), to_datetime64(2024, 9, 14))
    with pytest.raises(ValueError):
        cal.get_bartime_next(2 * bar_unit.day, to_datetime64(2024, 9, 13))

    # Out of calendar error
    out_of_calendar_year = 2026
    before_out_of_cal_year = 2025
    with pytest.raises(IndexError):
        cal.get_tradeday_next(to_datetime64(out_of_calendar_year, 1, 1))
    with pytest.raises(IndexError):
        cal.get_tradeday_last(to_datetime64(1990,1,1))
    with pytest.raises(IndexError):
        # datetime(out_of_calendar_year, 1, 1) 不会报错，因为以结束时间为K线时间，这个时间还是属于前一天的K线
        cal.get_bartimes_between(bar_unit.hour, to_datetime64(out_of_calendar_year, 1, 1, 0, 0, 0, 1), to_datetime64(out_of_calendar_year, 1, 2))
    with pytest.raises(IndexError):
        cal.get_bartime_next(bar_unit.hour, to_datetime64(2050, 1, 1))
    with pytest.raises(IndexError):
        # start of session is not found
        cal.get_next_open_close(to_datetime64(before_out_of_cal_year, 12, 31, 12))
    with pytest.raises(IndexError):
        # start of session is not found
        cal.get_next_session(to_datetime64(before_out_of_cal_year, 12, 31, 14))
    with pytest.raises(IndexError):
        # both start and end of session are not found
        cal.get_next_session(to_datetime64(out_of_calendar_year, 1, 1))
    with pytest.raises(IndexError):
        cal.get_next_session(to_datetime64(2050, 1, 1))
    with pytest.raises(IndexError):
        # actually it's trading, but next start of session if not found
        cal.is_trading(to_datetime64(before_out_of_cal_year, 12, 31, 15))
    with pytest.raises(IndexError):
        cal.is_trading_day(to_datetime64(out_of_calendar_year, 1, 1))

    # start > end
    with pytest.raises(ValueError):
        cal.get_bartimes_between(2 * bar_unit.hour, to_datetime64(2024, 9, 13), to_datetime64(2024, 9, 11))
    with pytest.raises(ValueError):
        cal.get_week_begins_between(to_datetime64(2024, 1, 15), to_datetime64(2024, 1, 1))
