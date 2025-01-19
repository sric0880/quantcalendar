from datetime import timezone

import numpy as np
import pandas as pd
from datetime_helper import to_seconds

from quantcalendar import Time7x24Calendar, bar_unit


# fmt: off
def test_tradedays(to_datetime64):
    cal = Time7x24Calendar()
    # days
    dates = cal.get_tradedays_gte(to_datetime64(2022, 1, 1), 1000)
    dates_right = (
        pd.date_range("2022-01-01", periods=1000, freq="D", tz=timezone.utc)
        .map(pd.Timestamp.timestamp)
        .to_list()
    )
    assert dates == dates_right

    last_date = dates[-1]
    assert cal.get_tradedays_lte(np.datetime64(last_date, "s"), 1000) == dates

    # month
    dates = cal.get_month_ends_gte(to_datetime64(2022, 1, 1), 100)
    dates_right = (
        pd.date_range("2022-01-31", periods=100, freq="ME", tz=timezone.utc)
        .map(pd.Timestamp.timestamp)
        .to_list()
    )
    assert dates == dates_right

    # weekdays
    dates = cal.get_week_days_gte(1, to_datetime64(2022, 1, 1), 500)
    dates1 = cal.get_week_days_gte(7, to_datetime64(2022, 1, 1), 500)
    dates2 = cal.get_week_begins_gte(to_datetime64(2022, 1, 1), 500)
    dates3 = cal.get_week_ends_gte(to_datetime64(2022, 1, 1), 500)
    assert dates == dates2
    assert dates1 == dates3

    assert cal.get_week_end_next(to_datetime64(2021, 12, 31)) == 1641081600

    # Monday
    dates_right = (
        pd.date_range("2022-01-01", periods=500, freq="W-MON", tz=timezone.utc)
        .map(pd.Timestamp.timestamp)
        .to_list()
    )
    assert dates == dates_right
    # Sunday
    dates1_right = (
        pd.date_range("2022-01-01", periods=500, freq="W-SUN", tz=timezone.utc)
        .map(pd.Timestamp.timestamp)
        .to_list()
    )
    assert dates1 == dates1_right

    assert cal.get_tradedays_lte(to_datetime64(2024, 9, 13, 1), 2) == [to_seconds(2024, 9, 12), to_seconds(2024, 9, 13)]
    assert cal.get_tradedays_lte(to_datetime64(2024, 9, 13), 2) == [to_seconds(2024, 9, 12), to_seconds(2024, 9, 13)]
    assert cal.get_tradedays_between(to_datetime64(2024, 9, 13), to_datetime64(2024, 9, 13)) == [to_seconds(2024, 9, 13)]
    assert cal.get_tradedays_between(to_datetime64(2024, 9, 13), to_datetime64(2024, 9, 14)) == [to_seconds(2024, 9, 13), to_seconds(2024, 9, 14)]

    month_ends = [ to_seconds(2024, 1, 31), to_seconds(2024, 2, 29), to_seconds(2024, 3, 31), ]
    month_begins = [ to_seconds(2024, 1, 1), to_seconds(2024, 2, 1), to_seconds(2024, 3, 1), ]
    week_ends = [ to_seconds(2024, 1, 7), to_seconds(2024, 1, 14), to_seconds(2024, 1, 21), ]
    week_begins = [ to_seconds(2024, 1, 1), to_seconds(2024, 1, 8), to_seconds(2024, 1, 15), ]
    week_days = [ to_seconds(2023, 12, 27), to_seconds(2024, 1, 3), to_seconds(2024, 1, 10), ]
    assert cal.get_month_ends_between(to_datetime64(2024, 1, 1), to_datetime64(2024, 3, 31)) == month_ends
    assert cal.get_month_ends_gte(to_datetime64(2024, 1, 1), 3) == month_ends
    assert cal.get_month_ends_gte(to_datetime64(2024, 1, 31), 3) == month_ends
    assert cal.get_month_begins_between(to_datetime64(2024, 1, 1), to_datetime64(2024, 3, 1)) == month_begins
    assert cal.get_month_begins_gte(to_datetime64(2024, 1, 1), 3) == month_begins
    assert cal.get_month_begins_gte(to_datetime64(2023, 12, 31), 3) == month_begins
    assert cal.get_week_ends_between(to_datetime64(2024, 1, 1), to_datetime64(2024, 1, 21)) == week_ends
    assert cal.get_week_ends_gte(to_datetime64(2024, 1, 1), 3) == week_ends
    assert cal.get_week_ends_gte(to_datetime64(2024, 1, 7), 3) == week_ends
    assert cal.get_week_begins_between(to_datetime64(2024, 1, 1), to_datetime64(2024, 1, 15)) == week_begins
    assert cal.get_week_begins_gte(to_datetime64(2024, 1, 1), 3) == week_begins
    assert cal.get_week_begins_gte(to_datetime64(2023, 12, 26), 3) == week_begins
    assert cal.get_week_days_between(3, to_datetime64(2023, 12, 27), to_datetime64(2024, 1, 10)) == week_days
    assert cal.get_week_days_gte(3, to_datetime64(2023, 12, 27), 3) == week_days


def test_trading_time(to_datetime64):
    cal = Time7x24Calendar()
    assert cal.is_trading(to_datetime64(2023, 1, 1))
    assert cal.is_trading_day(to_datetime64(2023, 1, 1))
    assert cal.is_trading_time(to_datetime64(2023, 1, 1))


def test_next_bartime(to_datetime64):
    cal = Time7x24Calendar()
    bartime_testcases = [
        (to_datetime64(2024, 9, 13), to_seconds(2024, 9, 13), 60),
        (to_datetime64(2024, 9, 13), to_seconds(2024, 9, 13), 300),
        (to_datetime64(2024, 9, 13), to_seconds(2024, 9, 13), 900),
        (to_datetime64(2024, 9, 13, 0, 0, 1), to_seconds(2024, 9, 13, 0, 1), 60),
        (to_datetime64(2024, 9, 13, 0, 1, 0), to_seconds(2024, 9, 13, 0, 1), 60),
        (to_datetime64(2024, 9, 13, 0, 0, 1), to_seconds(2024, 9, 13, 0, 5), 300),
        (to_datetime64(2024, 9, 13, 0, 0, 1), to_seconds(2024, 9, 13, 0, 15), 900),
        (to_datetime64(2024, 9, 13, 23, 59, 1), to_seconds(2024, 9, 14), 60),
        (to_datetime64(2024, 9, 13, 23), to_seconds(2024, 9, 13, 23), bar_unit.hour),
        (to_datetime64(2024, 9, 30), to_seconds(2024, 10, 1), bar_unit.mon),
        (to_datetime64(2024, 9, 30, 1), to_seconds(2024, 10, 1), bar_unit.mon),
        (to_datetime64(2024, 10, 1), to_seconds(2024, 10, 1), bar_unit.mon),
        (to_datetime64(2024, 10, 1, 1), to_seconds(2024, 11, 1), bar_unit.mon),
        (to_datetime64(2024, 9, 15), to_seconds(2024, 9, 16), bar_unit.week),
        (to_datetime64(2024, 9, 15, 1), to_seconds(2024, 9, 16), bar_unit.week),
        (to_datetime64(2024, 9, 16), to_seconds(2024, 9, 16), bar_unit.week),
        (to_datetime64(2024, 9, 16, 1), to_seconds(2024, 9, 23), bar_unit.week),
        (to_datetime64(2024, 9, 13), to_seconds(2024, 9, 13), bar_unit.day),
        (to_datetime64(2024, 9, 13, 1), to_seconds(2024, 9, 14), bar_unit.day),
    ]
    for query, answer, interval in bartime_testcases:
        assert cal.get_bartime_next(interval, query) == answer

    assert cal.get_bartime_next(60, to_datetime64(2024, 9, 13, 23, 59, 1)) == to_seconds(2024, 9, 14)
    assert cal.get_bartime_next(60, np.datetime64("2024-09-13T23:59:01")) == to_seconds(2024, 9, 14)


def test_get_bartimes(to_datetime64):
    cal = Time7x24Calendar()
    bartimes = cal.get_bartimes_gte(bar_unit.mon, to_datetime64(2024, 9, 13), count=20)
    assert bartimes[0] == to_seconds(2024, 10, 1)
    assert bartimes[1] == to_seconds(2024, 11, 1)

    bartimes = cal.get_bartimes_gte(bar_unit.week, to_datetime64(2024, 9, 13), count=20)
    assert bartimes[0] == to_seconds(2024, 9, 16)

    bartimes = cal.get_bartimes_gte(bar_unit.day, to_datetime64(2024, 9, 13), count=20)
    assert len(bartimes) == 20
    assert bartimes[0] == to_seconds(2024, 9, 13)
    assert bartimes[-1] == to_seconds(2024, 10, 2)

    bartimes = cal.get_bartimes_between(30*bar_unit.min, to_datetime64(2024, 9, 13, 1, 0, 1), to_datetime64(2024, 9, 14))
    assert bartimes[0] == to_seconds(2024, 9, 13, 1, 30)

    bartimes = cal.get_bartimes_between(4*bar_unit.hour, to_datetime64(2024, 9, 13), to_datetime64(2024, 9, 14))
    assert len(bartimes) == 6
    assert bartimes == [
        to_seconds(2024, 9, 13),
        to_seconds(2024, 9, 13, 4),
        to_seconds(2024, 9, 13, 8),
        to_seconds(2024, 9, 13, 12),
        to_seconds(2024, 9, 13, 16),
        to_seconds(2024, 9, 13, 20),
    ]
